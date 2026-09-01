# LOG018 — Outbox 패턴

## 배경 / 목표

Ch15에서 Spring Kafka의 DB-Kafka 트랜잭션 동기화를 실습하며 확인한 한계는 "DB 커밋이 끝난
직후 프로세스가 죽어서 Kafka 커밋이 실행될 기회조차 없는 크래시 윈도우"였다. Ch17
Follow-up에서는 컨슈머 멱등성이 "이미 도착한 메시지의 중복 처리"는 막아주지만 "DB 쓰기는
됐는데 그 결과를 Kafka로 발행하는 데 실패"하는 반대 방향 문제는 못 막는다고 짚었다. 이번
챕터는 그 갭을 Outbox 패턴으로 메운다 — DB 트랜잭션과 이벤트 발행 사이의 원자성을
아키텍처 차원에서 확보하는 게 목표다.

## 개념 정리

- **Dual write 문제의 본질**: DB와 Kafka는 서로 다른 저장소이고, 이 둘 사이에 "함께
  성공하거나 함께 실패"를 보장하는 표준 트랜잭션 프로토콜이 없다. 진짜 2단계 커밋(2PC)을
  하려면 Kafka가 XA 리소스 매니저 역할을 해야 하는데 지원하지 않는다. Ch15의 동기화는
  2PC가 아니라 "같은 스레드 안에서 트랜잭션 커밋에 콜백을 거는" 수준이었다.
- **Outbox의 핵심 트릭 — 문제를 시스템 밖에서 안으로 옮기기**: "서로 다른 두 시스템 간
  원자성 문제"를 풀지 않고, "RDBMS 하나 안에서의 원자성 문제"로 치환한다. `orders` 쓰기와
  `outbox_event` 쓰기를 같은 DB 트랜잭션에 넣으면 원자성은 DB가 그냥 보장해준다. Kafka로의
  실제 발행은 이 트랜잭션과 완전히 분리된 별도 단계(Relay)로 미뤄진다.
- **트레이드오프가 이동하는 세 지점**:
  - 지연 — 커밋 시점과 실제 Kafka 발행 시점 사이에 폴링 주기(`fixedDelay`)만큼 지연이 생긴다.
  - 중복 — Relay의 "발행 → 마킹" 사이에도 크래시 가능성이 남는다. 즉 Outbox는
    **at-least-once**이고, Ch17의 컨슈머 멱등성이 반드시 짝을 이뤄야 한다.
  - 순서 — 같은 `aggregate_id`의 이벤트가 같은 파티션에 들어가야 순서가 유지된다.
    Kafka 발행 시 파티션 키를 `aggregate_id`로 일관되게 써야 하는 이유다(Ch10 파티션 키
    설계와 직결).
- **Polling publisher vs CDC(Debezium)**: 이번 챕터는 JdbcTemplate 기반 스케줄러로 outbox
  테이블을 직접 폴링하는 방식을 썼다. 실무에서는 DB WAL을 직접 읽는 CDC(Debezium Outbox
  Event Router)가 지연·DB 부하 면에서 더 흔히 쓰이지만, Kafka Connect 인프라가 필요해 이
  프로젝트 범위 밖이다.
- **Outbox + Idempotent Consumer = practical exactly-once**: Outbox(발행 측 — 유실 없음,
  at-least-once)와 Ch17 Idempotent Consumer(수신 측 — 중복 제거)를 합치면 애플리케이션
  레벨에서 "정확히 한 번 처리된 것처럼 보이는" 효과를 얻는다. 이건 Ch16에서 다룬 Kafka
  자체의 exactly-once semantics(브로커 레벨, 프로듀서 세션 단위)와는 다른 레이어의
  보장이다.

## 진행 과정

### 1. 스키마 — `schema-chapter18.sql`

```sql
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS outbox_event;

CREATE TABLE orders (
    id BIGSERIAL PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL UNIQUE,
    status VARCHAR(20) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE outbox_event (
    id BIGSERIAL PRIMARY KEY,
    aggregate_id VARCHAR(100) NOT NULL,
    topic VARCHAR(100) NOT NULL,
    payload TEXT NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    published_at TIMESTAMP
);
```

`orders`는 Ch15와 같은 모양이지만 프로파일이 분리돼 있어 재정의해도 문제없다. `KafkaTopics`에는
`ORDER_EVENTS_OUTBOX = "order-events-outbox"`를 추가해 다른 챕터와 토픽을 분리했다.

### 2. `OutboxEventRepository` — outbox 테이블 접근

```java
public void save(String aggregateId, String topic, String payload) {
  jdbcTemplate.update("INSERT INTO outbox_event (aggregate_id, topic, payload) VALUES (?, ?, ?)",
      aggregateId, topic, payload);
}

public List<OutboxEventRow> findPending(int limit) {
  return jdbcTemplate.query(
      "SELECT id, topic, aggregate_id, payload FROM outbox_event WHERE status = 'PENDING' ORDER BY id LIMIT ?",
      (rs, rowNum) -> new OutboxEventRow(rs.getLong("id"), rs.getString("topic"),
          rs.getString("aggregate_id"), rs.getString("payload")),
      limit);
}

public void markPublished(Long id) {
  jdbcTemplate.update(
      "UPDATE outbox_event SET status = 'PUBLISHED', published_at = now() WHERE id = ?", id);
}
```

### 3. `OutboxOrderService` — Kafka를 모르는 서비스

```java
@Transactional
public void placeOrder(String orderId, boolean simulateFailure) {
  orderRepository.save(orderId, "CREATED");
  outboxEventRepository.save(orderId, KafkaTopics.ORDER_EVENTS_OUTBOX, "ORDER_CREATED: " + orderId);
  log.info("주문 저장 + outbox 기록 완료 (같은 DB 트랜잭션): orderId={}", orderId);
  if (simulateFailure) {
    throw new IllegalStateException("강제 실패 — orders, outbox_event 둘 다 롤백되는지 확인");
  }
}
```

Ch15의 `OrderTransactionService`와 대비되는 지점은 이 메서드에 **Kafka 코드가 한 줄도 없다**는
것이다. `OrderRepository`는 `transaction` 패키지 걸 그대로 재사용했다.

### 4. `OutboxRelay` — 폴링 발행자, 그리고 파티션 키 누락

처음 작성한 버전은 `kafkaTemplate.send(event.topic(), event.payload())`로 키 없이 발행했다.
리뷰 과정에서 "같은 `aggregate_id`는 같은 파티션에 들어가야 순서가 보장된다"는 개념과 어긋난다는
지적을 받고 다음과 같이 고쳤다.

```java
@Scheduled(fixedDelay = 1000)
public void relay() {
  List<OutboxEventRow> pending = outboxEventRepository.findPending(50);
  for (OutboxEventRow event : pending) {
    kafkaTemplate.send(event.topic(), event.aggregateId(), event.payload());
    outboxEventRepository.markPublished(event.id());
    log.info("outbox 이벤트 전송 완료: id={}, topic={}, payload={}", event.id(), event.topic(), event.payload());
  }
}
```

`kafkaTemplate`은 Chapter 6부터 있던 공유 `KafkaTemplate<String, String>` 빈을 그대로
재사용했다 — `KafkaConfig` 수정은 필요 없었다.

### 5. `SchedulingConfig` — 스케줄링을 프로파일에 격리

```java
@Configuration
@EnableScheduling
@Profile("chapter18")
public class SchedulingConfig {
}
```

`@EnableScheduling`을 `LearningApplication`(전역)이 아니라 `chapter18` 프로파일 전용
`@Configuration`에 둬서, 다른 챕터 프로파일의 컨텍스트 로딩에는 스케줄링 인프라 자체가
등록되지 않게 격리했다.

### 6. 테스트 작성 — 원자성 / 롤백 / Relay 발행

```java
@Test
@DisplayName("정상 케이스: 주문과 outbox 기록이 같은 트랜잭션으로 커밋된다")
void placeOrderCommitsOrderAndOutboxTogether() {
  String orderId = "outbox-order-" + System.currentTimeMillis();
  outboxOrderService.placeOrder(orderId, false);
  assertThat(orderRepository.countByOrderId(orderId)).isEqualTo(1);
  assertThat(outboxEventRepository.countByAggregateId(orderId)).isEqualTo(1);
}

@Test
@DisplayName("실패 케이스: 예외가 나면 주문과 outbox 기록이 함께 롤백된다")
void placeOrderRollsBackOrderAndOutboxTogether() {
  String orderId = "outbox-order-faile-" + System.currentTimeMillis();
  assertThatThrownBy(() -> outboxOrderService.placeOrder(orderId, true))
      .isInstanceOf(IllegalStateException.class);
  assertThat(orderRepository.countByOrderId(orderId)).isZero();
  assertThat(outboxEventRepository.countByAggregateId(orderId)).isZero();
}

@Test
@DisplayName("Relay가 폴링으로 outbox 이벤트를 Kafka에 발행하고 PUBLISHED로 마킹한다")
void relayPublishesPendingEventAndMarksPublished() {
  String orderId = "outbox-order-relay-" + System.currentTimeMillis();
  outboxOrderService.placeOrder(orderId, false);
  assertThat(waitForKafkaMessage(orderId, Duration.ofSeconds(5))).isTrue();
  assertThat(waitForPublishedStatus(orderId, Duration.ofSeconds(5))).isTrue();
}
```

`waitForKafkaMessage`는 `read_uncommitted` 기본 컨슈머로 `order-events-outbox`를
`earliest`부터 폴링하며 `record.key().equals(orderId)`로 확인한다.
`waitForPublishedStatus`는 별도 리포지토리 메서드를 추가하지 않고 `JdbcTemplate`을
테스트에 직접 주입받아 `outbox_event.status`를 폴링했다 — 이 조회는 검증 목적으로만
쓰이고 프로덕션 코드에는 필요 없어서 리포지토리를 굳이 늘리지 않았다.

### 7. 테이블이 초기화되지 않는 문제 — `schema-locations` 오타

첫 테스트 실행 로그가 이상했다. `outbox_event`의 id가 1이 아니라 6부터 시작했고,
Relay 발행 확인 폴링에서 현재 테스트와 무관한 orderId(`outbox-order-1788226319627`
등 훨씬 이전 타임스탬프)가 여러 개 섞여 나왔다. `application-chapter18.yaml`을
확인해보니:

```yaml
spring:
  sql:
    init:
      mode: always
      schema-locations: classpath:schema-chapter17.sql   # 오타 — chapter18.sql이어야 함
```

`chapter17.yaml`을 복사해서 만들다가 이 줄을 못 고친 것이었다. `schema-chapter17.sql`은
`idempotent_order_log`만 만들고 `orders`/`outbox_event`는 건드리지 않으므로, 컨텍스트가
새로 뜰 때마다 `DROP TABLE`이 실행되지 않고 이전 실행들의 데이터가 계속 쌓이고 있었다.
Kafka 토픽도 브로커에 영구 저장되기 때문에(키 없이 발행했던 4단계 이전 버전의 레코드까지)
두 가지 잔여 데이터가 겹쳐서 로그를 헷갈리게 만든 것이었다. `schema-chapter18.sql`로
고치고 재실행하니 테이블이 매번 깨끗하게 초기화됐고, 세 테스트 모두 오염 없이 다시
통과했다.

```
i.hkarling.learning.outbox.OutboxRelay   : outbox 이벤트 전송 완료: id=..., topic=order-events-outbox, ...
i.h.l.outbox.OutboxOrderServiceTest      : Kafka에서 확인: value=ORDER_CREATED: outbox-order-relay-...

BUILD SUCCESSFUL in 11s
```

## 시행착오 / Q&A

**Q. `waitForKafkaMessage`에서 `consumerRecord.value() != null && consumerRecord.key().equals(orderId)`로
짰더니 `NullPointerException`이 났다. 왜인가?**
A. null 체크는 실제로 역참조하는 필드와 같은 필드에 해야 한다. `key()`를 호출하려는데
`value() != null`을 검사하는 건 다른 대상을 보호하는 것이라 의미가 없다. 4단계에서
키 없이 발행하던 예전 `OutboxRelay` 버전으로 이미 여러 번 테스트를 돌린 탓에
`order-events-outbox` 토픽에는 `key == null`인 메시지가 이미 쌓여 있었고, `earliest`부터
다시 읽는 테스트가 그 레코드를 만나 `key().equals(...)`에서 NPE가 났다. `key() != null &&
key().equals(orderId)`로 고쳐서 해결했다 — null 가능성이 있는 필드는 그 필드 자체를
가드해야 한다는 원칙을 다시 확인한 사례다.

**Q. "Relay를 잠깐 멈췄다가 재개해도 유실 없이 처리되는지" 시나리오를 별도 테스트로
만들어야 하는가?**
A. 검토 후 만들지 않기로 했다. `OutboxRelay.relay()`는 매 폴링마다 `findPending()`으로
그 시점의 DB 상태만 보고 동작하는 stateless 쿼리다 — Relay가 계속 돌고 있었든 잠깐
멈췄다가 재개했든, `PENDING` 행이 있으면 다음 폴링에서 그냥 집어서 처리한다. "재개 후
복구"라는 시나리오를 문자로 흉내 내려면 `OutboxRelay`에 테스트 전용 on/off 토글을
추가해야 하는데, 이건 유실 없음이라는 구조적 보장 자체를 증명하는 데 필수가 아니라고
판단해 프로덕션 코드에 손대지 않는 쪽을 택했다.

## 트레이드오프 / 실무 함정 / 안티패턴

**트레이드오프**: Polling publisher는 구현이 단순하고 기존 인프라(JdbcTemplate)만으로
충분하지만, 폴링 주기만큼 지연이 생기고 DB에 지속적인 폴링 부하를 준다. CDC(Debezium)는
지연이 거의 없고 DB 부하가 없지만 Kafka Connect 운영 복잡도가 추가된다.

**실무 함정**: `outbox_event`가 무한히 쌓인다 — Ch17의 `idempotent_order_log`와 마찬가지로
TTL/보존 정책에 따라 `PUBLISHED` 행을 정리하는 배치가 필요하다. 또한 Relay를 여러 인스턴스로
스케일아웃하면 동시에 같은 `PENDING` 행을 집어가는 중복 픽업이 발생할 수 있다 — 실무에서는
`SELECT ... FOR UPDATE SKIP LOCKED`로 행 단위 잠금을 걸어야 한다. 이번 챕터는 단일 인스턴스
전제라 이 문제를 코드로 다루지 않았다.

**안티패턴**: Kafka 발행 시 파티션 키 없이 보내는 것 — 이번 챕터에서 실제로 겪은
안티패턴이다. `aggregate_id`를 키로 넘기지 않으면 파티션이 라운드로빈으로 분산돼 같은
주문에 대한 이벤트 순서가 보장되지 않는다.

## 더 생각해볼 것

Outbox + Idempotent Consumer 조합이 만드는 "practical exactly-once"는 Chapter 20의
CQRS + Kafka(이벤트로 읽기 모델 동기화)에서 다시 등장할 가능성이 크다 — 쓰기 모델의
변경을 outbox로 안전하게 이벤트화하고, 그걸 멱등하게 소비해서 읽기 모델을 갱신하는
구조이기 때문이다. 또한 이번에 다루지 않은 CDC 기반 Outbox(Debezium)는 Kafka Connect를
실습 범위에 넣게 되면 별도로 다뤄볼 만하다.

## 최종 구성

`schema-chapter18.sql`, `application-chapter18.yaml`(datasource, sql.init, chapter18
group-id) 신규. `KafkaTopics`에 `ORDER_EVENTS_OUTBOX` 추가. `io.hkarling.learning.outbox`
패키지 신규 — `OutboxEventRepository`, `OutboxEventRow`(record), `OutboxOrderService`
(`@Profile("chapter18")`, `@Transactional`), `OutboxRelay`(`@Scheduled(fixedDelay = 1000)`,
`@Profile("chapter18")`), `SchedulingConfig`(`@EnableScheduling`, `@Profile("chapter18")`).
테스트 `OutboxOrderServiceTest`(원자적 커밋 / 롤백 / Relay 발행+마킹 3개 `@Test`).
`KafkaConfig`는 변경 없음(기존 공유 `kafkaTemplate` 재사용).

## ADR

### Decision
Outbox 패턴을 폴링 발행자(Polling publisher) 방식으로 구현한다. 비즈니스 트랜잭션은
`orders` + `outbox_event`를 같은 DB 트랜잭션으로 커밋하는 것으로 끝내고, Kafka 발행은
별도 `@Scheduled` Relay가 전담한다. 파티션 키는 `aggregate_id`를 그대로 사용한다.

### Drivers
Ch15에서 확인한 DB-Kafka 동기화의 크래시 윈도우 한계, Ch17 Follow-up에서 예고된 반대
방향 dual-write 문제를 해결하는 것이 목표였다. CDC(Debezium)는 Kafka Connect 인프라가
필요해 이 프로젝트 범위 밖으로 판단해 폴링 방식을 택했다.

### Alternatives
CDC 기반 Outbox(Debezium Outbox Event Router)를 검토했으나 Kafka Connect 클러스터
구성이 필요해 기각. Relay를 테스트에서 멈췄다 재개하는 시나리오를 위해 on/off 토글을
프로덕션 코드에 추가하는 방안도 검토했으나, `findPending()`이 stateless 쿼리라 구조적으로
이미 보장되는 성질이라고 판단해 추가하지 않았다.

### Consequences
`outbox_event`의 무한 증가(TTL/정리 배치)와 Relay 스케일아웃 시 동시 폴링 중복 픽업
(`SELECT ... FOR UPDATE SKIP LOCKED` 필요) 문제는 실무 함정으로만 기록하고 이번 챕터
범위 밖으로 남겼다. `application-chapter18.yaml`의 `schema-locations` 오타로 테이블
초기화가 안 되던 문제를 겪었다 — 챕터별 yaml을 복사해서 만들 때 프로파일 고유
설정(스키마 경로, group-id 등)을 빠짐없이 바꿨는지 확인하는 습관이 필요하다는 교훈을
남겼다.

### Follow-ups
Chapter 19 — Kafka Streams 기초. Chapter 20 — CQRS + Kafka에서 이번 챕터의 Outbox +
Idempotent Consumer 조합이 읽기 모델 동기화에 재등장할 가능성을 열어둔다.
