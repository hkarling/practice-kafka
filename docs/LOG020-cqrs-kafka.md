# LOG020 — CQRS + Kafka (이벤트로 읽기 모델 동기화)

## 배경 / 목표

Phase 3의 마지막 챕터. Ch18(Outbox 패턴)에서 이미 Command 측(쓰기: `orders` + `outbox_event`
+ Kafka 발행)을 만들어뒀다. 이번 챕터는 그 이벤트를 구독해서 조회 전용 뷰(`order_summary`)를
만드는 Query 측을 추가해 CQRS를 완성한다. 동시에 Ch17에서 "이번 챕터는 멱등키 체크만
다루고, 멱등한 연산으로 설계하는 방식은 다루지 않는다"고 남겨뒀던 부분(UPSERT 기반 멱등성)을
실제로 구현해서 Phase 3를 마무리한다.

## 개념 정리

### 1. 배경 — CQS에서 CQRS로

CQRS(Command Query Responsibility Segregation)의 뿌리는 Bertrand Meyer의
CQS(Command-Query Separation) 원칙이다. 원래는 메서드 레벨의 규칙이었다 — "커맨드(상태를
바꾸는 메서드)는 값을 반환하지 않아야 하고, 쿼리(값을 반환하는 메서드)는 상태를 바꾸지
않아야 한다." Greg Young이 이 아이디어를 아키텍처 레벨로 확장한 게 CQRS다. 메서드 하나가
아니라 시스템 전체를 쓰기를 처리하는 부분(Command)과 읽기를 처리하는 부분(Query)으로
분리한다. 하나의 도메인 모델로 복잡한 비즈니스 규칙(쓰기 시점의 검증, 트랜잭션 무결성)과
다양한 조회 요구사항(화면마다 다른 형태의 데이터, 검색, 통계)을 동시에 만족시키려다 보면
모델이 비대해지고 양쪽 요구사항이 서로 타협하게 된다는 문제의식에서 나왔다.

### 2. 목적 — 읽기와 쓰기는 근본적으로 다른 요구사항을 가진다

쓰기는 보통 정확성이 최우선이다 — 무결성 제약, 트랜잭션, 정규화된 구조로 데이터 중복과
모순을 막아야 한다. 읽기는 보통 속도와 형태의 유연성이 최우선이다 — 조인 없이 바로 화면에
뿌릴 수 있는 비정규화된 구조, 캐시, 검색 인덱스가 유리하다. CQRS는 이 두 요구사항을 하나의
모델이 억지로 감당하게 하지 않고, 아예 다른 모델·다른 저장소·다른 확장 전략을 쓸 수 있게
풀어준다.

### 3. 어디에 쓰이는가 — 실무 사례

- **이커머스 상품 카탈로그**: 상품 등록/재고 변경(쓰기)은 드물고 상품 조회/검색/필터링
  (읽기)은 압도적으로 많다. 쓰기 모델은 재고 무결성을, 읽기 모델은 검색 성능(별도 검색
  인덱스)을 각각 최적화한다.
- **소셜 미디어 피드**: 게시물 작성(쓰기)과 팔로워별로 조합된 피드 조회(읽기)는 완전히
  다른 처리 형태다. 새 게시물이 올라오면 이벤트로 흘려보내 각 팔로워의 피드(읽기 모델)를
  미리 만들어두는 "fan-out on write" 패턴과 자주 결합된다.
- **금융 거래 원장 vs 통계 대시보드**: 원장은 무결성이 생명이라 정규화된 구조를 유지해야
  하지만, 대시보드는 실시간 집계된 요약치만 있으면 된다.
- **Event Sourcing과의 결합**: CQRS는 Event Sourcing(상태 자체가 아니라 "무슨 일이
  있었는가"를 이벤트 로그로 저장)과 같이 언급되는 경우가 많다. 이벤트 로그가 유일한 진실의
  원천이고, 여러 읽기 모델을 그로부터 독립적으로 파생시킨다.

### 4. 왜 Kafka가 이 패턴과 잘 맞는가

Kafka의 로그 기반 특성(삭제되지 않고, 순서가 있고, 처음부터 다시 읽을 수 있는 로그 — Ch6)이
CQRS가 필요로 하는 것과 정확히 맞아떨어진다. 하나의 이벤트 스트림을 여러 소비자가 각자
독립적으로 구독해서, 서로 다른 형태의 읽기 모델(관계형 요약 테이블, 검색 인덱스, 캐시,
통계용 집계)을 자기 속도대로 만들어낼 수 있다. 소비자를 하나 더 늘려서 새로운 읽기 모델을
추가해도 쓰기 모델이나 다른 읽기 모델에 전혀 영향을 주지 않는다.

### 5. 읽기 모델 갱신은 반드시 멱등해야 한다 — Ch17의 예고를 여기서 완성

Outbox는 at-least-once다(Ch18). 같은 이벤트가 두 번 도착할 수 있다. Ch17에서 멱등성을 두
갈래로 나눴다.

1. 멱등키 체크 — "이미 처리했는가"를 별도 테이블에 기록해두고 확인 (Ch17에서 코드로 다룬
   방식)
2. **멱등한 연산으로 설계** — 몇 번을 실행해도 결과가 같은 연산으로 만듦 (Ch17에서는
   코드로 다루지 않고 남겨뒀던 부분)

이번 챕터는 2번을 구현한다. `order_summary`를 `UPSERT`(`INSERT ... ON CONFLICT DO
UPDATE`)로 갱신하면, 같은 이벤트가 몇 번을 재전달되든 결과는 항상 같다 — `status =
'ORDER_CREATED'`를 두 번 SET해도 문제없는 것과 같은 이치다. Ch17처럼 별도 멱등키
테이블이나 manual ack 컨트롤이 필요 없다. "연산 자체가 멱등하면 멱등키 체크 인프라 자체가
필요 없어진다"는 걸 실제 코드로 대비해볼 수 있다.

### 6. DB의 읽기 복제본(read replica)이나 Materialized View와는 뭐가 다른가

- Read replica는 쓰기 모델과 완전히 같은 스키마의 복사본이다. "쓰기 모델과 다른 형태로
  최적화된 읽기"는 못 만든다 — 그냥 같은 구조를 여러 대로 복제해서 부하만 분산하는 것이다.
- DB의 Materialized View는 같은 DB 안에서 갱신되는 경우가 많아, 갱신 시점에 쓰기 성능에
  영향을 줄 수 있고 DB 자체의 확장 한계를 그대로 물려받는다.
- CQRS + Kafka는 읽기 모델을 완전히 분리된 프로세스·저장소로 만들 수 있다. 쓰기 모델이
  Postgres든, 읽기 모델은 Elasticsearch든 Redis든 별개로 선택하고 독립적으로 확장할 수
  있다.

### 7. 트레이드오프 — 공짜가 아니다

Ch18/19에서 반복해서 나온 eventual consistency(쓰기와 읽기 모델 반영 사이의 지연)가
여기서도 그대로 적용된다. 추가로 시스템 복잡도 자체가 늘어난다 — 모델이 두 개가 되고, 그
사이를 잇는 동기화 파이프라인(이번 챕터의 Outbox+Projector)이 생기고, 장애 시나리오도
늘어난다. CQRS는 모든 시스템에 필요한 패턴이 아니라, 읽기/쓰기 요구사항이 확실히 갈리고 그
차이가 복잡도를 감수할 만큼 클 때 쓰는 패턴이다.

## 진행 과정

### 1. 스키마 — `schema-chapter20.sql`

```sql
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS outbox_event;
DROP TABLE IF EXISTS order_summary;

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

CREATE TABLE order_summary (
    order_id VARCHAR(100) PRIMARY KEY,
    status VARCHAR(50) NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT now()
);
```

`orders`/`outbox_event`는 Ch18과 같은 모양이다 — Command 측을 그대로 재사용하기 때문이다.
`order_summary`가 이번 챕터에서 새로 추가한 읽기 모델이다.

### 2. `application-chapter20.yaml`

```yaml
spring:
  application:
    name: learning
  datasource:
    url: jdbc:postgresql://localhost:5432/practice_kafka
    username: kafka
    password: kafka
  sql:
    init:
      mode: always
      schema-locations: classpath:schema-chapter20.sql
  kafka:
    consumer:
      group-id: chapter20-default-group
      auto-offset-reset: earliest
```

Ch19에서 겪었던 실수(항상-켜져있는 `OrderEventConsumer`가 `group-id` 없이 기동 실패)를
반복하지 않도록 처음부터 `kafka.consumer.group-id`를 넣었다.

### 3. Ch18 Command 측을 이 챕터에서도 쓸 수 있게 프로파일 확장

이번 챕터의 핵심은 "이미 있는 Command 측을 재사용"하는 것이므로, `OutboxOrderService`/
`OutboxRelay`/`SchedulingConfig`를 새로 복제하지 않고 `@Profile("chapter18")`을
`@Profile({"chapter18", "chapter20"})`으로 확장했다. `OutboxEventRepository`,
`OrderRepository`는 원래 `@Profile`이 없는 항상-활성 빈이라 손댈 필요가 없었다.

### 4. `cqrs` 패키지 — `OrderSummaryRepository`

```java
public void upsert(String orderId, String status) {
  jdbcTemplate.update("""
       INSERT INTO order_summary (order_id, status, updated_at)
       VALUES (?, ?, NOW())
       ON CONFLICT (order_id)
       DO UPDATE SET
          status = EXCLUDED.status,
          updated_at = NOW()
      """, orderId, status);
}
```

Ch17의 `tryMarkProcessed`(`INSERT ... ON CONFLICT DO NOTHING`)와 대비된다 — 거기서는
"이미 처리했으면 무시"였다면, 여기서는 "이미 있으면 최신 값으로 덮어쓰기"다. 둘 다 원자적
단일 SQL 문으로 처리해서 check-then-act 경쟁 조건을 피한다는 원칙은 같다.

### 5. `OrderSummaryProjector` — 읽기 모델 갱신 컨슈머

```java
@KafkaListener(
    id = "order-summary-projector",
    topics = KafkaTopics.ORDER_EVENTS_OUTBOX,
    groupId = "chapter20-order-summary-group")
public void listen(ConsumerRecord<String, String> consumerRecord) {
  String orderId = consumerRecord.key();
  String status = consumerRecord.value().split(":")[0];
  orderSummaryRepository.upsert(orderId, status);
  log.info("읽기 모델 갱신: orderId={}, status={}", orderId, status);
}
```

`OutboxRelay`가 키를 `aggregateId`(orderId)로 발행하도록 이미 고쳐뒀기 때문에(Ch18),
`consumerRecord.key()`를 바로 orderId로 쓸 수 있었다. `value()`는 `"ORDER_CREATED:
order-123"` 형태(Ch18 `OutboxOrderService`가 만드는 payload)라 `:` 앞부분만 잘라 상태로
썼다. Ch17과 달리 manual ack나 멱등키 저장소가 전혀 없다 — UPSERT 자체가 멱등하기 때문에
그 인프라가 필요 없다.

### 6. 첫 테스트 실패 — 옛날 키 없는 레코드가 발목을 잡다

테스트를 처음 돌렸을 때 두 테스트 모두 타임아웃으로 실패했다. 로그를 보니
`order-events-outbox-0` 파티션에서 `Seeking to offset 0` → `Record in retry and not yet
recovered`가 계속 반복되며 offset 0에서 진행이 안 되고 있었다.

원인은 Ch18을 진행하면서 `OutboxRelay`가 키 없이 발행하던 초기 버전으로 몇 번 테스트를
돌렸던 잔여 데이터였다(Ch18 LOG의 "시행착오 / Q&A"에서 다룬 바로 그 이슈). 그때 키가
`null`인 레코드가 `order-events-outbox` 토픽의 offset 0에 그대로 남아있었다.
`OrderSummaryProjector.listen()`이 `consumerRecord.key()`를 그대로 `order_summary.order_id`
(PRIMARY KEY, NOT NULL)에 넘기다 보니, 키가 `null`인 그 레코드를 만나 `INSERT`가 PK 제약
위반으로 실패하고, 별도 에러 핸들러가 없는 기본 리스너 컨테이너가 계속 같은 레코드를
재시도하며 멈춰 있었다.

```
docker compose exec kafka kafka-topics --delete --topic order-events-outbox --bootstrap-server localhost:9092
docker compose exec kafka kafka-topics --create --topic order-events-outbox --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092
```

토픽을 지우고 다시 만들어서 옛날 잔여 데이터를 치운 뒤 재실행하니 두 테스트 모두
통과했다. 로그에서 `cqrs-dup-...` orderId에 대해 "읽기 모델 갱신" 로그가 두 번(중복 이벤트
두 번 처리) 찍혔지만 `order_summary`에는 한 행만 남은 것도 확인했다.

## 시행착오 / Q&A

**Q. Ch18의 옛날 키 없는 레코드가 이번에도 문제가 될 거라고 예상할 수 있었나?**
A. Ch18 LOG(시행착오 5번, 파티션 키 안티패턴)에서 이미 "키 없이 발행하던 예전 버전으로
테스트를 돌린 탓에 토픽에 `key == null`인 메시지가 쌓여 있었다"는 걸 기록해뒀었다. 이번
챕터는 같은 토픽(`order-events-outbox`)을 재사용했기 때문에, 그 잔여 데이터가 그대로
넘어와서 또 문제가 됐다. 과거 챕터의 실무 함정 기록이 다음 챕터의 디버깅에 실제로
도움이 된 사례였다.

**Q. `OrderSummaryProjector`에 `consumerRecord.key() == null` 방어 코드를 넣어야 하는가?**
A. 이번엔 넣지 않았다 — 토픽을 정리한 이후로는 `OutboxRelay`가 항상 `aggregateId`를 키로
채워서 발행하므로, 정상 운영 경로에서는 `key()`가 `null`일 수 없다. 다만 이 토픽을 여러
챕터/여러 프로듀서가 공유해서 써온 이 프로젝트의 특성상, 과거 잔여 데이터나 다른 프로듀서의
실수로 키 없는 메시지가 다시 섞일 가능성은 남아있다 — 실무라면 방어적으로 `key() == null`인
레코드를 스킵하고 로그만 남기는 처리를 넣는 게 안전하다. "트레이드오프/실무 함정"에 남겨둔다.

## 트레이드오프 / 실무 함정 / 안티패턴

**트레이드오프**: 멱등한 연산(UPSERT)으로 설계하면 Ch17의 멱등키 체크(별도 테이블 + manual
ack)가 필요 없어져 코드가 단순해진다. 다만 모든 연산이 이렇게 "몇 번을 실행해도 결과가
같게" 설계될 수 있는 건 아니다 — `balance += amount`처럼 누적 연산은 멱등하게 만들기 어렵고,
그런 경우 Ch17의 멱등키 체크 방식으로 돌아가야 한다. 이번 챕터의 `order_summary`는 "최신
상태로 덮어쓰기"라는 연산 자체가 멱등해서 UPSERT가 자연스럽게 맞았을 뿐이다.

**실무 함정**: 하나의 Kafka 토픽을 여러 챕터(또는 실무에서는 여러 서비스/여러 기능)가
공유해서 오래 써오면, 과거의 잘못된 발행 로직이 만든 잔여 데이터가 훨씬 나중에 추가된
새로운 컨슈머를 오작동시킬 수 있다. 이번 챕터에서 실제로 겪은 사례가 정확히 이거였다 —
Ch18에서 이미 고쳐진 버그의 흔적이 Ch20의 테스트를 막았다. 토픽을 영구히 재사용하는
운영 환경이라면, "과거에 한 번이라도 잘못된 형식으로 발행된 적이 있는지"를 소비자 코드가
방어적으로 다뤄야 한다 — 오래된 이벤트 스키마에 대한 하위 호환성 처리가 여기에 해당한다.

**안티패턴**: 컨슈머가 메시지 필드(이번 챕터의 `consumerRecord.key()`)를 검증 없이 그대로
DB의 NOT NULL/PK 컬럼에 흘려보내는 것. Kafka는 스키마를 강제하지 않으므로, 컨슈머
입장에서는 브로커에서 오는 데이터도 일종의 외부 입력으로 보고 방어적으로 다뤄야 한다는
원칙(Ch12의 poison pill 논의와 같은 맥락)이 이번에도 적용됐다.

## 더 생각해볼 것

- `OrderSummaryProjector`에 `key() == null` 방어 코드를 추가해서, 향후 이 토픽에 다시
  잘못된 형식의 메시지가 섞여도 컨슈머 전체가 멈추지 않고 해당 레코드만 스킵하도록 만드는
  것.
- 이번 챕터는 읽기 모델을 하나(`order_summary`)만 만들었지만, 같은 `order-events-outbox`
  스트림에서 서로 다른 목적의 읽기 모델을 여러 개 독립적으로 파생시키는 것 — 예를 들어
  Ch19의 KTable 스타일 집계(이벤트 타입별 카운트)와 이번 챕터의 요약 뷰를 동시에 유지하는
  구조.
- 읽기 모델이 스키마 변경 등으로 손상되면, `order-events-outbox`를 처음부터 다시 읽어
  `order_summary`를 통째로 재구축하는 것 — Kafka가 로그 기반이라 가능한 이 재구축
  시나리오는 이번 챕터에서 코드로 다루지 않았다.
- Phase 4(이커머스/물류/금융 멀티모듈 실습)에서 서비스 간 경계가 명확히 나뉘면, 이번
  챕터의 Outbox+Projector 조합이 서비스 간 데이터 동기화의 기본 패턴으로 다시 등장할
  가능성이 크다.

## 최종 구성

`io.hkarling.learning.cqrs` 패키지 신규 — `OrderSummaryRepository`(UPSERT 기반),
`OrderSummaryProjector`(`@KafkaListener`, `@Profile("chapter20")`). `schema-chapter20.sql`,
`application-chapter20.yaml`(datasource, chapter20 group-id) 신규. Ch18의
`OutboxOrderService`/`OutboxRelay`/`SchedulingConfig`의 `@Profile`을 `{"chapter18",
"chapter20"}`으로 확장해 Command 측을 재사용. 테스트 `OrderSummaryProjectorTest`(정상 반영
/ 중복 재전달 UPSERT 멱등성 2개 `@Test`). `KafkaTopics`는 변경 없음(Ch18의
`ORDER_EVENTS_OUTBOX` 재사용).

## ADR

### Decision
읽기 모델(`order_summary`) 갱신은 별도 멱등키 테이블 없이 `INSERT ... ON CONFLICT DO
UPDATE`(UPSERT)로 구현한다. Command 측(Ch18)은 프로파일을 확장해 재사용하고, 새로
복제하지 않는다.

### Drivers
Ch17에서 예고된 "멱등한 연산으로 설계"하는 방식을 실제로 구현해보는 게 목표였다. Ch18의
Command 측을 그대로 재사용하는 것이 CQRS의 "이미 있는 쓰기 이벤트를 구독해서 읽기 모델을
만든다"는 개념을 가장 직접적으로 보여준다고 판단했다.

### Alternatives
`order_summary` 갱신에도 Ch17 스타일의 멱등키 체크 테이블을 추가하는 방식을 검토했으나,
UPSERT 자체가 멱등해서 불필요한 인프라라고 판단해 기각했다. Command 측을 chapter20 전용으로
복제하는 방식도 검토했으나, 코드 중복이 CQRS의 재사용성이라는 메시지와 어긋난다고 보고
프로파일 확장 쪽을 택했다.

### Consequences
`order-events-outbox` 토픽을 여러 챕터가 공유해서 오래 써온 탓에, Ch18에서 이미 고쳐진
버그(키 없는 발행)의 흔적이 이번 챕터의 테스트를 막는 실제 장애를 겪었다. 과거 챕터의
LOG에 남긴 실무 함정 기록이 이번 디버깅에 실질적으로 도움이 됐다는 것도 확인했다.
`OrderSummaryProjector`에 방어적 null 체크를 넣지 않은 채로 남겨뒀다는 점은 "더 생각해볼
것"에 명시해뒀다.

### Follow-ups
Phase 3 종료. Phase 4(이커머스/물류/금융 멀티모듈 실습)에서 이번 챕터의 Outbox+Projector
조합이 서비스 간 데이터 동기화 패턴으로 재등장할 가능성이 있다.
