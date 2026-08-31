# LOG017 — 멱등성 설계 (Producer 멱등성, Consumer 멱등성)

## 배경 / 목표

Ch16에서 "Kafka의 exactly-once는 프로듀서 세션 내부 재시도만 커버하고, 앱 코드의
재호출이나 업스트림의 재전달은 못 막는다"는 갭을 실측으로 확인했다. 이번 챕터는
그 갭을 애플리케이션 레벨에서 메우는 방법 — 컨슈머 멱등성 설계 — 을 다룬다.
Ch16 LOG의 "더 생각해볼 것"에서 예고했던 대로, `order_processing_log`(UNIQUE
없음)에 UNIQUE 제약만 추가하면 바로 멱등키 체크 패턴이 된다는 힌트를 그대로
formalize하는 게 이번 챕터의 출발점이다.

## 개념 정리

- **Producer 멱등성의 실질적 한계**: `enable.idempotence=true`는 같은 프로듀서
  세션 안에서 시퀀스 번호로 브로커 레벨 중복 저장을 막는다. 하지만 이 PID는
  프로듀서가(트랜잭션 없이) 재시작되면 새로 발급된다 — "재시작 전에 보냈는지
  확신 못 하고 재전송"하는 흔한 장애 복구 시나리오는 프로듀서 멱등성 범위
  밖이다. 결론적으로 엔드투엔드 중복 방지는 프로듀서 혼자 못 하고, **컨슈머가
  최종 방어선**이 되어야 한다.
- **Consumer 멱등성의 두 갈래**:
  1. 멱등키 체크 — "이미 처리했는가"를 기록해두고 처리 전에 확인.
  2. 멱등한 연산으로 설계 — 몇 번을 실행해도 결과가 같은 연산으로 만듦
     (`balance = balance + 100` 대신 `balance = 100` 같은 절대값 SET, 또는
     UPSERT). 도메인마다 적용 가능 여부가 달라 이번 챕터는 1번만 코드로
     다뤘다.
- **원자성이 핵심**: 멱등키 기록과 실제 비즈니스 처리가 다른 트랜잭션에
  있으면 "기록은 됐는데 처리는 안 됨" 같은 틈이 생긴다. `INSERT ... ON
  CONFLICT DO NOTHING`처럼 체크와 기록을 원자적 연산 하나로 합치는 게
  안전하다 — SELECT 후 INSERT하는 check-then-act 방식은 그 사이에 경쟁
  조건 여지가 있다.

## 진행 과정

`KafkaConfig`는 이번에도 손대지 않았다 — `manualAckKafkaListenerContainerFactory`,
`kafkaTemplate` 재사용으로 충분했다(Ch16과 동일한 이유).

### 1. 인프라 구성

```sql
-- schema-chapter17.sql
DROP TABLE IF EXISTS idempotent_order_log;
CREATE TABLE idempotent_order_log (
    id BIGSERIAL PRIMARY KEY,
    order_id VARCHAR(100) NOT NULL UNIQUE,
    processed_at TIMESTAMP NOT NULL DEFAULT now()
);
```

Ch16의 `order_processing_log`와 컬럼 구성은 동일하고 `order_id UNIQUE`만
추가됐다 — 의도적 대조. `KafkaTopics`엔 `ORDER_EVENTS_IDEMPOTENT =
"order-events-idempotent"`를 새로 추가해 다른 챕터 메시지와 섞이지 않게 했다.

### 2. 멱등키 체크 — 원자적 연산으로

```java
// IdempotentOrderLogRepository.java
public boolean tryMarkProcessed(String orderId) {
  int updated = jdbcTemplate.update(
      "INSERT INTO idempotent_order_log (order_id) VALUES (?) ON CONFLICT (order_id) DO NOTHING",
      orderId);
  return updated == 1; // true = 신규 처리, false = 이미 처리된 이벤트
}
```

### 3. 컨슈머 — 처리 전 멱등키 체크

```java
// IdempotentConsumer.java
@KafkaListener(
    id = "idempotent-listener",
    topics = KafkaTopics.ORDER_EVENTS_IDEMPOTENT,
    groupId = "chapter17-idempotent-group",
    containerFactory = "manualAckKafkaListenerContainerFactory")
public void listen(ConsumerRecord<String, String> consumerRecord, Acknowledgment ack) {
  String orderId = consumerRecord.key();

  if (!idempotentOrderLogRepository.tryMarkProcessed(orderId)) {
    log.info("이미 처리된 이벤트 — skip: orderId={}", orderId);
    ack.acknowledge();
    return;
  }
  log.info("신규 이벤트 처리 완료: orderId={}", orderId);

  if (consumerRecord.value() != null && consumerRecord.value().startsWith("crash:")) {
    throw new IllegalStateException("크래시 시뮬레이션 — 멱등키는 이미 커밋됐다, 재전달돼도 안전한지 확인");
  }

  ack.acknowledge();
}
```

`tryMarkProcessed`의 insert는 오토커밋이라 리스너 메서드 안에서 나중에 예외가
나도 이미 DB에 반영돼 있다는 게 포인트다.

### 4. 검증 — 재전달 중복 방지

같은 orderId로 두 번 전송(재전달 흉내):

```
i.h.l.idempotency.IdempotentConsumer : 신규 이벤트 처리 완료: orderId=order-idem-dup-f119c22d-...
i.h.l.idempotency.IdempotentConsumer : 이미 처리된 이벤트 — skip: orderId=order-idem-dup-f119c22d-...
```

`countByOrderId == 1` 통과. 테스트는 `Thread.sleep` 고정 대기 대신, 두 번째
메시지의 오프셋을 `AdminClient.listConsumerGroupOffsets()`로 폴링해 실제
커밋됐는지 확인하는 방식을 썼다(Ch16 `AtMostOnceConsumer` 테스트의
`alreadyCommittedPast` 패턴을 폴링형으로 확장) — Ch16에서 지적된 "고정 sleep은
leftover가 쌓일수록 점점 불안정해진다"는 안티패턴을 반복하지 않기 위함이다.

### 5. 검증 — 크래시 후 재시도가 중복 없이 안전한지

`"crash:" + orderId`로 전송:

```
i.h.l.idempotency.IdempotentConsumer     : 신규 이벤트 처리 완료: orderId=order-idem-crash-16e912ab-...
o.s.k.l.KafkaMessageListenerContainer    : Seeking to offset 2 for partition order-events-idempotent-0
o.s.k.l.KafkaMessageListenerContainer    : Record in retry and not yet recovered
i.h.l.idempotency.IdempotentConsumer     : 이미 처리된 이벤트 — skip: orderId=order-idem-crash-16e912ab-...
```

`countByOrderId == 1` 통과. 예상대로 **재시도 1번 만에 즉시 복구**됐다 — 첫
시도에서 멱등키가 이미 커밋됐기 때문에, 재시도(같은 레코드 재처리) 시점엔
`tryMarkProcessed`가 `false`를 반환해 예외 없이 바로 `ack`한다. `manualAck
KafkaListenerContainerFactory`엔 커스텀 에러 핸들러가 없어 Spring Kafka
기본값(`DefaultErrorHandler`의 `FixedBackOff(0, 9)`, 최대 10번 재시도)이
적용되는데, 이번엔 그 10번을 다 소진할 필요가 없었다. Ch16의
`AtLeastOnceConsumer`가 같은 크래시 시나리오에서 멱등성 없이 10번 재시도를
다 거치며 중복 행을 쌓았던 것과 나란히 대조된다.

## 시행착오 / Q&A

이번 챕터는 가이드대로 특별한 삽질 없이 진행됐다.

## 트레이드오프 / 실무 함정 / 안티패턴

**트레이드오프**: DB 유니크 제약 기반 멱등성 체크는 메시지마다 DB round-trip이
하나 추가된다. 처리량이 매우 높은 경로에서는 Redis 같은 인메모리 스토어로
멱등키를 체크하는 선택지도 있지만, 그러면 "커밋됐다"는 내구성 보장이 약해지는
트레이드오프가 생긴다.

**실무 함정**: `idempotent_order_log`가 무한히 쌓인다 — 실무에서는 TTL/보존
기간을 정해 오래된 멱등키를 정리하는 배치가 필요하다(단, 정리 주기가 재전달
가능 기간보다 짧으면 안 된다). 또한 이번 챕터는 단순화를 위해 `orderId`(비즈니스
키)를 그대로 멱등키로 썼는데, 실무에서는 "같은 주문에 대한 서로 다른 정당한
이벤트"(주문생성, 주문취소 등)까지 하나로 묶여 두 번째 이벤트가 막혀버릴 수
있다 — 진짜 멱등키는 이벤트마다 고유한 식별자(예: 페이로드의 UUID `eventId`)여야
하는 경우가 많다.

**안티패턴**: `SELECT` 후 `INSERT`하는 check-then-act 방식 — 두 쿼리 사이에
경쟁 조건이 생길 수 있다. `INSERT ... ON CONFLICT DO NOTHING`처럼 원자적
연산 하나로 합치는 게 안전하다. 인메모리 `Set`/`HashMap`으로 멱등키를 관리하는
것도 흔한 실수 — 컨슈머 재시작이나 다중 인스턴스 스케일아웃 상황에서 전혀
보호되지 않는다.

## 더 생각해볼 것

컨슈머 멱등성은 "이미 도착한 메시지의 중복 처리"는 막아주지만, "DB 쓰기는
됐는데 그 결과를 다시 Kafka로 발행하는 데 실패"하는 반대 방향 문제(Ch15에서
다룬 DB-Kafka 동기화의 나머지 절반)는 못 막는다 — 이건 Chapter 18(Outbox
패턴)의 영역이다.

## 최종 구성

`application-chapter17.yaml`, `schema-chapter17.sql`(`idempotent_order_log`,
`order_id UNIQUE`) 신규. `KafkaTopics`에 `ORDER_EVENTS_IDEMPOTENT` 추가.
`io.hkarling.learning.idempotency` 패키지 신규 — `IdempotentOrderLogRepository`,
`IdempotentConsumer`(`@Profile("chapter17")`), 테스트
`IdempotentConsumerTest`(재전달 중복 방지 / 크래시 후 재시도 안전성 2개
`@Test`). `KafkaConfig`는 변경 없음.

## ADR

### Decision
멱등키 체크는 DB UNIQUE 제약 + `INSERT ... ON CONFLICT DO NOTHING`으로,
체크와 기록을 원자적 연산 하나로 합쳐서 처리한다. 멱등키는 이번 챕터에서
단순화를 위해 `orderId`(비즈니스 키)를 그대로 사용한다.

### Drivers
Ch16에서 예고된 "UNIQUE 제약만 추가하면 멱등키 체크 패턴이 된다"는 힌트를
그대로 formalize하는 것이 목표였다. check-then-act 방식은 경쟁 조건 여지가
있어 배제했다.

### Alternatives
`SELECT COUNT` 후 조건부 `INSERT`(check-then-act) 방식을 고려했으나 두 쿼리
사이 원자성이 없어 기각. `INSERT` 실행 후 `DataIntegrityViolationException`을
catch하는 방식도 대안이었으나, 예외 기반 흐름 제어가 핫패스에서 오버헤드·로그
노이즈를 만들어 `ON CONFLICT DO NOTHING`을 택했다. 인메모리 `Set`/`HashMap`
기반 dedup도 재시작·스케일아웃에 취약해 기각.

### Consequences
`idempotent_order_log`의 무한 증가 문제(TTL/보존 정책)는 이번 챕터 범위 밖으로
남겨뒀다 — 실무 함정으로만 기록. `orderId`를 멱등키로 쓴 것은 단순화한
선택이고, 실제로는 이벤트별 고유 식별자가 필요한 경우가 많다는 점을 짚어뒀다.

### Follow-ups
Chapter 18 — Outbox 패턴. 컨슈머 멱등성이 못 막는 반대 방향(DB 쓰기는 성공,
Kafka 발행은 실패) 문제를 다룰 예정.
