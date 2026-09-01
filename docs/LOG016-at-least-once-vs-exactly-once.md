# LOG016 — at-least-once vs exactly-once

## 배경 / 목표

Phase 3 첫 챕터. 지금까지 여러 챕터에서 조각조각 건드려온 "배달 보장(delivery
semantics)"을 하나의 축으로 정리한다. Ch8(커밋 타이밍), Ch13(재시도로 인한
잠재적 중복), Ch15(DB-Kafka 트랜잭션 동기화와 그 한계)에서 이미 절반쯤
다뤘던 내용을, 이번엔 "유실/중복이 실제로 어떻게 발생하는지"를 대표적인
4가지 상황으로 직접 재현해서 눈으로 확인하는 게 목표다. 문제를 고치는 건
이번 챕터의 범위가 아니다 — 그건 Ch17(멱등성 설계)·Ch18(Outbox)의 몫이고,
이번 챕터는 "왜 그 챕터들이 필요한가"를 먼저 체감하는 단계다.

## 개념 정리

### 1. 세 가지 배달 보장

| 보장 | 커밋 시점 | 유실 | 중복 |
|---|---|---|---|
| at-most-once | 처리 전에 커밋 | 가능 | 없음 |
| at-least-once | 처리 후에 커밋 | 없음 | 가능 |
| exactly-once | 별도 메커니즘 필요 | 없음 | 없음 |

핵심은 "커밋을 언제 하느냐"가 유실이냐 중복이냐를 가른다는 것이다. 처리
전에 커밋하면(at-most-once) 처리 중 죽었을 때 그 메시지는 다시 안 온다
(오프셋은 이미 넘어갔으니) — 유실이다. 처리 후에 커밋하면(at-least-once)
처리는 끝났는데 커밋 직전에 죽으면 재시작 후 같은 메시지를 또 받는다 —
중복이다. 이 둘을 동시에 피하려면(exactly-once) 커밋과 처리 결과 반영을
원자적으로 묶는 별도 장치가 필요하다.

### 2. Kafka가 말하는 "exactly-once semantics(EOS)"의 실제 범위

공식 문서가 말하는 EOS는 consume-transform-produce 파이프라인, 즉 **Kafka
토픽 간 파이프라인 내부**에 한정된다. 구성 요소는 두 가지다 — 멱등성
프로듀서(`enable.idempotence`, Kafka 3.0+ 기본 true)는 프로듀서-브로커
사이 네트워크 재시도로 인한 브로커 레벨 중복 저장만 막는다(프로듀서가
전송 확인 응답을 못 받아 같은 배치를 재전송해도, 브로커가 시퀀스 번호로
같은 배치임을 알아채고 한 번만 저장한다). 트랜잭션(Ch15에서 다룬 DB-Kafka
동기화의 그 트랜잭션 메커니즘)은 "오프셋 커밋"과 "다음 토픽으로의 발행"을
원자적으로 묶어서, 컨슈머가 처리한 것과 그 결과를 발행한 것이 항상 짝을
이루게 한다. 이 두 장치 모두 "Kafka 안에서 Kafka가 관리하는 리소스"에만
적용된다는 공통점이 있다.

### 3. "Kafka 내부에서의 exactly-once"와 "외부 부작용까지 포함한 exactly-once"는 다른 이야기다

Ch15에서 이미 확인했듯, DB처럼 Kafka 트랜잭션에 안 묶이는 외부 시스템
쓰기가 걸리면 이 보장은 깨진다. Kafka의 EOS는 "Kafka가 관리하는 오프셋과
Kafka가 관리하는 발행"이라는 자기 완결적인 두 대상 사이의 원자성이지,
"컨슈머가 그 메시지를 처리하면서 발생시키는 모든 부작용"(DB 쓰기, 외부
API 호출 등)까지 포함하는 원자성이 아니다. 이 갭은 Kafka만으로는 못
메우고, 컨슈머 멱등성 설계(Ch17)나 Outbox(Ch18)가 메우는 영역이다.

### 4. 프로듀서 멱등성이 막아주는 것과 못 막아주는 것

`enable.idempotence`가 막는 건 "같은 `send()` 호출 하나가 네트워크 문제로
클라이언트 내부에서 재시도될 때"의 브로커 레벨 중복 저장이다 — 프로듀서가
PID(Producer ID)와 시퀀스 번호를 매겨서 보내고, 브로커는 이미 본 시퀀스
번호의 재전송을 무시한다. 하지만 애플리케이션 코드가 **별개의 `send()`
호출을 의도적으로(또는 실수로) 두 번** 하면, 프로듀서 입장에서는 그냥
"다른 두 건의 정상 발행 요청"이라 시퀀스 번호도 각각 새로 매겨지고 전혀
못 막는다 — 이번 챕터에서 실측으로 확인했다. "멱등성 프로듀서를 쓰면
중복이 안 생긴다"는 오해가 실무에서 흔한데, 정확히는 "네트워크 계층의
숨겨진 재시도로 인한 중복"만 막아준다는 좁은 범위를 기억해야 한다.

## 진행 과정

새 패키지 `io.hkarling.learning.semantics`에 4가지 대표 시나리오를 구성했다.
기존 `KafkaConfig`의 빈(`manualAckKafkaListenerContainerFactory`(Ch8),
`kafkaTemplate`(Ch13), `transactionalKafkaTemplate`(Ch15))을 전부 재사용해서
**이번 챕터는 `KafkaConfig`에 새 빈을 하나도 추가하지 않았다** — Ch13·14·15
ADR에서 세 챕터 연속 지적됐던 "`KafkaConfig` 비대화"를 이번엔 만들지 않는
방향으로 설계했다.

### 1. 인프라 구성

```yaml
# application-chapter16.yaml
spring:
  datasource:
    url: jdbc:postgresql://localhost:5432/practice_kafka
    username: kafka
    password: kafka
  sql:
    init:
      mode: always
      schema-locations: classpath:schema-chapter16.sql
  kafka:
    consumer:
      group-id: chapter16-default-group
      auto-offset-reset: earliest
```

```sql
-- schema-chapter16.sql
DROP TABLE IF EXISTS order_processing_log;
CREATE TABLE order_processing_log (
    id BIGSERIAL PRIMARY KEY,
    order_id VARCHAR(100) NOT NULL,
    processed_at TIMESTAMP NOT NULL DEFAULT now()
);
```

Ch15의 `orders` 테이블(`order_id UNIQUE`)과 달리, 이번 테이블은 **의도적으로
UNIQUE 제약을 두지 않았다** — 중복이 실제로 몇 건 쌓이는지 행 개수로 그대로
드러나야 하기 때문이다. `KafkaTopics`엔 이번 챕터 전용 토픽
`ORDER_EVENTS_SEMANTICS = "order-events-semantics"`를 추가했다 — 다른
챕터의 누적 메시지와 섞이지 않게 하려는 목적(Ch12와 동일한 이유).

### 2. 시나리오 A — at-most-once (유실)

```java
// AtMostOnceConsumer.java
@KafkaListener(
    id = "at-most-once-listener",
    topics = KafkaTopics.ORDER_EVENTS_SEMANTICS,
    groupId = "chapter16-at-most-once-group",
    containerFactory = "manualAckKafkaListenerContainerFactory")
public void listen(ConsumerRecord<String, String> consumerRecord, Acknowledgment ack) {
  ack.acknowledge(); // 처리도 안 했는데 먼저 커밋

  if (consumerRecord.value() != null && consumerRecord.value().startsWith("crash:")) {
    log.warn("크래시 시뮬레이션 — 오프셋은 이미 커밋되어 이 메시지는 다시 오지 않는다: orderId={}", consumerRecord.key());
    return; // DB 반영 없이 종료
  }

  logRepository.insert(consumerRecord.key());
  log.info("처리 완료: orderId={}", consumerRecord.key());
}
```

검증은 `AdminClient.listConsumerGroupOffsets()`로 커밋된 오프셋을 직접
조회해서, 발행 시점의 `RecordMetadata` 오프셋보다 커밋 오프셋이 앞서있는지
확인하는 방식을 썼다. 같은 그룹 id로 raw consumer를 만들어 재구독하는 방법도
고려했지만, 그러면 **이미 살아있는 `@KafkaListener` 컨테이너와 같은 그룹에
새 멤버가 끼어들어 리밸런싱을 유발**한다(Ch9에서 다룬 그 리밸런싱이 테스트
코드 때문에 부작용으로 일어나는 셈). `AdminClient`로 오프셋만 조회하는 건
그룹에 "가입"하지 않는 순수 메타데이터 질의라 이 문제가 없다.

### 3. 시나리오 B — at-least-once (중복)

```java
// AtLeastOnceConsumer.java
@KafkaListener(
    id = "at-least-once-listener",
    topics = KafkaTopics.ORDER_EVENTS_SEMANTICS,
    groupId = "chapter16-at-least-once-group",
    containerFactory = "manualAckKafkaListenerContainerFactory")
public void listen(ConsumerRecord<String, String> consumerRecord, Acknowledgment ack) {
  logRepository.insert(consumerRecord.key()); // 처리(부작용)를 먼저
  log.info("처리(DB 기록) 완료, 커밋 전: orderId = {}", consumerRecord.key());

  if (consumerRecord.value() != null && consumerRecord.value().startsWith("crash:")) {
    throw new IllegalStateException("크래시 시뮬레이션 — 커밋 전에 실패, 재전달된다");
  }

  ack.acknowledge(); // 처리 성공을 확인한 뒤에야 커밋
}
```

시나리오 A와 `ack.acknowledge()` 위치만 정반대다. 커밋 전에 예외가 나면
오프셋이 안 넘어간 채 남고, 별도 에러 핸들러를 안 달아뒀으니 Spring Kafka
프레임워크 기본값(`DefaultErrorHandler`의 `FixedBackOff(0, 9)` — Ch8에서
다룬 그 설정)이 적용돼 간격 없이 총 10번 같은 레코드로 `listen()`이
재호출된다. 실행 결과 실제로 같은 `orderId`에 대해 `order_processing_log`
행이 여러 개 쌓이는 것을 확인했다.

### 4. 시나리오 C — 프로듀서 멱등성의 실제 범위

```java
kafkaTemplate.send(KafkaTopics.ORDER_EVENTS_SEMANTICS, orderId, "EVENT:" + orderId).get();
kafkaTemplate.send(KafkaTopics.ORDER_EVENTS_SEMANTICS, orderId, "EVENT:" + orderId).get();
```

`kafkaTemplate`은 `enable.idempotence=true`(Kafka 4.2.1 기본값)인 멱등성
프로듀서다 — 로그에서 `Instantiated an idempotent producer`, `ProducerId
set to N with epoch 0`으로 직접 확인했다. 그런데도 앱 코드가 `send()`를
의도적으로 두 번 호출하면, 매번 시퀀스 번호가 다른 별개의 전송으로
취급되어 토픽엔 그대로 2건이 쌓인다. `read_uncommitted` raw consumer로
확인했다(매번 새 `UUID` 기반 그룹이라 leftover/리밸런싱 간섭 없음).

### 5. 시나리오 D — Kafka 트랜잭션(EOS)의 실제 범위

```java
// RedeliveryTransactionService.java
private final OrderProcessingLogRepository logRepository;
private final KafkaTemplate<String, String> transactionalKafkaTemplate;

@Transactional
public void recordEvent(String orderId) {
  logRepository.insert(orderId);
  transactionalKafkaTemplate.send(KafkaTopics.ORDER_EVENTS_SEMANTICS, orderId, "EVENT_RECORDED:" + orderId);
  log.info("이벤트 기록(DB) + 발행 완료: orderId={}", orderId);
}
```

같은 `orderId`로 `recordEvent()`를 두 번 호출해서 "업스트림이 같은 이벤트를
재전달했다"는 상황을 흉내냈다. 실행 결과 `order_processing_log`엔 2행,
`read_committed`로 확인한 토픽에도 진짜로 커밋된 레코드 2건이 쌓였다 — 각
호출은 자기 안에서 DB+Kafka가 원자적으로 묶이지만(Ch15에서 검증한 그
동기화는 정상 동작), "같은 이벤트가 두 번 들어오는 것" 자체는 트랜잭션이
막아주는 범위 밖이라는 게 실측으로 증명됐다.

## 시행착오 / Q&A

**Q. 시나리오 A에서 크래시 분기를 타도 DB에 계속 기록되는 문제가 있었다.**
A. `crash:` 분기 안에서 WARN 로그만 찍고 `return;`을 빠뜨려서, 그 아래
`logRepository.insert()`가 그대로 실행되고 있었다. "유실"을 재현하려면
분기 안에서 처리를 확실히 건너뛰어야 하는데, 이 흐름 제어 실수 때문에
매번 정상 처리로 흘러가고 있었다.

**Q. 시나리오 B가 `Thread.sleep(15000)`으로는 계속 실패했다.**
A. `chapter16-at-least-once-group`이 `earliest`로 시작하는데,
시나리오 A가 같은 토픽에 계속 `"crash:" + orderId` 형식 메시지를 쌓아왔고
(접두사만 보고 크래시 여부를 판단하므로 orderId 무관하게 전부 크래시로
처리됨), 재실행할 때마다 이 leftover가 늘어나서 이 그룹이 "이번에 보낸
메시지"에 도달하기까지 걸리는 시간 자체가 계속 길어지고 있었다. 고정
`Thread.sleep`은 재실행할수록 필요한 시간이 계속 늘어나는 밑 빠진 독이라,
조건이 만족될 때까지 폴링하는 방식(`waitForCountGreaterThan`, 500ms 간격,
데드라인까지 반복)으로 바꾸고, 근본적으로는 토픽을 삭제·재생성해서
leftover를 정리하는 것으로 해결했다. Ch8·Ch9·Ch13에서 반복돼온 "leftover
누적" 문제가 이번엔 "다음 메시지 도달 지연으로 인한 테스트 타임아웃"이라는
새로운 형태로 나타난 사례다.

**Q. `order_processing_log.order_id`가 `VARCHAR(50)`이었는데 일부
시나리오에서 값이 안 들어갔다.**
A. 접두사 + `UUID.randomUUID()`(36자) 조합의 길이를 실제로 재보니
`"order-lost-"`(11자, 시나리오 A)는 47자로 안 넘었지만 `"order-dup-consumer-"`
(19자, 시나리오 B)는 55자, `"order-dup-producer-"`(20자, 시나리오 C 예정)는
56자로 50자를 넘었다. PostgreSQL은 `VARCHAR(50)` 초과 값을 자르는 게 아니라
`value too long for type character varying(50)` 예외를 던진다.
`VARCHAR(100)`으로 넓혀서 해결했다.

**Q. 시나리오 D에서 트랜잭션 동기화가 전혀 안 되는 것처럼 보였다.**
A. `RedeliveryTransactionService`의 필드명이 `kafkaTemplate`으로 되어
있었다. 이 프로젝트엔 `KafkaTemplate<String, String>` 타입 빈이 3개
(`acksZeroKafkaTemplate`, `kafkaTemplate`, `transactionalKafkaTemplate`)
있어서, 필드명과 정확히 일치하는 빈으로 매칭되는 Spring의 동작 때문에
의도한 `transactionalKafkaTemplate`이 아니라 **트랜잭션 아이디가 없는
일반 `kafkaTemplate`**에 조용히 연결되고 있었다. 컴파일 에러도, 기동 시
에러도 없이 조용히 잘못 연결된다는 점에서 Ch12(`ConditionalOnMissingBean`
raw 타입 문제)·Ch15(반대 방향 — 필드명을 정확히 맞춰야 올바른 빈에
연결됨)에 이어 같은 계열의 실수가 세 번째로 재현된 사례다. 필드명을
`transactionalKafkaTemplate`으로 고쳐서 해결했다.

## 트레이드오프 / 실무 함정 / 안티패턴

**트레이드오프**: exactly-once에 가까워질수록 지연시간·복잡도 비용이
커진다. Kafka의 EOS조차 "프로듀서 세션 내부의 네트워크 재시도"만
커버하고, 애플리케이션 코드의 재호출이나 업스트림의 재전달은 전혀 못
막는다 — 그래서 대부분의 실무 시스템은 at-least-once + 컨슈머 멱등성
조합(Ch17)을 택한다.

**실무 함정**: "Kafka가 exactly-once를 지원한다"는 말을 "내 비즈니스
로직도 자동으로 exactly-once가 된다"로 오해하기 쉽다 — 이번 챕터에서
실측한 것처럼 그 보장은 Kafka 파이프라인 내부에만 해당된다. 특히 "업스트림
시스템이 같은 이벤트를 재전달하는 상황"(네트워크 타임아웃으로 응답을 못
받은 발신자가 안전하게 재시도하는 흔한 패턴)은 Kafka의 EOS로 전혀 못
막는다는 게 시나리오 D에서 실측으로 드러났다 — 재전달된 두 번째 호출도
프로듀서 입장에선 트랜잭션이 정상적으로 커밋되는 "정당한 새 이벤트"일
뿐이다. 또한 같은 제네릭 타입의 `KafkaTemplate` 빈이 여러 개 있는
프로젝트에서는 필드명 하나 잘못 짓는 것만으로 컴파일 에러 없이 트랜잭션
특성이 다른 프로듀서에 연결될 수 있다 — 커스텀 Kafka 빈이 누적될수록 이
위험도 커진다.

**안티패턴**: 고정된 `Thread.sleep`으로 "이 정도면 충분하겠지"라고
판단하는 것. leftover가 쌓일수록 필요한 대기 시간이 계속 늘어나는 구조라,
재실행할수록 점점 더 자주 실패하게 된다. 조건이 실제로 만족될 때까지
폴링하는 방식이 더 안전하고, 재현 자체를 깨끗하게 하고 싶으면 토픽을
리셋하는 습관이 필요하다(Ch13에서도 이미 나온 교훈).

## 더 생각해볼 것

`order_processing_log`(UNIQUE 없음)와 `orders`(Ch15, `order_id UNIQUE`)를
나란히 놓고 보면, "중복을 막는 가장 단순한 방법 중 하나가 이미 DB 제약으로
가능하다"는 힌트가 보인다 — Ch17에서 이걸 멱등키 체크 패턴으로 정식화할
예정이다. 또한 시나리오 B의 재시도(`DefaultErrorHandler` 소진)와 Ch13의
DLQ가 사실 같은 메커니즘(`FixedBackOff` 소진 후 복구 단계)의 다른
결말이라는 것도 다시 확인했다 — 재시도 소진 후 "그냥 포기"(이번 챕터)와
"DLT로 발행"(Ch13)의 차이만 있을 뿐이다.

## 최종 구성

`application-chapter16.yaml`, `schema-chapter16.sql`(`order_processing_log`,
`VARCHAR(100)`) 신규. `KafkaTopics`에 `ORDER_EVENTS_SEMANTICS` 추가.
`io.hkarling.learning.semantics` 패키지 신규 — `OrderProcessingLogRepository`,
`AtMostOnceConsumer`, `AtLeastOnceConsumer`, `RedeliveryTransactionService`
(모두 `@Profile("chapter16")`), 테스트 `DeliverySemanticsTest`(시나리오
A/B/C/D 4개 `@Test`). `KafkaConfig`는 변경 없음 — 기존 빈
(`manualAckKafkaListenerContainerFactory`, `kafkaTemplate`,
`transactionalKafkaTemplate`)을 전부 재사용했다.

## ADR

### Decision
이번 챕터는 `KafkaConfig`에 새 빈을 추가하지 않고 기존 빈만 재사용한다.
검증용 raw consumer는 매번 새 UUID 기반 그룹을 쓰거나(시나리오 C/D,
Ch15 패턴), 아예 그룹에 가입하지 않는 `AdminClient` 오프셋 조회를
쓴다(시나리오 A). 중복 관찰용 테이블(`order_processing_log`)엔 의도적으로
UNIQUE 제약을 두지 않는다.

### Drivers
Ch13·14·15에서 세 챕터 연속 "`KafkaConfig` 비대화"가 ADR에 지적됐던 걸
더 키우고 싶지 않았고, 마침 이번 챕터에 필요한 속성 조합(수동 ack,
일반 발행, 트랜잭셔널 발행)이 전부 이미 존재했다. 같은 그룹으로 raw
consumer를 만들면 살아있는 리스너 컨테이너와 리밸런싱이 얽힌다는 걸
시나리오 A 설계 중에 확인해서, 그룹 비가입 방식(`AdminClient`)을
새로 도입했다. UNIQUE 제약을 안 둔 건 "막는 것"이 아니라 "문제를 눈으로
보는 것"이 이번 챕터의 목적이기 때문이다.

### Alternatives
시나리오 A 검증에 같은 그룹의 raw consumer를 재사용하는 방법도 있었으나,
리밸런싱 부작용 때문에 기각. `order_processing_log`에 처음부터 UNIQUE
제약을 걸어 중복을 막아버리는 설계도 고려했지만, 그러면 이번 챕터가
증명하려는 문제 자체가 안 보이게 되어 기각 — 그 제약을 활용한 멱등성
설계는 Ch17로 미룬다.

### Consequences
`KafkaConfig`가 이번엔 커지지 않았다 — 다만 앞으로도 매 챕터 재사용
가능한 속성 조합인지 먼저 확인하는 습관을 유지할 필요가 있다. leftover
누적이 "유실"뿐 아니라 "테스트 타임아웃"으로도 나타날 수 있다는 게 이번에
새로 드러나서, 앞으로 여러 시나리오가 같은 토픽/유사 접두사를 공유하는
실습에서는 처음부터 폴링 기반 대기를 기본값으로 쓰는 게 나을 것 같다.

### Follow-ups
Chapter 17 — 멱등성 설계 (Producer 멱등성, Consumer 멱등성). 이번 챕터의
`order_processing_log` 패턴을 멱등키 체크로 정식화하는 것부터 시작할
예정.
