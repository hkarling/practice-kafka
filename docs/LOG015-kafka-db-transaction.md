# LOG015 — 트랜잭션: Kafka 트랜잭션과 DB 트랜잭션 조합

## 배경 / 목표

Phase 2 마지막 챕터. DB에 상태를 저장하고 그 결과를 Kafka로 발행하는 건 서로 다른 두
시스템에 대한 쓰기다 — 순서대로 실행해도 그 사이에 프로세스가 죽으면 "DB엔 저장됐는데
이벤트는 안 나간" 상태, 또는 "이벤트는 나갔는데 DB엔 없는" 상태가 생길 수 있다(dual-write
문제). 이번 챕터는 Spring Kafka가 제공하는 DB-Kafka 트랜잭션 동기화 메커니즘을 직접
써보고, 이게 예외 상황에서의 롤백은 확실히 잡아주지만 **진짜 원자성(2단계 커밋)은
아니라는 한계**를 몸으로 확인하는 게 목표다. 이 한계가 바로 Chapter 18 Outbox 패턴이
필요한 이유다.

## 개념 정리

- **Kafka 프로듀서 트랜잭션**: `transactional.id`를 설정하면 프로듀서가 여러 토픽/파티션에
  걸친 발행을 하나의 원자적 단위로 묶을 수 있다(`beginTransaction`/`commitTransaction`/
  `abortTransaction`). `DefaultKafkaProducerFactory.setTransactionIdPrefix(String)`을
  호출하면 그 팩토리로 만드는 프로듀서가 트랜잭셔널해진다(`transactionCapable() == true`).
- **`ChainedKafkaTransactionManager`는 Spring Kafka 4.1.0에서 이미 `@Deprecated`다** —
  소스(`ChainedKafkaTransactionManager.java`)에서 직접 확인했다. DB 트랜잭션 매니저와
  Kafka 트랜잭션 매니저를 하나로 묶는 예전 방식인데, 이제는 권장되지 않는다.
- **대신 Spring Kafka는 별도 체이닝 없이도 자동 동기화를 지원한다**: `@Transactional`
  (예: JDBC `DataSourceTransactionManager`)이 이미 활성화된 상태에서 트랜잭셔널
  `KafkaTemplate.send()`를 호출하면, `ProducerFactoryUtils.getTransactionalResourceHolder(...)`가
  `TransactionSynchronizationManager.isSynchronizationActive()`를 감지해서 그 Kafka
  발행을 현재 DB 트랜잭션에 자동으로 동기화한다(`KafkaResourceSynchronization` 등록).
  DB 트랜잭션이 커밋되면 Kafka 트랜잭션도 커밋되고, DB가 롤백되면 Kafka도 abort된다.
  이것도 소스(`ProducerFactoryUtils.java`, `KafkaTemplate.java`)를 까서 확인한 내용이다.
- **`isolation.level=read_committed`**: 컨슈머 설정. 이 값이면 abort되거나 아직 커밋
  안 된 트랜잭션의 레코드는 컨슈머에 아예 안 보인다. 기본값(`read_uncommitted`)은
  커밋 여부와 무관하게 다 보여준다 — 그래서 "커밋됐는지"를 검증하려면 반드시
  `read_committed` 컨슈머로 확인해야 한다.
- **이래도 진짜 원자성은 아니다**: 이번 챕터에서 만든 동기화는 "같은 프로세스, 같은
  메서드 안에서 예외가 나서 롤백되는 경우"는 확실히 커버한다. 하지만 DB 커밋이 실제로
  끝난 **직후** 프로세스가 죽어서 Kafka 커밋이 아예 실행될 기회조차 없는 경우는 못
  막는다 — 진짜 2단계 커밋(2PC)이 아니라 "같은 스레드의 트랜잭션 동기화 콜백"일 뿐이기
  때문이다. 이 갭이 Outbox 패턴이 필요한 이유다.

## 진행 과정

### 1. 의존성/스키마/설정 추가

```kotlin
// learning/build.gradle.kts
implementation("org.springframework.boot:spring-boot-starter-jdbc")
runtimeOnly("org.postgresql:postgresql")
```

```sql
-- schema-chapter15.sql
DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
    id BIGSERIAL PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL UNIQUE,
    status VARCHAR(20) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now()
);
```

```yaml
# application-chapter15.yaml
spring:
  datasource:
    url: jdbc:postgresql://localhost:5432/practice_kafka
    username: kafka
    password: kafka
  sql:
    init:
      mode: always
      schema-locations: classpath:schema-chapter15.sql
  kafka:
    consumer:
      group-id: chapter15-default-group
      auto-offset-reset: earliest
```

`spring-boot-starter-jdbc`가 있으면 Spring Boot가 `JdbcTemplate`과 `PlatformTransactionManager`
(`JdbcTransactionManager`)를 자동 설정해줘서, `@EnableTransactionManagement` 없이
`@Transactional`을 바로 쓸 수 있다.

### 2. `KafkaConfig`에 트랜잭셔널 프로듀서 전용 빈 추가

```java
@Bean
public ProducerFactory<String, String> transactionalProducerFactory(KafkaProperties properties) {
  Map<String, Object> props = properties.buildProducerProperties();
  DefaultKafkaProducerFactory<String, String> factory = new DefaultKafkaProducerFactory<>(props);
  factory.setTransactionIdPrefix("chapter15-tx-");
  return factory;
}

@Bean
public KafkaTemplate<String, String> transactionalKafkaTemplate(
    ProducerFactory<String, String> transactionalProducerFactory) {
  return new KafkaTemplate<>(transactionalProducerFactory);
}
```

Chapter 13에서 만든 공유 `kafkaTemplate`(DLQ 발행에도 쓰임)은 건드리지 않고 별도 빈으로
분리했다 — `transactionIdPrefix`는 여러 속성이 세트로 함께 달라지는 경우라 전용 팩토리가
맞다는 기존 판단 기준(LOG013)을 그대로 따랐다.

### 3. `OrderRepository`, `OrderTransactionService`, 테스트 작성

`OrderRepository`는 `JdbcTemplate` 기반으로 단순하게(`save`, `countByOrderId`).
`OrderTransactionService.placeOrder()`가 이 챕터의 핵심이다:

```java
@Transactional
public void placeOrder(String orderId, boolean simulateFailure) {
  orderRepository.save(orderId, "CREATED");
  transactionalKafkaTemplate.send(KafkaTopics.ORDER_EVENTS, orderId, "ORDER_CREATED:" + orderId);
  log.info("주문 저장 + 이벤트 발행 완료: orderId={}", orderId);

  if (simulateFailure) {
    throw new IllegalStateException("강제 실패 — DB, Kafka 둘 다 롤백되는지 확인");
  }
}
```

### 4. 첫 실행 실패 — `initTransactions() failed` (단일 브로커 replication factor)

```
org.apache.kafka.common.errors.TimeoutException: Timeout expired after 60000ms while awaiting InitProducerId
```

Kafka 트랜잭션은 내부 `__transaction_state` 토픽(트랜잭션 코디네이터 상태 저장용)이
필요한데, Confluent 이미지 기본값은 이 토픽의 `replication.factor=3`, `min.isr=2`다.
로컬 `docker-compose.yml`은 브로커가 1개뿐이라 이 토픽 생성 자체가 안 되고,
`InitProducerId` 요청이 응답을 못 받은 채 타임아웃났다. `docker-compose.yml`의 `kafka`
서비스에 두 줄을 추가해서 해결했다:

```yaml
KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
```

추가 후 `docker compose down && docker compose up -d`로 재기동(이미 잘못된 상태로
반쯤 만들어졌을 `__transaction_state` 토픽을 깨끗하게 다시 잡기 위해). Chapter 6~13은
트랜잭션을 안 썼기 때문에 이 설정 공백이 지금까지 한 번도 안 드러났었다.

### 5. 실패 케이스 로그 해석 — `TransactionAbortedException`이 ERROR로 찍히는 게 맞는가

`placeOrderRollsBackOnFailure` 테스트에서 이런 로그가 나왔다:

```
INFO ... 주문 저장 + 이벤트 발행 완료: orderId=tx-order-fail-...
INFO ... [Producer ...] Aborting incomplete transaction
ERROR ... o.s.k.support.LoggingProducerListener : Exception thrown when sending a message ...
org.apache.kafka.common.errors.TransactionAbortedException: Failing batch since transaction was aborted
```

**이건 버그가 아니라 의도한 결과의 증거다.** `KafkaTemplate.send()`는 비동기라 호출
직후 바로 리턴한다(그래서 "발행 완료" 로그가 실제 브로커 전송 전에 먼저 찍힘). 그 다음
`IllegalStateException`이 던져지고, `@Transactional` AOP가 DB 트랜잭션을 롤백 대상으로
표시하면서, 거기 동기화되어 있던 Kafka 트랜잭션도 같이 abort된다. 아직 브로커로 실제
전송되지 않고 버퍼에 남아있던 레코드는 로컬에서 `TransactionAbortedException`으로
실패 처리되는데, `KafkaTemplate`은 기본으로 `LoggingProducerListener`를 갖고 있어서
(별도 설정 없이도 내장) 이 실패가 ERROR 레벨로 찍힌다. 콘솔만 보면 "뭔가 잘못됐나?"
싶지만, 사실 이 ERROR 로그 자체가 "메시지가 브로커에 커밋되지 않았다"는 증거다.

### 6. 성공 케이스도 assertion으로 검증하기

처음엔 `placeOrderSucceeds` 테스트가 `주문 저장 + 이벤트 발행 완료` 로그 한 줄만
찍고 끝났다. 이 로그는 `send()` 호출 직후(비동기, 브로커 응답 전)에 찍히는 것뿐이라
"진짜 커밋됐다"는 증거가 안 된다 — 커밋 관련 로그는 Kafka 클라이언트 내부에서 DEBUG
레벨이라 안 보인다. Chapter 14에서 짚었던 "assertion 없이 로그만 보고 통과 판단하지
말 것"이 여기서도 그대로 반복됐다.

`isolation.level=read_committed` 컨슈머로 직접 확인하는 헬퍼를 추가했다:

```java
private boolean orderEventCommitted(String orderId) {
  Map<String, Object> consumerProps = new HashMap<>();
  consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
  consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "tx-verify-" + orderId);
  consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
  consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
  consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
  consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

  try (Consumer<String, String> consumer =
      new DefaultKafkaConsumerFactory<String, String>(consumerProps).createConsumer()) {
    consumer.subscribe(List.of(KafkaTopics.ORDER_EVENTS));
    long deadline = System.currentTimeMillis() + 5000;
    while (System.currentTimeMillis() < deadline) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
      for (ConsumerRecord<String, String> record : records) {
        if (orderId.equals(record.key())) {
          return true;
        }
      }
    }
    return false;
  }
}
```

`read_committed`는 커밋된 트랜잭션의 레코드만 보여주고 abort되거나 아직 커밋 안 된
레코드는 아예 안 보여준다 — 그래서 성공/실패 케이스 둘 다에 대해 진짜 증거가 된다.
반대로 `OrderEventConsumer`(Chapter 6, 항상 켜져 있는 `read_uncommitted` 리스너)로는
이걸 증명할 수 없다.

두 테스트 모두 이 assertion을 추가해서 통과했다:

```java
assertThat(orderEventCommitted(orderId)).isTrue();   // 성공 케이스
assertThat(orderEventCommitted(orderId)).isFalse();  // 실패 케이스
```

## 시행착오 / Q&A

**Q. `ChainedKafkaTransactionManager`를 안 쓰고도 DB-Kafka 트랜잭션이 묶이는 게 맞나?**
A. 맞다. Spring Kafka 소스(`ProducerFactoryUtils`, `KafkaTemplate.getTheProducer`)를
직접 확인했다. `TransactionSynchronizationManager`에 활성 트랜잭션이 있으면
`ProducerFactoryUtils.getTransactionalResourceHolder(...)`가 그 트랜잭션에 Kafka
리소스를 자동으로 동기화 등록한다. `ChainedKafkaTransactionManager`는 오히려
Spring Kafka 4.1.0에서 `@Deprecated` 상태였다.

**Q. 단일 브로커에서 `KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1`을 쓰는 게
운영에서도 맞는 설정인가?**
A. 아니다. 이건 로컬 학습 환경(브로커 1개)이라 필요한 예외적 조정이다. 실제 운영
클러스터(브로커 3대 이상)에서는 기본값(`replication.factor=3`, `min.isr=2`)을 유지해야
트랜잭션 코디네이터 자체의 가용성이 보장된다.

**Q. `TransactionAbortedException`이 ERROR로 찍히는데 테스트는 왜 통과하나?**
A. `assertThatThrownBy`로 우리가 명시적으로 예상한 예외이기 때문이다. 로그의 ERROR
레벨은 심각도 표시일 뿐 테스트 프레임워크의 pass/fail 판정과는 무관하다 — 이번
시나리오에서는 오히려 "의도대로 abort됐다"는 걸 보여주는 유용한 신호였다.

## 트레이드오프 / 실무 함정 / 안티패턴

**트레이드오프**: DB-Kafka 트랜잭션 동기화는 "예외로 인한 롤백"은 확실히 처리하지만
프로세스 크래시로 인한 갭(DB 커밋 직후 죽는 경우)은 못 막는다. 완전한 원자성이
필요하면 Outbox 패턴(Chapter 18)처럼 아예 다른 접근이 필요하다.

**실무 함정**: 단일 브로커(또는 소규모) 개발/테스트 환경에서 Kafka 트랜잭션을 처음
켤 때 `__transaction_state` 토픽의 기본 복제 계수(3)가 브로커 수보다 크면
`InitProducerId`가 응답 없이 타임아웃난다 — 에러 메시지가 트랜잭션 자체의 문제처럼
보이지만 실제 원인은 복제 계수 불일치다. 이번에 정확히 겪었다.

**안티패턴**: 트랜잭셔널 프로듀서 발행 성공을 "예외가 안 났다"만으로 판단하는 것.
`send()`는 비동기라 호출 직후 로그가 찍혀도 실제 커밋 여부와 무관하다 — `read_committed`
컨슈머로 직접 확인하지 않으면 "그럭저럭 되는 것처럼 보이는" 상태에 그친다.

## 더 생각해볼 것

이번에 확인한 DB-Kafka 동기화의 한계(크래시 윈도우)를 실제로 재현해볼 수 있을까 —
예를 들어 `@Transactional` 메서드 안에서 DB 커밋 이후 Kafka 커밋 이전 시점에 강제로
프로세스를 죽이는 실험은 일반적인 테스트로는 어렵다(Spring이 두 커밋을 같은 메서드
호출 스택 안에서 순차 실행하기 때문). 이 한계를 "이론이 아니라 직접 겪어보는" 방법은
Chapter 18에서 Outbox 패턴을 만들 때, "Outbox 없이 그냥 DB+Kafka 트랜잭션 동기화만
쓰면 왜 부족한가"를 다시 짚어보면서 자연스럽게 이어질 것 같다. 또한 `KafkaConfig`가
이번에도 빈 2개(`transactionalProducerFactory`, `transactionalKafkaTemplate`)가
늘어서 계속 누적되고 있다 — LOG013, LOG014에서 반복해서 관찰한 패턴이다.

## 최종 구성

`learning/build.gradle.kts`에 `spring-boot-starter-jdbc`, `postgresql` 드라이버 추가.
`schema-chapter15.sql`, `application-chapter15.yaml`(datasource, sql.init, chapter15
group-id) 신규. `docker-compose.yml`의 `kafka` 서비스에
`KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR`/`KAFKA_TRANSACTION_STATE_LOG_MIN_ISR`
추가(단일 브로커 트랜잭션 지원). `KafkaConfig`에 `transactionalProducerFactory`/
`transactionalKafkaTemplate` 빈 추가. `io.hkarling.learning.transaction` 패키지 신규
— `OrderRepository`(JdbcTemplate 기반), `OrderTransactionService`(`@Transactional` +
트랜잭셔널 `KafkaTemplate.send()`), `OrderTransactionServiceTest`(성공/실패 케이스,
DB assertion + `read_committed` 컨슈머로 Kafka 커밋 여부 직접 검증).

## ADR

### Decision
`ChainedKafkaTransactionManager` 없이 Spring Kafka의 자동 트랜잭션 동기화
(`@Transactional` DB 트랜잭션 + 트랜잭셔널 `KafkaTemplate`)를 사용한다. 트랜잭셔널
프로듀서는 기존 `kafkaTemplate`(Chapter 13, DLQ 공유)과 분리된 전용 빈으로 만든다.

### Drivers
`ChainedKafkaTransactionManager`가 Spring Kafka 4.1.0에서 이미 deprecated임을 소스로
확인했다. 자동 동기화 메커니즘이 별도 체이닝 설정 없이도 동일한 효과(예외 시 DB+Kafka
동시 롤백)를 내는 것도 소스로 검증했다.

### Alternatives
`ChainedKafkaTransactionManager`를 그대로 쓰는 방법도 있었으나 deprecated라 기각.
공유 `kafkaTemplate` 빈에 그냥 `transactionIdPrefix`를 설정하는 방법도 검토했지만,
Chapter 13 DLQ 발행 동작에 영향을 줄 수 있어 전용 빈으로 분리하는 쪽을 선택했다.

### Consequences
로컬 단일 브로커 환경의 트랜잭션 관련 설정 공백(`__transaction_state` 복제 계수)이
`docker-compose.yml`에 새로 반영됐다 — 앞으로 이 프로젝트에서 Kafka 트랜잭션을 쓰는
모든 챕터(Outbox 등)가 이 설정 위에서 동작하게 된다. `KafkaConfig`가 계속 커지고
있다는 관찰이 이번에도 반복됐다(LOG013, LOG014에 이어 세 번째).

### Follow-ups
Chapter 16 — at-least-once vs exactly-once 트레이드오프. Chapter 18 Outbox 패턴에서
이번 챕터의 "동기화 vs 진짜 원자성" 한계를 다시 짚을 예정.
