# LOG011 — KafkaTemplate + @KafkaListener 제대로 보기

## 배경 / 목표

Phase 2 시작. `KafkaTemplate`과 `@KafkaListener`는 사실 Chapter 6부터 계속
써왔지만, 그때그때 필요한 속성만 설명 없이 넘어간 채 썼다. 이번 챕터는 ①
지금까지 써온 것들을 제대로 정리하고, ② 아직 안 써본 기능(메시지 헤더, 유연한
리스너 파라미터, 배치 리스너)을 실습으로 채운다.

## 개념 정리

### `KafkaTemplate` 제대로 보기

`KafkaTemplate<K, V>`는 `ProducerFactory<K, V>` 하나를 감싼 얇은 래퍼다.
내부적으로 Kafka `Producer<K, V>` 인스턴스 하나를 재사용하며, Kafka의
`Producer`는 원래 스레드-세이프하게 설계되어 있어서 `KafkaTemplate`도
애플리케이션 전체에서 **싱글턴 빈 하나로 공유**해서 쓴다.

**`send()` 계열 — 지금까지 쓴 것:**
- `send(String topic, V value)` — 키 없이 (Chapter 6, 10)
- `send(String topic, K key, V value)` — 키 지정 (Chapter 6, 10)
- `send(ProducerRecord<K, V> record)` — 헤더까지 직접 제어 (이번 챕터)

**안 써본 것**: `send(String topic, Integer partition, K key, V value)` —
파티셔너 로직을 우회하고 파티션 번호를 직접 지정.

**리턴 타입**: 모든 `send()`는 `CompletableFuture<SendResult<K, V>>`를
반환한다. `.get()`(블로킹, Chapter 7~10)이나 `.whenComplete(...)`(논블로킹
콜백, Chapter 7)으로 결과를 받는다. `SendResult`는 `getRecordMetadata()`로
브로커가 실제 배정한 partition/offset을 담고 있다 (Chapter 10에서 계속
꺼내본 그 객체).

### `@KafkaListener` 제대로 보기

`@KafkaListener`가 붙은 메서드 하나당, Spring Kafka는 ① 엔드포인트를 만들고
→ ② `MessageListenerContainer`를 생성해서 → ③ `KafkaListenerEndpointRegistry`에
등록한다 (Chapter 9에서 `registry.getListenerContainer(id)`로 직접 꺼내본 것).

**지금까지 쓴 속성들:**

| 속성 | 의미 | 처음 쓴 챕터 |
|---|---|---|
| `topics` | 구독할 토픽 (컴파일 타임 상수만 허용) | 6 |
| `groupId` | 컨슈머 그룹, 미지정 시 yaml 기본값 상속 | 6 (상속 때문에 8~9에서 삽질) |
| `containerFactory` | 어떤 `ConcurrentKafkaListenerContainerFactory` 빈을 쓸지 | 8 |
| `concurrency` | 그룹 안에 컨슈머 스레드 몇 개 | 9 |
| `id` | `KafkaListenerEndpointRegistry`에서 찾을 이름 | 9 |

**메서드 파라미터 — 지금까지는 항상 `ConsumerRecord<K, V>` 하나만 받았지만,
사실 훨씬 유연하다:**
- `ConsumerRecord<K, V>` — 레코드 전체 (지금까지 계속 쓴 방식)
- 값 타입만(`String value`) — Spring이 암묵적으로 `@Payload`를 붙여 값만 뽑아줌
- `@Header(...)` 파라미터 — 특정 메타데이터만 개별 파라미터로
- `Acknowledgment` — 커밋 제어용 (Chapter 8)
- `List<ConsumerRecord<K, V>>` — 배치 리스너 모드일 때

**배치 리스너 ≠ `BATCH` ack 모드**: 이름이 비슷해 헷갈리기 쉽지만 서로 다른
축이다. `BATCH` ack 모드(Chapter 8)는 "언제 커밋하냐"(poll()로 가져온 레코드
전체를 다 처리한 뒤 한 번에 커밋)의 문제고, 배치 리스너(`setBatchListener(true)`)는
"리스너 메서드가 몇 개씩 호출되냐"(레코드 하나마다 호출할지, `poll()` 결과
전체를 `List`로 한 번에 넘길지)의 문제다. 두 축은 독립적이라 이론적으로는
4가지 조합(단건+RECORD, 단건+BATCH, 배치+RECORD, 배치+BATCH)이 다 가능하지만,
실제로는 "배치로 받아서 배치로 커밋"이 자연스럽게 같이 쓰인다 — 배치 리스너인데
레코드마다 커밋하면 배치로 묶은 이점(처리량)이 커밋 오버헤드로 상쇄되기
때문이다.

## 진행 과정

### 1. 코드 구성

```yaml
# application-chapter11.yaml
spring:
  application:
    name: learning
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: chapter11-default-group
      auto-offset-reset: earliest
```

```java
// OrderEventProducer.java — sendWithHeader 추가
package io.hkarling.learning.kafka;

import java.nio.charset.StandardCharsets;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@RequiredArgsConstructor
@Component
public class OrderEventProducer {

  private final KafkaTemplate<String, String> kafkaTemplate;

  public void send(String value) {
    kafkaTemplate.send(KafkaTopics.ORDER_EVENTS, value);
  }

  public void send(String key, String value) {
    kafkaTemplate.send(KafkaTopics.ORDER_EVENTS, key, value);
  }

  public void sendWithHeader(String key, String value, String source) {
    ProducerRecord<String, String> producerRecord
        = new ProducerRecord<>(KafkaTopics.ORDER_EVENTS, key, value);
    producerRecord.headers().add("source", source.getBytes(StandardCharsets.UTF_8));
    kafkaTemplate.send(producerRecord);
  }

}
```

```java
// OrderEventHeaderConsumer.java
package io.hkarling.learning.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@Profile("chapter11")
public class OrderEventHeaderConsumer {

  @KafkaListener(
      id = "order-event-header-listener",
      topics = KafkaTopics.ORDER_EVENTS,
      groupId = "chapter11-header-group")
  public void listen(
      @Payload String value,
      @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
      @Header(KafkaHeaders.OFFSET) long offset,
      @Header(value = "source", required = false) String source
  ) {
    log.info("partition={}, offset={}, source={}, value={}", partition, offset, source, value);
  }

}
```

```java
// KafkaConfig.java — batchKafkaListenerContainerFactory 추가
@Bean
public ConcurrentKafkaListenerContainerFactory<String, String> batchKafkaListenerContainerFactory(
    ConsumerFactory<String, String> consumerFactory) {
  ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
  factory.setConsumerFactory(consumerFactory);
  factory.setBatchListener(true);
  return factory;
}
```

```java
// OrderEventBatchConsumer.java
package io.hkarling.learning.kafka;

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@Profile("chapter11")
public class OrderEventBatchConsumer {

  @KafkaListener(
      id = "order-event-batch-listener",
      topics = KafkaTopics.ORDER_EVENTS,
      groupId = "chapter11-batch-group",
      containerFactory = "batchKafkaListenerContainerFactory")
  public void listen(List<ConsumerRecord<String, String>> records) {
    log.info("배치 크기: {} 건", records.size());
    for (ConsumerRecord<String, String> record : records) {
      log.info("  partition={}, offset={}, value={}", record.partition(), record.offset(), record.value());
    }
  }

}
```

```java
// KafkaListenerFeaturesTest.java
package io.hkarling.learning.kafka;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@Slf4j
@SpringBootTest
@ActiveProfiles("chapter11")
@DisplayName("KafkaTemplate + @KafkaListener 기본 — 헤더, 배치")
class KafkaListenerFeaturesTest {

  @Autowired
  OrderEventProducer producer;

  @Test
  @DisplayName("커스텀 헤더를 실어 보내고, 유연한 파라미터로 수신한다")
  void sendAndReceiveWithHeader() throws InterruptedException {
    producer.sendWithHeader("order-header-test", "결제 완료", "payment-service");
    Thread.sleep(3000);
  }

  @Test
  @DisplayName("배치 리스너는 여러 레코드를 한 번에 묶어서 받는다")
  void batchListenerReceivesMultipleRecords() throws InterruptedException {
    for (int i = 0; i < 10; i++) {
      producer.send("batch-test-" + i);
    }
    Thread.sleep(3000);
  }
}
```

### 2. 헤더 발행/수신 확인

`sendWithHeader()`로 보낸 메시지가 `OrderEventHeaderConsumer`에서
`source=payment-service`로 정상 수신됐다 — 리스너 로그에 찍힌 `source` 값이
발행 시점에 넣은 값과 정확히 일치하는지가 이 단계의 확인 포인트다. 같은
리스너가 헤더 없이 발행된 기존 메시지(`send(key, value)`로 보낸 것들)도
같이 받는데, 그건 `source=null`로 찍혔다 — `@Header(value = "source",
required = false)` 덕분에 예외 없이 넘어갔다는 것도 로그로 확인했다.

### 3. 배치 리스너 삽질 — `containerFactory` 속성을 빠뜨림

`OrderEventBatchConsumer`를 처음 작성할 때 `@KafkaListener`에
`containerFactory = "batchKafkaListenerContainerFactory"`를 빠뜨렸다.
결과:
```
배치 크기: 1 건
ClassCastException: class java.lang.String cannot be cast to class ConsumerRecord
    at OrderEventBatchConsumer.listen(...)
    at RecordMessagingMessageListenerAdapter.onMessage(...)
```

**원인**: `containerFactory`를 안 넣으면 기본(비-배치) 컨테이너 팩토리로
등록된다. 컨테이너는 레코드를 한 건씩 리스너에 넘기려 하는데, 메서드
파라미터는 `List<ConsumerRecord<String, String>>`라 타입이 안 맞아서 값을
억지로 끼워 넣다가, 리스트를 순회(`for` 루프의 암묵적 캐스팅)하는 순간
`ClassCastException`이 났다. 스택트레이스의 `RecordMessagingMessageListenerAdapter`
(배치용 `BatchMessagingMessageListenerAdapter`가 아님)가 결정적 단서였다.

**해결**: `containerFactory = "batchKafkaListenerContainerFactory"` 추가.
재실행하니 `배치 크기: 10 건`과 함께 10개 레코드가 전부 `ConsumerRecord`로
정상 순회됐다.

### 4. 서로 다른 그룹은 각자 전체를 받는다 (Chapter 4 재확인)

배치 테스트로 보낸 10개 메시지를 `chapter11-batch-group`,
`chapter11-default-group`(`OrderEventConsumer`),
`chapter11-header-group`(`OrderEventHeaderConsumer`) **세 그룹 모두**가
각자 전부 받았다. 컨슈머 그룹끼리는 메시지를 나눠 갖지 않고 각자 독립된
전체 사본을 받는다는 것 — Chapter 4(Pub/Sub)에서 배운 개념이 실습으로 다시
확인된 것뿐, 버그가 아니다.

## 시행착오 / Q&A

**Q. 배치 리스너에서 `ClassCastException`이 왜 났나?**
A. `containerFactory` 속성을 깜빡해서 기본(비-배치) 팩토리로 등록됐기
때문. 스택트레이스에 `RecordMessagingMessageListenerAdapter`가 찍혀 있으면
"배치로 등록 안 됐다"는 뜻으로 바로 의심할 수 있다.

**Q. 테스트에 `Thread.sleep` 대신 `CountDownLatch`/`Awaitility`를 리스너에
심어서 완료를 기다리면 안 되나?**
A. 검토했지만 채택하지 않았다. 이 프로젝트는 "서비스 간 통신은 Kafka를
통해서만, 직접 호출 없음"이 원칙인데, 컨슈머 내부에 테스트가 들여다볼 공유
상태(래치 등)를 심는 건 그 원칙이 금지하는 "직접 호출"을 테스트 코드에서
몰래 만드는 것과 같다. 실제로 프로듀서/컨슈머가 분리된 서비스라면 애초에
같은 프로세스가 아니라서 이 방식 자체가 성립하지 않는다. Chapter 8~9부터
써온 "컨슈머 밖에서 독립적으로 관찰"(raw consumer, CLI 오프셋 확인)이 더
현실적인 검증 방법이다.

## 트레이드오프 / 실무 함정 / 안티패턴

**트레이드오프**: 배치 리스너는 처리량은 늘지만, 배치 안 레코드 하나가
실패했을 때 전체를 어떻게 처리할지(전체 재시도? 부분 커밋?)를 직접
설계해야 한다 — 단건 리스너보다 에러 핸들링이 복잡해진다 (Chapter 13에서
본격적으로 다룬다).

**실무 함정**: 커스텀 헤더를 읽을 때 `required = false`를 빠뜨리면, 그
헤더 없이 발행된(과거 버전 프로듀서가 보낸) 메시지를 만나는 순간 리스너
전체가 예외로 죽는다 — 헤더 스키마를 바꿀 때는 항상 하위 호환을 염두에
둬야 한다. 배포는 보통 프로듀서와 컨슈머가 동시에 바뀌지 않으므로(롤링
배포 중에는 구버전 프로듀서와 신버전 컨슈머가 한동안 공존한다), 새 헤더를
"필수"로 잡으면 그 배포 전환 구간에서 반드시 예외가 난다. 배치 리스너를
쓸 때 `containerFactory`를 빠뜨리면 컴파일 에러도 없이 런타임에야(그것도
첫 메시지가 들어와야) `ClassCastException`으로 드러난다 — 리스너를 추가할
때마다 컨테이너 팩토리가 의도한 것과 일치하는지 확인하는 습관이 필요하다.

**안티패턴**: 테스트를 통과시키려고 프로덕션 컨슈머 코드에 테스트 전용
훅(래치, 공유 리스트)을 심는 것 — 실제 분산 환경에서는 성립하지 않는
검증 방법이면서, 프로덕션 코드에 테스트만을 위한 결합을 만든다.

## 더 생각해볼 것

`@SendTo`로 리스너의 리턴값을 그대로 다른 토픽에 발행하는 간단한 파이프라인
패턴이나, `RecordFilterStrategy`로 리스너 호출 전에 특정 조건의 레코드를
걸러내는 기능은 이번에 다루지 않았다. 배치 리스너에서 레코드 하나가
실패하면 어떻게 처리해야 할지는 Chapter 13(에러 처리 — 재시도, DLQ 설계)에서
이어진다.

## 최종 구성

`application-chapter11.yaml` 추가. `OrderEventProducer`에 `sendWithHeader()`
추가. `OrderEventHeaderConsumer`, `OrderEventBatchConsumer` 신규
(둘 다 `@Profile("chapter11")`). `KafkaConfig`에
`batchKafkaListenerContainerFactory` 빈 추가. 테스트
`KafkaListenerFeaturesTest`(`sendAndReceiveWithHeader`,
`batchListenerReceivesMultipleRecords`) 작성.

## ADR

### Decision
테스트에서 리스너의 수신 완료를 확인하는 방법으로, 리스너 내부에 테스트용
훅(래치/공유 리스트)을 심지 않고 계속 `Thread.sleep` + 로그 육안 확인 또는
독립된 raw consumer 관찰 방식을 유지한다.

### Drivers
이 프로젝트의 "서비스 간 통신은 Kafka를 통해서만" 원칙을 테스트 코드에서도
지키기 위해서다. 리스너에 테스트 훅을 심는 건 실제로 분리된 서비스라면
성립하지 않는 방식이라, 검증 자체가 현실을 반영하지 못하게 된다.

### Alternatives
`CountDownLatch`를 리스너에 주입하거나 `Awaitility`로 공유 상태를 폴링하는
방식 — 검토했으나 위 이유로 기각. Chapter 9의 `ContainerTestUtils
.waitForAssignment()`처럼 "컨슈머 자체의 공개된 상태"(파티션 배정 등)를
기다리는 것과, "리스너 내부 로직의 부작용"을 훔쳐보는 것은 성격이 다르다고
판단했다.

### Consequences
이번 챕터의 헤더/배치 테스트는 여전히 `Thread.sleep` 기반이라 완전한
assertion은 아니다. 다만 검증이 필요한 대상이 "프로듀서가 보낸 것이
브로커에 반영됐는가"(Chapter 10처럼 `RecordMetadata`로 확인 가능) 수준이면
raw consumer로 충분히 assertion화할 수 있다 — 필요할 때 그렇게 개선한다.

### Follow-ups
Chapter 12 — 직렬화/역직렬화 (JSON, Schema Registry 개념).
