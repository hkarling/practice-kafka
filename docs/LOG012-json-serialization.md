# LOG012 — 직렬화/역직렬화: JSON, Schema Registry 개념

## 배경 / 목표

Phase 2 챕터 12. 지금까지 계속 `String` 값만 주고받았는데, 실무에서는 구조화된
도메인 객체를 주고받는 게 일반적이다. JSON 직렬화/역직렬화를 실제로 구성해보고,
그 과정에서 타입 정보 헤더·보안(신뢰 패키지)·역직렬화 실패(poison pill) 처리를
확인한다. Schema Registry는 인프라가 없어 개념만 다룬다.

## 개념 정리

- **JSON 직렬화**: Spring Kafka가 Jackson 기반으로 제공하는
  `Serializer`/`Deserializer` 구현체로 객체 ↔ JSON 바이트를 변환한다.
- **타입 정보 헤더(`__TypeId__`)와 그 대가**: 직렬화기가 메시지에 Java 풀
  클래스명을 헤더로 자동으로 넣어, 컨슈머가 어떤 타입으로 역직렬화할지 알게
  해준다. 편리하지만 프로듀서·컨슈머가 같은 Java 클래스 구조를 공유해야 하는
  암묵적 결합이 생긴다 — 다른 언어로 만든 서비스와는 안 통한다.
- **보안 — 신뢰 패키지(Trusted Packages)**: 헤더에 적힌 아무 클래스나 믿고
  역직렬화하면 임의 클래스 인스턴스화(역직렬화 공격) 위험이 있다. 역직렬화기가
  기본적으로 화이트리스트에 없는 패키지는 거부하도록 설계돼 있어,
  `addTrustedPackages(...)`로 허용 패키지를 명시해야 한다.
- **Schema Registry 개념 (실습 없음)**: Confluent Schema Registry는 메시지에
  Java 클래스명 대신 스키마 ID만 담고, 실제 스키마(Avro/Protobuf/JSON Schema)는
  중앙 레지스트리에 등록해 조회한다. 언어 중립적이고, 호환성 모드
  (backward/forward/full)로 스키마 진화를 배포 전에 검증할 수 있다. 이
  프로젝트엔 Schema Registry 인프라가 없어 개념만 다루고, 나중에 별도로 인프라를
  추가해 Avro로 실습해보고 싶다는 의견이 있어 후속 과제로 남긴다.
- **역직렬화 실패(Poison Pill)와 `ErrorHandlingDeserializer`**: 역직렬화기를
  단독으로 쓰면, 역직렬화 실패가 `Consumer.poll()` 내부(Spring 개입 전)에서
  `SerializationException`으로 터져 **컨슈머 스레드 자체가 죽는다.**
  `ErrorHandlingDeserializer`로 감싸면 실패를 "오염된 레코드"로 변환해 컨테이너의
  `DefaultErrorHandler`(Chapter 8에서 이미 배후에서 동작 중이던 그 메커니즘)로
  정상적으로 넘겨준다.

## 진행 과정

### 1. 코드 구성

```yaml
# application-chapter12.yaml
spring:
  application:
    name: learning
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: chapter12-default-group
      auto-offset-reset: earliest
```

```java
// KafkaTopics.java — 기존 order-events(String)와 형식을 섞지 않기 위해 새 토픽 추가
public static final String ORDER_EVENTS_JSON = "order-events-json";
```

```java
// OrderEvent.java
package io.hkarling.learning.kafka;

import java.time.Instant;

public record OrderEvent(
    String orderId,
    String eventType,
    Instant occurredAt
) {

}
```

```java
// OrderEventJsonConsumer.java
package io.hkarling.learning.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@Profile("chapter12")
public class OrderEventJsonConsumer {

  @KafkaListener(
      id = "order-event-json-listener",
      topics = KafkaTopics.ORDER_EVENTS_JSON,
      groupId = "chapter12-json-group",
      containerFactory = "jsonKafkaListenerContainerFactory")
  public void listen(OrderEvent event) {
    log.info("수신: {}", event);
  }
}
```

```java
// JsonSerializationTest.java
package io.hkarling.learning.kafka;

import java.time.Instant;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;

@Slf4j
@SpringBootTest
@ActiveProfiles("chapter12")
@DisplayName("직렬화/역직렬화 — JSON")
class JsonSerializationTest {

  @Autowired
  KafkaTemplate<String, OrderEvent> jsonKafkaTemplate;

  @Autowired
  KafkaTemplate<String, String> kafkaTemplate;

  @Test
  @DisplayName("OrderEvent를 JSON으로 발행하고 같은 타입으로 역직렬화해 받는다")
  void sendAndReceiveJson() throws Exception {
    OrderEvent event = new OrderEvent("order-json-1", "ORDER_PLACED", Instant.now());
    jsonKafkaTemplate.send(KafkaTopics.ORDER_EVENTS_JSON, event.orderId(), event).get();
    Thread.sleep(3000);
  }

  @Test
  @DisplayName("깨진 JSON(poison pill)을 보내도 컨슈머가 죽지 않고 다음 메시지를 받는다")
  void poisonPillDoesNotKillConsumer() throws Exception {
    kafkaTemplate.send(KafkaTopics.ORDER_EVENTS_JSON, "poison-key", "this-is-not-json").get();

    OrderEvent event = new OrderEvent("order-json-2", "ORDER_PLACED", Instant.now());
    jsonKafkaTemplate.send(KafkaTopics.ORDER_EVENTS_JSON, event.orderId(), event).get();

    Thread.sleep(10000);
  }
}
```

`KafkaConfig`엔 JSON 발행/구독용 빈(`jsonProducerFactory`, `jsonKafkaTemplate`,
`jsonConsumerFactory`, `jsonKafkaListenerContainerFactory`)을 추가했다 — 최종
코드는 아래 3번, 4번 항목에서 다룬다(초안 그대로 안 갔고 두 번 고쳤다).

### 2. 첫 번째 삽질 — `JsonSerializer`/`JsonDeserializer` deprecated

처음엔 `org.springframework.kafka.support.serializer.JsonSerializer`/
`JsonDeserializer`로 작성했는데 IDE가 deprecated 경고를 띄웠다. Spring Kafka
4.1.0 소스를 직접 까보니:

```
@deprecated since 4.0 in favor of {@link JacksonJsonSerializer} for Jackson 3.
```

이 프로젝트의 실제 런타임 classpath를 확인해보니 이미 Jackson 3
(`tools.jackson.core:jackson-databind:3.1.4`)이 물려 있었다 — `JsonSerializer`는
Jackson 2 기반이라 이 프로젝트에선 애초에 안 맞는 선택이었다.
`JacksonJsonSerializer`/`JacksonJsonDeserializer`로 교체했다. API는 거의
동일하고(`addTrustedPackages(String...)` 메서드명도 같음), `Deserializer<T>`
인터페이스를 구현하는 것도 동일해서 `ErrorHandlingDeserializer`로 감싸는 것도
그대로 됐다.

### 3. Poison pill 실험 — 재시도 없이 즉시 스킵

`poisonPillDoesNotKillConsumer()` 실행 결과:
```
ERROR ... DefaultErrorHandler : Backoff FixedBackOffExecution[interval=0, currentAttempts=1, maxAttempts=0] exhausted for order-events-json-0@4
...
수신: OrderEvent[orderId=order-json-2, eventType=ORDER_PLACED, occurredAt=...]
```

**`maxAttempts=0`** — Chapter 8의 `fail-1`(`maxAttempts=9`, 9번 재시도)과 다르게
**재시도를 전혀 안 하고 즉시 포기**했다. 역직렬화 실패는 재시도해도 같은
바이트·같은 파싱 에러로 항상 똑같이 실패할 게 뻔하니, `DefaultErrorHandler`가
이런 실패는 재시도 없이 바로 로그하고 넘어가도록 구분해서 처리한다 — 리스너
(비즈니스 로직) 예외와 역직렬화 예외를 다르게 다룬다는 걸 실측으로 확인했다.
그 바로 다음 정상 메시지(`order-json-2`)가 문제없이 수신되어, 컨슈머가 poison
pill 때문에 죽지 않는다는 것도 확인됐다.

### 4. 두 번째 삽질 — `@ConditionalOnMissingBean`이 raw 타입 기준이라 기본 빈이 사라짐

`jsonConsumerFactory`(`ConsumerFactory<String, OrderEvent>`)를 추가하자,
Chapter 8의 `manualAckKafkaListenerContainerFactory`가 컨텍스트 시작 시점에
실패했다:
```
No qualifying bean of type 'ConsumerFactory<String, String>' available
```

Spring Boot의 `KafkaAutoConfiguration#kafkaConsumerFactory`는
`@ConditionalOnMissingBean(ConsumerFactory.class)`(제네릭 무시, raw 타입
기준)로 되어 있어서, **`ConsumerFactory` 타입 빈이 하나라도 있으면 자기
기본 빈을 아예 안 만든다.** `jsonConsumerFactory`가 등장하자 Boot는 "이미
있네" 하고 기본 `ConsumerFactory<String, String>` 빈 생성 자체를 건너뛰었다.
`KafkaConfig`에 명시적 `stringConsumerFactory` 빈을 추가해서 해결했다.

### 5. 더 큰 문제 발견 — 이 버그가 Chapter 7부터 프로듀서 쪽에서도 조용히 있었다

같은 컨디션 리포트에 `KafkaAutoConfiguration#kafkaProducerFactory`/
`#kafkaTemplate`도 "Did not match" — `acksZeroProducerFactory`/
`acksZeroKafkaTemplate`(Chapter 7)가 존재한다는 이유로 이미 스킵되고 있었다.
차이는, 프로듀서 쪽은 후보가 우연히 **하나뿐**이라(`acksZeroKafkaTemplate`)
`@Autowired KafkaTemplate<String, String> kafkaTemplate`이 타입으로 유일하게
매칭돼서 **에러 없이 조용히 잘못된 빈에 연결**돼 있었다. 즉 Chapter 7부터
지금까지 `kafkaTemplate`은 실제로 `acksZeroKafkaTemplate`과 같은 프로듀서
(acks=0)였다.

직접 검증했다:
```java
log.info("kafkaTemplate acks 설정: {}",
    kafkaTemplate.getProducerFactory().getConfigurationProperties().get("acks"));
```
→ `chapter12` 프로파일(yaml에 `producer.acks` 미설정)에서 처음엔 `0`이 찍혔다.

`KafkaConfig`에 명시적 `stringProducerFactory`/`kafkaTemplate` 빈을 추가해서
해결했다(빈 메서드 이름을 정확히 `kafkaTemplate`으로 지어야, 여러
`KafkaTemplate<String, String>` 후보 중 필드 이름과 매칭되어 올바른 빈으로
연결된다). 수정 후 같은 진단 로그를 `chapter07`(yaml에 `producer.acks: all`
명시)에서 확인하니 `null`이 아니라(이번엔 설정 자체가 있으니) 정상적으로
반영됐고, `ProducerAcksTest.compareAcks()`를 재실행해 실제로 서로 다른
프로듀서를 비교한 결과를 얻었다 — 자세한 내용과 정정된 수치는 LOG007 참고.

## 시행착오 / Q&A

**Q. 역직렬화 실패가 났는데 왜 9번 재시도를 안 하나?**
A. 리스너 예외(비즈니스 로직 실패, Chapter 8)는 "다시 하면 될 수도 있으니"
재시도하지만, 역직렬화 실패는 같은 바이트를 다시 파싱해도 항상 같은 에러가 나서
재시도가 무의미하다. `DefaultErrorHandler`가 이 둘을 구분해서 처리한다.

**Q. 커스텀 `ConsumerFactory`/`ProducerFactory` 빈을 추가했을 뿐인데 왜 관련
없어 보이는 다른 챕터의 빈이 못 뜨나?**
A. Spring Boot의 `@ConditionalOnMissingBean(SomeType.class)`가 제네릭을
무시하고 raw 타입으로만 판단하기 때문이다. `ConsumerFactory<String,
OrderEvent>` 하나만 추가해도 Boot는 "ConsumerFactory 있네"라고 판단해서
`ConsumerFactory<String, String>`용 기본 빈까지 통째로 안 만든다. 컨슈머
쪽은 후보가 아예 없어져서 하드 에러로 드러났지만, 프로듀서 쪽은 후보가
우연히 하나 있어서(`acksZeroKafkaTemplate`) 에러 없이 조용히 잘못
연결되는 훨씬 위험한 상황이었다.

## 트레이드오프 / 실무 함정 / 안티패턴

**트레이드오프**: JSON은 사람이 읽을 수 있고 스키마 강제가 없어 빠르게
시작하기 좋지만, 타입 안전성과 스키마 진화 관리는 전적으로 애플리케이션
코드(그리고 개발자의 규율)에 맡겨진다. Avro/Protobuf + Schema Registry는
초기 설정 비용이 있지만 스키마 호환성을 배포 파이프라인 차원에서 강제할 수
있다.

**실무 함정 (이번 챕터 최대 수확)**: 커스텀 `ProducerFactory`/
`ConsumerFactory`/`KafkaTemplate` 빈을 추가할 때, Spring Boot의 자동 구성
기본 빈이 **제네릭과 무관하게 raw 타입 기준으로 조용히 사라질 수 있다.**
후보가 여러 개면 하드 에러로 바로 드러나지만(Chapter 12), 후보가 우연히
하나뿐이면 **아무 에러 없이 의도와 다른 빈에 연결**된다(Chapter 7부터
지금까지 실제로 겪음). 커스텀 프로듀서/컨슈머 팩토리 빈을 추가할 때마다
"내가 만든 빈 하나 때문에, 이름이 겹치지도 않는 다른 기본 빈이 사라지지
않았는지"를 확인하는 습관이 필요하다 — 이런 경우 항상 명시적으로 대체
기본 빈을 만들어주는 게 안전하다.

**안티패턴**: 자동 주입이 예외 없이 성공했다고 "의도한 빈이 연결됐다"고
단정하는 것. 타입이 맞으면 Spring은 조용히 연결해주지, "이게 내가 기대한
그 빈인가"는 검증해주지 않는다. 여러 프로듀서/컨슈머 빈이 존재하는
프로젝트에서는 실제로 어떤 빈이 연결됐는지(예: 설정값을 로그로 찍어보는
것) 한 번쯤 직접 확인하는 게 안전하다.

## 더 생각해볼 것

Schema Registry를 실제로 인프라에 추가해서 Avro로 실습해보고 싶다 —
`docker-compose.yml`에 Schema Registry 컨테이너를 추가하는 것부터 시작하면
될 것 같다. 배치 리스너(Chapter 11)와 JSON 역직렬화를 조합하면 배치 안의
레코드 하나가 poison pill일 때 배치 전체를 어떻게 처리해야 할지도 남은
질문 — Chapter 13(에러 처리 — 재시도, DLQ 설계)에서 이어진다.

## 최종 구성

`KafkaTopics`에 `ORDER_EVENTS_JSON` 추가. `OrderEvent` 레코드 신규.
`OrderEventJsonConsumer` 신규(`@Profile("chapter12")`). `application-chapter12.yaml`
추가. `KafkaConfig`에 `jsonProducerFactory`/`jsonKafkaTemplate`
(`JacksonJsonSerializer` 기반), `jsonConsumerFactory`(`JacksonJsonDeserializer`
+ `ErrorHandlingDeserializer` + trusted packages)/`jsonKafkaListenerContainerFactory`,
그리고 이번에 드러난 버그를 고치기 위한 `stringConsumerFactory`,
`stringProducerFactory`/`kafkaTemplate` 빈 추가. 테스트
`JsonSerializationTest`(`sendAndReceiveJson`, `poisonPillDoesNotKillConsumer`)
작성.

## ADR

### Decision
`JsonSerializer`/`JsonDeserializer`(Jackson 2, deprecated) 대신
`JacksonJsonSerializer`/`JacksonJsonDeserializer`(Jackson 3)를 사용한다.
Spring Boot의 자동 구성 기본 빈이 사라지는 문제는, 매번 명시적으로 대체
기본 빈(`stringConsumerFactory`, `stringProducerFactory`/`kafkaTemplate`)을
직접 정의해서 해결한다.

### Drivers
`JsonSerializer`는 deprecated됐고 이 프로젝트는 이미 Jackson 3 classpath라
새 클래스가 실제로 맞는 선택이었다. `@ConditionalOnMissingBean`이 raw
타입 기준이라는 건 Spring Boot 프레임워크 자체의 동작이라 우리가 바꿀 수
없으니, 커스텀 빈을 추가할 때마다 "기본 빈 대체품도 같이 명시한다"는
규칙으로 대응하는 게 유일한 선택지였다.

### Alternatives
`@Primary`나 `@Qualifier`로 기존 방식을 유지하는 것도 고려했으나, 근본
원인(기본 빈 자체가 안 만들어짐)을 해결하지 못해 기각. 커스텀 빈에
`@ConditionalOnMissingBean`을 직접 걸어 Boot 기본 빈과 공존을 시도하는
방법도 있었지만, 오히려 조건 평가가 더 복잡해져서 명시적 대체 빈 쪽이
더 명확하다고 판단했다.

### Consequences
Chapter 7의 `compareAcks()` 결과가 사실 같은 프로듀서를 비교한 것이었다는
게 드러나 LOG007에 정정이 필요했다(별도 반영). Chapter 8~11은 프로듀서의
acks 값과 무관한 내용을 검증하고 있어 결론에는 영향이 없었다. 앞으로
`KafkaConfig`에 커스텀 팩토리 빈을 추가할 때마다 이 패턴(대체 기본 빈 명시)을
습관적으로 점검한다.

### Follow-ups
Chapter 13 — 에러 처리 (재시도, DLQ 설계). Schema Registry 실습은 여유
있을 때 별도로 진행.
