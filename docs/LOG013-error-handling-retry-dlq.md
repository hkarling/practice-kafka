# LOG013 — 에러 처리: 재시도, DLQ 설계

## 배경 / 목표

Phase 2 마지막 챕터. LOG012에서 "역직렬화 실패는 재시도 없이 즉시 스킵되지만,
스킵된 메시지는 로그에만 남고 사라진다"는 걸 확인했다. 이번 챕터는 그 실패한
메시지를 **버리지 않고 보존**하는 DLQ(Dead Letter Topic) 패턴을 실제로
구성하고, 지수 백오프 재시도까지 함께 확인한다.

## 개념 정리

- **지금까지의 한계**: Chapter 8, 12에서 본 `DefaultErrorHandler`의 기본
  복구 동작은 재시도를 다 소진하면 로그만 남기고 커밋을 넘겨버리는 것이었다.
  실패한 메시지가 뭐였는지는 로그를 뒤지지 않는 한 사라진다.
- **`DeadLetterPublishingRecoverer`**: `DefaultErrorHandler`의 복구 단계를
  커스터마이징해서, 실패한 레코드를 별도 토픽(DLT, Dead Letter Topic)으로
  자동 발행한다. 원본 예외 정보와 원본 위치(토픽/파티션/오프셋)를 헤더에
  실어서 보내주기 때문에, DLQ만 봐도 추적이 가능하다.
- **기본 목적지 이름**: `원본토픽 + "-dlt"` (하이픈, 소문자). Spring Kafka
  4.1.0 소스를 직접 확인한 결과다 — `.DLT`(점, 대문자)가 아니다 (아래 진행
  과정 2번 참고, 필자가 옛날 기억으로 잘못 안내했다가 정정한 부분).
- **`ExponentialBackOff` vs `FixedBackOff`**: Chapter 8의 `FixedBackOff(0, 9)`는
  간격 없이 9번 연달아 재시도했다. 실무에서는 보통 지수 백오프를 써서
  간격을 점점 늘린다 — 일시적 장애가 회복할 시간을 주고, 동시 재시도로
  인한 부하 집중을 막는다.
- **`DefaultErrorHandler`의 기본 동작은 생성자와 무관하게 유지된다**:
  `DefaultErrorHandler`는 `ExceptionClassifier`를 상속하는데, 그 기본
  생성자가 "역직렬화 예외 등은 재시도 안 함" 같은 기본 분류를 항상 설정한다.
  `new DefaultErrorHandler(recoverer, backOff)`처럼 커스텀 recoverer/backOff를
  줘도, 상위 클래스의 무인자 생성자가 먼저 실행되니 이 기본 분류는 그대로
  살아있다 — Spring Kafka 소스로 직접 확인했다 (아래 시행착오 참고).

## 진행 과정

### 1. 코드 구성

```yaml
# application-chapter13.yaml
spring:
  application:
    name: learning
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: chapter13-default-group
      auto-offset-reset: earliest
```

```java
// KafkaTopics.java — DLT 토픽 상수 추가
package io.hkarling.learning.kafka;

import org.springframework.kafka.retrytopic.RetryTopicConstants;

public final class KafkaTopics {

  public static final String ORDER_EVENTS = "order-events";
  public static final String ORDER_EVENTS_JSON = "order-events-json";
  public static final String ORDER_EVENTS_DLT = ORDER_EVENTS + RetryTopicConstants.DEFAULT_DLT_SUFFIX;

  private KafkaTopics() {
  }

}
```

```java
// KafkaConfig.java — DLQ용 에러 핸들러 + 컨테이너 팩토리 + 기본 프로듀서 빈(Chapter 7 버그 수정분 포함)
@Bean
public ProducerFactory<String, String> stringProducerFactory(KafkaProperties properties) {
  Map<String, Object> props = properties.buildProducerProperties();
  return new DefaultKafkaProducerFactory<>(props);
}

@Bean
public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> stringProducerFactory) {
  return new KafkaTemplate<>(stringProducerFactory);
}

@Bean
public DefaultErrorHandler dlqErrorHandler(KafkaTemplate<String, String> kafkaTemplate) {
  DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate);

  ExponentialBackOff backOff = new ExponentialBackOff(500, 2.0);
  backOff.setMaxInterval(5000);
  backOff.setMaxElapsedTime(15000);

  return new DefaultErrorHandler(recoverer, backOff);
}

@Bean
public ConcurrentKafkaListenerContainerFactory<String, String> dlqKafkaListenerContainerFactory(
    ConsumerFactory<String, String> stringConsumerFactory, DefaultErrorHandler dlqErrorHandler) {
  ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
  factory.setConsumerFactory(stringConsumerFactory);
  factory.setCommonErrorHandler(dlqErrorHandler);
  return factory;
}
```

```java
// OrderEventDlqConsumer.java
package io.hkarling.learning.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@Profile("chapter13")
public class OrderEventDlqConsumer {

  @KafkaListener(
      id = "order-event-dlq-listener",
      topics = KafkaTopics.ORDER_EVENTS,
      groupId = "chapter13-dlq-group",
      containerFactory = "dlqKafkaListenerContainerFactory")
  public void listen(ConsumerRecord<String, String> consumerRecord) {
    if (consumerRecord.value() != null && consumerRecord.value().startsWith("fail")) {
      log.warn("처리 실패: partition={}, offset={}, value={}",
          consumerRecord.partition(), consumerRecord.offset(), consumerRecord.value());
      throw new IllegalStateException("처리 실패: " + consumerRecord.value());
    }
    log.info("처리 완료: partition={}, offset={}, value={}",
        consumerRecord.partition(), consumerRecord.offset(), consumerRecord.value());
  }

}
```

```java
// OrderEventDlqObserver.java
package io.hkarling.learning.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@Profile("chapter13")
public class OrderEventDlqObserver {

  @KafkaListener(
      id = "order-event-dlt-listener",
      topics = KafkaTopics.ORDER_EVENTS_DLT,
      groupId = "chapter13-dlt-observer-group")
  public void listen(ConsumerRecord<String, String> consumerRecord) {
    log.warn("DLQ 수신: partition={}, offset={}, value={}, headers={}",
        consumerRecord.partition(), consumerRecord.offset(), consumerRecord.value(), consumerRecord.headers());
  }

}
```

```java
// DlqTest.java
package io.hkarling.learning.kafka;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;

@Slf4j
@SpringBootTest
@ActiveProfiles("chapter13")
@DisplayName("에러 처리 — 재시도, DLQ")
class DlqTest {

  @Autowired
  KafkaTemplate<String, String> kafkaTemplate;

  @Test
  @DisplayName("재시도를 다 소진한 메시지는 DLQ 토픽으로 옮겨진다")
  void failedMessageGoesToDlq() throws Exception {
    kafkaTemplate.send(KafkaTopics.ORDER_EVENTS, "dlq-key", "fail-dlq-test").get();
    kafkaTemplate.send(KafkaTopics.ORDER_EVENTS, "dlq-key-2", "normal-message").get();

    Thread.sleep(30000); // 지수 백오프 재시도 + DLQ 발행/수신까지 관찰할 시간
  }

}
```

### 2. 시행착오 — DLT 접미사 오타 (`.DLT` vs `-dlt`)

처음엔 `ORDER_EVENTS_DLT = ORDER_EVENTS + ".DLT"`로 안내받아 작성했는데,
`OrderEventDlqObserver`가 `.DLT`(리터럴, 앞에 원본 토픽명이 안 붙은 이상한
이름)라는 토픽을 구독하는 로그(`partitions assigned: [.DLT-0]`)가 찍혔다.
Spring Kafka 4.1.0 소스를 직접 까보니:

```java
DEFAULT_DESTINATION_RESOLVER = (cr, e) -> new TopicPartition(cr.topic() + "-dlt", cr.partition());
```

**기본 접미사는 `-dlt`(하이픈, 소문자)였다** — 필자가 옛날 버전 기억으로
잘못 안내했던 것. `ORDER_EVENTS + "-dlt"`로 고쳐서 해결했다.

이 리터럴 문자열을 직접 타이핑하는 대신, Spring Kafka가 제공하는
`org.springframework.kafka.retrytopic.RetryTopicConstants.DEFAULT_DLT_SUFFIX`
상수를 쓰기로 했다 — 오타를 원천적으로 막기 위해서다. `DeadLetterPublishingRecoverer`의
기본 리졸버 자신은 이 상수를 안 쓰고 리터럴을 직접 박아뒀지만(Spring Kafka
코드베이스 자체의 비일관성), 우리 코드에서는 상수를 쓰는 게 안전하다.

**참고로 리졸버를 직접 호출해서 "이 토픽의 DLT 이름이 뭐야?"라고 물어보는
방법은 없다** — `DEFAULT_DESTINATION_RESOLVER`가 `private static final`이라
접근 불가능하고, 설령 접근 가능했어도 `BiFunction<ConsumerRecord, Exception,
TopicPartition>`이라 런타임에 레코드가 있어야 계산되는 값인데
`@KafkaListener(topics = ...)`는 컴파일 타임 상수만 받는다 (Chapter 6에서
확인한 그 제약). 그래서 상수를 가져와 문자열로 직접 조합하는 게 유일한
방법이었다.

### 3. 시행착오 — leftover 누적으로 재현이 여러 번 필요했음

디버깅 과정에서 테스트를 여러 번 돌리다 보니(토픽 이름 오타 수정 전후,
sleep 시간 부족 등으로 중간에 끊긴 실행들), Chapter 8에서 이미 겪었던
패턴이 그대로 재현됐다 — `fail-dlq-test`가 완주하지 못한 채 여러 번 쌓여서,
재실행할 때마다 leftover를 하나씩 순서대로 복구하느라 "왜 새로 보낸 메시지가
아니라 계속 옛날 것만 처리되지?"로 헷갈렸다.

`order-events`/`order-events-dlt` 토픽을 완전히 지우고 재생성해서 깨끗한
상태로 재현했다:
```
docker compose exec kafka kafka-topics --delete --topic order-events --bootstrap-server localhost:9092
docker compose exec kafka kafka-topics --delete --topic order-events-dlt --bootstrap-server localhost:9092
docker compose exec kafka kafka-topics --create --topic order-events --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
```

### 4. 최종 확인 — 전체 사이클 한 번에 관찰

`Thread.sleep`을 30초로 늘리고 깨끗한 토픽에서 재실행하니, 전체 사이클이
한 번에 다 보였다:

```
처리 실패 로그가 지수 백오프 간격으로 반복 (총 ~18초):
  500ms → 1.1s → 2.1s → 4.0s → 5.0s(캡) → 5.0s(캡)
Backoff 소진 → offset 1(normal-message)로 seek
DLQ 수신 — original-partition/offset 헤더에 원본 위치 그대로
처리 완료: offset=1, value=normal-message  ← 정상 메시지가 드디어 처리됨
```

재시도가 소진된 직후 컨테이너가 다음 레코드(정상 메시지)로 넘어가서 정상
처리하는 것까지 확인됐다 — Chapter 8에서 배운 "실패 레코드는 다음 레코드가
처리·커밋될 때 같이 묻혀 넘어간다"는 원리가 DLQ 발행이라는 새로운 복구
방식에서도 동일하게 적용된다는 것도 재확인했다.

### 5. `&` vs `&&` 실수

`OrderEventDlqConsumer`에 `consumerRecord.value() != null &
consumerRecord.value().startsWith("fail")`처럼 비트 AND(`&`)가 들어가 있었다.
지금 데이터로는 결과가 같지만 short-circuit이 안 되어서, `value()`가 `null`인
레코드(예: tombstone)를 만나면 `NullPointerException`이 날 수 있는 잠재
버그였다. `&&`로 수정했다.

## 시행착오 / Q&A

**Q. `dlqErrorHandler`처럼 커스텀 recoverer/backOff를 주면
`DefaultErrorHandler`의 원래 기본 동작(예: 역직렬화 예외 비재시도 분류)을
잃는 거 아닌가?**
A. 아니다. `DefaultErrorHandler`가 상속하는 `ExceptionClassifier`의 무인자
생성자가 이 기본 분류를 항상 설정하는데, 어떤 `DefaultErrorHandler` 생성자
오버로드를 쓰든 상위 클래스 생성자는 먼저 실행된다. 즉 생성자 인자로
바뀌는 건 recoverer와 backOff 두 가지뿐, 나머지 동작은 그대로 유지된다.
소스를 직접 확인해서 검증했다.

**Q. DLT 토픽 이름을 리졸버에서 직접 조회해올 수는 없나?**
A. 안 된다. 리졸버가 `private`인 데다, `@KafkaListener(topics = ...)`가
컴파일 타임 상수만 받기 때문에 설령 조회 API가 있었어도 애노테이션에는
못 썼을 것이다. `RetryTopicConstants.DEFAULT_DLT_SUFFIX` 상수로 직접
조합하는 게 최선이었다.

**Q. "처리 실패"/"처리 완료" 로그가 전혀 안 보였던 이유는?**
A. 두 가지가 겹쳤다. ① 처음엔 실패 케이스에 `log.warn`이 아예 없어서
"처리 실패" 로그가 원천적으로 안 찍혔다. ② "처리 완료"(정상 메시지)는 앞
레코드(실패한 메시지)가 재시도를 다 소진하기 전까지 순서상 처리될 기회
자체가 없었다 — 재시도가 다 끝나기 전에 sleep이 먼저 끝나서 안 보인
것뿐이었다.

**Q. 새 컨슈머 그룹이 구독을 시작하면 예전에 쌓인 내용을 다 처리해야만 넘어가는 게
정상 동작인가?**
A. 그렇다. Kafka 파티션은 순서가 고정된 로그라 오프셋 순서대로만 읽을 수 있고,
커밋된 오프셋이 없는 그룹은 `auto-offset-reset` 설정(이 프로젝트는 계속
`earliest`)에 따라 처음부터 읽는다. 앞 레코드가 재시도 중이면 뒤 레코드는
순서상 절대 먼저 처리되지 않는다 — Chapter 8·9와 이번 챕터에서 반복된 leftover
문제가 전부 이 원리 하나로 설명된다. "지금부터 것만 보고 싶다"면
`auto-offset-reset: latest`로 바꾸면 되는데, 이 프로젝트는 재생 현상을 직접
체감하는 게 학습 목적이라 일부러 `earliest`를 계속 썼다.

**Q. `auto-offset-reset` 같은 설정은 yaml에 전역으로 두는 것보다 컨슈머
팩토리별로 나누는 게 맞나?**
A. "얼마나 다른 게 필요한가"에 따라 다르다. ① 앱의 모든 컨슈머가 같은 정책이면
yaml 전역 설정이 가장 단순하다(불필요한 추상화를 미리 만들지 않는다).
② 리스너 하나(또는 소수)만 다르면 `@KafkaListener(properties =
"auto.offset.reset=latest")`처럼 리스너 단위로 오버라이드하는 게 전용
`ConsumerFactory` 빈을 새로 만드는 것보다 가볍다. ③ 역직렬화기, trusted
packages처럼 **여러 속성이 세트로 함께 달라져야** 할 때만(`jsonConsumerFactory`가
이 경우) 전용 팩토리를 만든다. 속성 하나 때문에 팩토리를 통째로 새로 만드는 건
과한 추상화다.

## 트레이드오프 / 실무 함정 / 안티패턴

**트레이드오프**: DLQ는 메시지 유실을 막아주지만, "DLQ에 쌓인 걸 누가
언제 어떻게 재처리할지"는 별도로 설계해야 한다. 그냥 쌓아두기만 하면
"조용히 사라지는 것"이 "조용히 방치되는 것"으로 바뀔 뿐이다.

**실무 함정**: 원본 토픽과 DLT 토픽의 파티션 수가 다르면(이번 실습에서
실제로 겪음), `DeadLetterPublishingRecoverer`가 원본과 같은 파티션 번호로
보내려다 "존재하지 않는 파티션" 경고를 내고 프로듀서가 임의로 파티션을
정하게 된다 — 발행 자체는 성공하지만, "같은 키는 같은 파티션"이라는
보장이 DLQ 쪽에서는 깨질 수 있다는 뜻이다. 또한 재현 테스트를 여러 번
돌리면 leftover가 쌓여 디버깅이 헷갈리는 건 Chapter 8부터 계속 반복되는
패턴이니, 확실하게 재현하고 싶을 땐 토픽을 리셋하는 습관이 필요하다.

**안티패턴**: `&`/`&&`, `|`/`||`를 혼동해서 쓰는 것 — 지금 당장은 결과가
같아 보여도 short-circuit 여부가 달라서 예상치 못한 예외(NPE 등)로 이어질
수 있다. 컴파일러가 안 잡아주는 실수라 리뷰에서 놓치기 쉽다.

## 더 생각해볼 것

DLQ에 쌓인 메시지를 실제로 재처리하려면 어떤 도구/절차가 필요할까 —
수동으로 원본 토픽에 재발행하는 도구, 아니면 일정 주기로 자동 재시도하는
별도 컨슈머? 이번엔 다루지 않은 `@RetryableTopic`(비차단 재시도 —
재시도마다 별도 토픽을 만들어 원래 파티션을 막지 않는 방식)도 대안으로
남겨둔다. 배치 리스너(Chapter 11)와 DLQ를 조합하면 배치 안 레코드 하나가
실패했을 때 배치 전체를 DLQ로 보낼지, 그 레코드만 골라낼지도 별도 설계가
필요하다.

## 최종 구성

`KafkaTopics`에 `ORDER_EVENTS_DLT` 추가(`RetryTopicConstants.DEFAULT_DLT_SUFFIX`
사용). `KafkaConfig`에 `dlqErrorHandler`(`DeadLetterPublishingRecoverer` +
`ExponentialBackOff`), `dlqKafkaListenerContainerFactory` 추가, 그리고
Chapter 7 버그 수정으로 이미 추가됐던 `stringProducerFactory`/`kafkaTemplate`을
이번 챕터의 DLQ 발행에도 그대로 사용. `OrderEventDlqConsumer`,
`OrderEventDlqObserver` 신규(둘 다 `@Profile("chapter13")`). 테스트
`DlqTest`(`failedMessageGoesToDlq`) 작성.

## ADR

### Decision
DLQ 목적지 토픽 이름은 `DeadLetterPublishingRecoverer`의 기본 규칙
(`원본토픽 + "-dlt"`)을 그대로 따르되, 접미사 리터럴을 직접 타이핑하지 않고
`RetryTopicConstants.DEFAULT_DLT_SUFFIX` 상수를 사용해 조합한다.

### Drivers
기본 접미사가 `.DLT`(점, 대문자)라고 잘못 기억하고 있다가 직접 겪은
오타(그래서 옵저버가 엉뚱한 토픽을 구독) 때문에, 리터럴 문자열 대신
프레임워크가 제공하는 상수를 쓰는 쪽이 안전하다고 판단했다.

### Alternatives
`DeadLetterPublishingRecoverer` 생성 시 커스텀 목적지 리졸버
(`BiFunction<ConsumerRecord, Exception, TopicPartition>`)를 직접 지정해서
원하는 이름 규칙을 쓰는 방법도 있었으나, 기본 규칙을 그대로 쓰는 게
Spring Kafka의 관례를 따르는 것이고 커스터마이징할 이유가 없어 기각.

### Consequences
디버깅 과정에서 Chapter 7의 `@ConditionalOnMissingBean` 버그, Chapter 8
스타일의 leftover 누적 문제를 다시 마주쳤다 — 새로운 문제가 아니라 이미
배운 패턴이 반복된 것이라 원인 파악은 빨랐다. `KafkaConfig`가 챕터를
거치며 계속 누적되고 있으니(LOG007 ADR에서 예견한 대로), 다음 챕터부터는
필요하면 책임별로 분리하는 것도 고려할 시점이다.

### Follow-ups
Chapter 14 — Testcontainers로 Kafka 통합 테스트. 지금까지는 로컬 Docker
Compose의 실제 Kafka에 의존해서 테스트해왔는데, CI 환경에서도 재현 가능한
격리된 테스트로 전환하는 방법을 다룬다.
