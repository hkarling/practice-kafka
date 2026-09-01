# LOG009 — Consumer Group: 파티션 할당, 리밸런싱

## 배경 / 목표

Phase 1 챕터 9. LOG008 마지막 질문 — "지금은 컨슈머가 그룹당 하나뿐이라 파티션 3개를
혼자 다 가져간다. 컨슈머가 여러 개면 파티션이 어떻게 나뉘고, 하나가 죽으면 무슨 일이
일어날까?" — 를 이어서, 같은 그룹 안에 컨슈머 여러 개를 실제로 띄워 파티션 배정과
리밸런싱을 관찰한다.

## 개념 정리

### 1. Consumer Group

같은 `group-id`를 공유하는 컨슈머들의 집합이다. 파티션 하나는 그룹 안에서 항상 정확히
하나의 컨슈머에게만 배정된다 — 병렬 처리량의 상한은 컨슈머 수가 아니라 **파티션 수**다.
LOG006에서 예고했던 "파티션이 순서 보장과 병렬성의 트레이드오프 축"이라는 내용이, 이번
챕터에서는 "파티션 수가 곧 컨슈머 병렬성의 물리적 한계"라는 형태로 다시 등장한다.

### 2. 파티션 할당 전략(Assignor)

| 전략 | 동작 방식 | 특징 |
|---|---|---|
| `Range` (기본값) | 파티션 번호 순서대로 순차 배정 | 나머지가 생기면 앞쪽 컨슈머가 더 가져감 (불균등 가능) |
| `RoundRobin` | 컨슈머에게 파티션을 한 개씩 순환 배정 | Range보다 균등하지만, 리밸런싱 시 배정이 크게 재구성될 수 있음 |
| `(Cooperative)Sticky` | 가능한 한 이전 배정을 유지하며 최소한만 재배정 | 리밸런싱 비용(정지 범위)이 가장 작음 |

이번 챕터는 기본값인 `Range`로 실습했고, `concurrency=2`일 때 실제로 앞쪽 컨슈머가
파티션을 더 많이 가져가는 불균등 배정을 실측으로 확인했다(아래 진행 과정 2번).

### 3. 리밸런싱 트리거

컨슈머 조인, 정상/비정상 이탈(세션 타임아웃), 처리 로직이 `max.poll.interval.ms`를
넘겨 "죽은 컨슈머"로 판단될 때 발생한다. 마지막 경우가 특히 실무에서 놓치기 쉬운데,
컨슈머 프로세스 자체는 멀쩡히 살아있어도 리스너 메서드 하나가 오래 걸리면 그룹 코디네이터
입장에서는 "응답 없는 컨슈머"로 간주해 파티션을 다른 컨슈머에게 넘겨버린다.

### 4. 정상 탈퇴 vs 장애 탈퇴

`close()`로 명시적으로 나가면 즉시 `LeaveGroup` 요청이 전송되어 리밸런싱이 바로
트리거된다. 반면 프로세스가 죽어서 하트비트가 끊기면 `session.timeout.ms`(기본 45초)까지
기다린 뒤에야 죽은 것으로 간주된다 — 장애 복구 속도에 큰 차이를 만든다. 이 비대칭은 아래
진행 과정 3번에서 `close()` 케이스만 실측했다(강제 종료로 세션 타임아웃을 재현하려면
45초를 온전히 기다려야 해서 이번 챕터 범위에서는 다루지 않았다).

### 5. 리밸런싱 중 오프셋 보존

Spring Kafka 컨테이너는 파티션을 반납(revoke)하기 직전에 자동으로 커밋한다. 그래서
리밸런싱이 일어나도 새로 그 파티션을 받은 컨슈머는 마지막 커밋 위치부터 이어서 처리한다
(오프셋이 0으로 리셋되지 않음). 이건 LOG006에서 확인한 "브로커가 커밋된 오프셋을
기억한다"는 성질이 있어야만 가능한 동작이다 — LOG004의 `EventLog`처럼 오프셋이 컨슈머
메모리에만 있었다면, 파티션이 다른 컨슈머로 넘어가는 순간 그 진행 위치 정보 자체가
사라졌을 것이다.

## 진행 과정

### 1. 코드 구성과 시행착오

`OrderEventGroupConsumer`를 새 그룹(`chapter09-group`)·`concurrency` 속성으로
구성해서, 같은 그룹 안에 여러 `KafkaConsumer` 인스턴스를 JVM 하나에서 재현했다.

```yaml
# application-chapter09.yaml
spring:
  application:
    name: learning
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: chapter09-default-group
      auto-offset-reset: earliest
```

```java
// OrderEventGroupConsumer.java
package io.hkarling.learning.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@Profile("chapter09")
public class OrderEventGroupConsumer {

  @KafkaListener(
      id = "orderEventGroupListener",
      topics = KafkaTopics.ORDER_EVENTS,
      groupId = "chapter09-group",
      concurrency = "2")
  public void listen(ConsumerRecord<String, String> consumerRecord) {
    log.info("[{}] partition={}, offset={}, value={}",
        Thread.currentThread().getName(),
        consumerRecord.partition(), consumerRecord.offset(), consumerRecord.value());
  }

}
```

구성 과정에서 세 가지 문제를 만났다.

**① yaml 기본 group-id와 명시적 groupId 충돌** — `application-chapter09.yaml`의
`consumer.group-id`를 처음엔 `chapter09-group`으로 지정했는데, `groupId`를 안 준
`OrderEventConsumer`가 이 값을 그대로 상속받아 `OrderEventGroupConsumer`
(`groupId="chapter09-group"`, `concurrency=3`)와 같은 그룹에 합류해버렸다. 의도한
3개가 아니라 4개 컨슈머가 파티션 3개를 다투게 되어, 1명은 빈 배정(`[]`)을
받았다. yaml 기본값을 `chapter09-default-group`으로 분리해서 해결.

**② `order-events` 토픽이 파티션 1개로 축소되어 있었음** — 새 그룹뿐 아니라
기존 단일 컨슈머 그룹(`chapter08-manual-group`)조차 파티션을 하나만 받는 걸 보고
의심해서 `kafka-topics --describe`로 확인하니 `PartitionCount: 1`이었다. 이전에
Docker 환경이 재구성되면서(정확한 시점은 특정 못 함) 토픽이 기본 파티션 수로
재생성된 것으로 추정 — CLAUDE.md에 이미 적어둔 "다른 PC/재시작 후 파티션 3개로
재생성 필요"가 실제로 발생한 사례였다. `kafka-topics --alter --partitions 3`로
파티션 수만 늘려서 복구했다 (기존 데이터 유지, 키를 안 쓰므로 파티션 재계산으로
인한 순서 문제도 없음).

**③ 이전 챕터 리스너가 항상 같이 뜨는 노이즈** — `@Component`는 활성 프로파일과
무관하게 항상 컴포넌트 스캔되어, Chapter 8의 `OrderEventManualAckConsumer`가
Chapter 9 테스트에도 매번 같이 조인해 로그를 채우고 그룹 형성 시간도 늘렸다.
챕터 전용 실험 컨슈머에 `@Profile("chapterNN")`을 붙여 해당 프로파일에서만
뜨도록 격리했다 (`OrderEventGroupConsumer`에 붙인 `@Profile("chapter09")`가
위 코드에 이미 반영됨). Chapter 8의 `OrderEventManualAckConsumer`에도 동일하게
추가:

```java
@Slf4j
@Component
@Profile("chapter08")  // 추가
public class OrderEventManualAckConsumer {
  // ... (리스너 로직은 LOG008과 동일)
}
```

`OrderEventConsumer`는 여러 챕터에서 재사용하는 베이스라인 성격이라 프로파일로
묶지 않고 그대로 둠 — LOG007 ADR의 "기능으로 이름 붙인다" 원칙과 일관되게,
챕터 전용 실험 코드만 프로파일로 격리.

### 2. 정적 배정 — 균등(3:3)과 불균등(2:3) 관찰

```java
// ConsumerGroupTest.java
package io.hkarling.learning.kafka;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.ActiveProfiles;

@Slf4j
@SpringBootTest
@ActiveProfiles("chapter09")
@DisplayName("Consumer Group — 파티션 할당")
class ConsumerGroupTest {

  @Autowired
  KafkaTemplate<String, String> kafkaTemplate;

  @Autowired
  KafkaListenerEndpointRegistry registry;

  @Test
  @DisplayName("concurrency=3인 컨슈머 그룹이 파티션 3개를 배정받는다")
  void observePartitionAssignment() throws Exception {
    MessageListenerContainer container = registry.getListenerContainer("orderEventGroupListener");

    ContainerTestUtils.waitForAssignment(container, 3); // order-events 파티션 3개가 배정될 때까지 대기(폴링), sleep 대체

    for (int i = 0; i < 9; i++) {
      kafkaTemplate.send("order-events", "group-test-" + i).get();
    }
  }
}
```

처음엔 `Thread.sleep(5000)`으로 "컨슈머가 조인할 시간"을 그냥 확보하는 방식이었는데,
`spring-boot-starter-kafka-test`가 제공하는 `ContainerTestUtils.waitForAssignment()`로
바꿨다 — 실제로 파티션이 배정될 때까지 폴링하고, 타임아웃 시 예외를 던지므로 테스트
자체가 "파티션이 제대로 배정됐다"는 assertion 역할을 한다. `@KafkaListener`에 `id`를
명시해야 `registry.getListenerContainer(id)`로 컨테이너를 찾을 수 있다.

`concurrency=3`으로 실행하면 파티션 3개가 정확히 1:1:1로 나뉘었다:
```
Finished assignment for group: {
  consumer-chapter09-group-3 = [order-events-0],
  consumer-chapter09-group-4 = [order-events-1],
  consumer-chapter09-group-5 = [order-events-2]
}
```

`concurrency=2`로 줄이면 Range Assignor가 앞쪽 컨슈머에게 나머지를 몰아줬다:
```
consumer-chapter09-group-2 = [order-events-0, order-events-1]
consumer-chapter09-group-3 = [order-events-2]
```

### 3. 동적 리밸런싱 — raw `KafkaConsumer`로 조인/이탈 재현

Spring이 관리하는 컨테이너는 시작 시 한꺼번에 조인해버려서 "이미 안정된 그룹에
나중에 합류"하는 상황을 재현할 수 없다. 그래서 테스트 안에서 순수
`org.apache.kafka.clients.consumer.KafkaConsumer`를 직접 만들어 같은 그룹에
끼워넣었다.

```java
// RebalanceTest.java
package io.hkarling.learning.kafka;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;

@Slf4j
@SpringBootTest
@ActiveProfiles("chapter09")
@DisplayName("Consumer Group — 리밸런싱")
class RebalanceTest {

  @Autowired
  KafkaTemplate<String, String> kafkaTemplate;

  @Test
  @DisplayName("안정된 그룹에 새 컨슈머가 조인하면 리밸런싱이 일어난다")
  void newConsumerJoinsAndTriggersRebalance() throws InterruptedException {
    log.info("=== 5초 대기: chapter09-group이 안정될 시간 ===");
    Thread.sleep(5000);

    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "chapter09-group"); // 같은 그룹!
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    log.info("=== raw KafkaConsumer가 chapter09-group에 조인합니다 ===");
    try (KafkaConsumer<String, String> rawConsumer = new KafkaConsumer<>(props)) {
      rawConsumer.subscribe(List.of("order-events"));
      rawConsumer.poll(Duration.ofSeconds(3)); // poll을 해야 실제로 join이 진행됨

      log.info("=== 5초 대기: 조인으로 인한 리밸런싱 로그 관찰 ===");
      Thread.sleep(5000);

      log.info("=== raw KafkaConsumer가 그룹을 떠납니다 ===");
    } // try-with-resources가 close() 호출 → 그룹에서 정상 탈퇴

    log.info("=== 5초 대기: 탈퇴로 인한 리밸런싱 로그 관찰 ===");
    Thread.sleep(5000);
  }
}
```

**결과** (컨슈머 2개로 안정된 상태에서 시작):

| 단계 | 배정 |
|---|---|
| 조인 전 | `[0,1]` / `[2]` (컨슈머 2개) |
| raw consumer 조인 → 리밸런싱 | `[0]` / `[1]` / `[2]` (컨슈머 3개, 1:1:1) |
| raw consumer `close()` → 리밸런싱 | `[0,1]` / `[2]` (컨슈머 2개, 조인 전과 동일) |

조인 시 로그에 `Setting offset for partition order-events-0 to the committed
offset FetchPosition{offset=27}`이 찍힌 게 핵심 — 파티션을 새로 받은 컨슈머가
오프셋 0이 아니라 **이전 컨슈머가 커밋해둔 위치(27)부터** 이어받았다. `close()`
직후에는 `sending LeaveGroup request ... due to the consumer is being closed`로
즉시 탈퇴 처리되어, 세션 타임아웃을 기다리지 않고 바로 리밸런싱이 시작됐다.

## 시행착오 / Q&A

**Q. 컨슈머 그룹에 멤버가 왜 의도한 것보다 하나 더 많이 잡히나?**
A. `groupId`를 안 준 `@KafkaListener`는 yaml의 `consumer.group-id` 기본값을
상속받는다. 그 기본값과 다른 리스너의 명시적 `groupId`가 우연히 같으면 그룹이
합쳐진다 (위 진행 과정 ① 참고).

**Q. 컨슈머가 하나뿐인 그룹인데 왜 파티션을 하나만 받나?**
A. 배정 로직 문제가 아니라 토픽 자체의 파티션 수가 줄어 있었다. `kafka-topics
--describe`로 실제 파티션 수부터 확인하는 게 먼저다 (위 진행 과정 ② 참고).

**Q. 리밸런싱 중 파티션을 새로 받은 컨슈머는 어디서부터 읽나?**
A. 0이 아니라 마지막 커밋된 오프셋부터. Spring Kafka 컨테이너가 파티션을
반납하기 직전 자동으로 커밋해주기 때문에 데이터 유실/중복 재처리가 최소화된다.

**Q. 정상 종료(`close()`)와 프로세스 강제 종료는 리밸런싱 타이밍이 다른가?**
A. 다르다. `close()`는 `LeaveGroup`을 명시적으로 보내 즉시 리밸런싱을 트리거하지만,
하트비트가 그냥 끊기는 경우(강제 종료, 네트워크 단절)는 `session.timeout.ms`
(기본 45초)까지 기다려야 한다 — 장애 감지 속도에 실질적 차이가 있다.

## 트레이드오프 / 실무 함정 / 안티패턴

**트레이드오프**: 컨슈머를 늘리면 처리량이 늘지만 파티션 수가 상한이다 — 그
이상 늘리면 유휴 컨슈머만 늘어난다. Eager 리밸런싱(기본)은 리밸런싱 중 그룹
전체가 멈추는 비용이 있다 — Cooperative 방식은 이 정지 범위를 줄이지만 이번
챕터에서는 다루지 않았다.

**실무 함정**: yaml 프로파일 기본값과 리스너에 명시한 `groupId`가 겹치면
의도치 않게 그룹이 합쳐진다 — 여러 리스너가 있는 프로젝트에서 그룹 이름
관리를 느슨하게 하면 실제 운영에서도 똑같이 발생할 수 있는 문제다. 또한
`@Component` 기반 리스너는 프로파일과 무관하게 항상 뜬다는 걸 모르면, "왜
상관없는 컨슈머가 같이 로그를 찍고 리밸런싱에 끼어들지?"로 헤매기 쉽다.

**안티패턴**: `concurrency`를 파티션 수보다 많이 주는 것 — 남는 컨슈머는
영원히 빈 배정만 받고 리소스만 차지한다. 그룹 이름을 프로젝트 전역에서
재사용 가능한 `default-group`처럼 지어서, 다른 컨텍스트(다른 챕터/다른
서비스)가 무심코 같은 그룹에 합류하게 만드는 것도 마찬가지다.

## 더 생각해볼 것

지금까지는 키 없이 발행해서 메시지가 어느 파티션에 갈지 예측할 수 없었다.
같은 주문 ID의 이벤트들이 항상 같은 파티션(=같은 컨슈머)으로 가서 순서가
보장되게 하려면 어떻게 설계해야 할까? → Chapter 10(이벤트 순서 보장, 파티션
키 설계)로 이어지는 질문.

## 최종 구성

`application-chapter09.yaml` 추가 (`group-id: chapter09-default-group`).
`OrderEventGroupConsumer` 신규 (`@Profile("chapter09")`, `groupId
="chapter09-group"`, `concurrency` 조절 실험용). `OrderEventManualAckConsumer`에
`@Profile("chapter08")` 추가. 테스트 `ConsumerGroupTest`(정적 배정 관찰),
`RebalanceTest`(동적 조인/이탈 관찰) 작성 — 코드는 위 진행 과정 참고.

## ADR

### Decision
컨슈머 그룹 내 다중 컨슈머는 `@KafkaListener`의 `concurrency` 속성으로,
"이미 안정된 그룹에 새 컨슈머가 늦게 합류/이탈"하는 동적 리밸런싱은 테스트
안에서 순수 `KafkaConsumer`를 직접 생성해 재현했다. 챕터 전용 실험 리스너
(`OrderEventGroupConsumer`, `OrderEventManualAckConsumer`)에는 `@Profile`을
붙여 해당 챕터 프로파일에서만 뜨도록 격리했다.

### Drivers
Spring이 관리하는 리스너 컨테이너는 애플리케이션 시작 시점에 설정된 수만큼
한꺼번에 그룹에 조인하기 때문에, "그룹이 이미 안정된 뒤 새 멤버가 조인"하는
리밸런싱의 핵심 상황을 `concurrency`만으로는 재현할 수 없었다. 또한
`@Component`는 활성 프로파일과 무관하게 항상 스캔되므로, 챕터별 실험 코드가
누적될수록 서로 다른 챕터의 리스너들이 같이 떠서 로그가 뒤섞이고 그룹 형성
시간도 늘어나는 문제가 실제로 발생했다.

### Alternatives
두 번째 애플리케이션 인스턴스를 별도 터미널로 띄워 진짜 다중 프로세스
리밸런싱을 재현하는 방법도 고려했으나, 이 프로젝트의 "테스트로 실습하고
결과를 로그로 남긴다"는 흐름과 맞지 않아 raw `KafkaConsumer`를 테스트 안에
직접 생성하는 방식을 택했다.

### Consequences
챕터 전용 실험 리스너에 `@Profile`을 붙이는 관례가 이번에 추가됐다 — 앞으로
새 챕터의 실험용 컨슈머/프로듀서를 만들 때도 이 패턴을 따르는 게 좋다. 다만
여러 챕터에서 재사용하는 베이스라인 성격의 코드(`OrderEventConsumer` 등)는
프로파일로 묶지 않는다는 기준(LOG007 ADR과 일관)도 함께 유지한다.

### Follow-ups
Chapter 10 — 이벤트 순서 보장 (파티션 키 설계).
