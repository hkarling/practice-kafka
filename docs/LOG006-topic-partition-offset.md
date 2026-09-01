# LOG006 — 토픽, 파티션, 오프셋

## 배경 / 목표

Phase 1 시작. Phase 0에서 개념으로 다룬 것들을 실제 Kafka로 확인한다. 특히 LOG004의
`EventLog`(append-only 로그 + 오프셋)가 사실 Kafka **파티션 하나**를 흉내 낸 것이었다는
전제 위에서, `EventLog`로는 확인할 수 없었던 "토픽이 여러 파티션으로 쪼개져 있으면
무슨 일이 생기는가"를 실측한다. 프로젝트에서 처음으로 진짜 Kafka 브로커를 직접 기동해서
쓰는 챕터.

## 개념 정리

### 1. 토픽(Topic)

메시지의 논리적 분류 단위다. 프로듀서/컨슈머는 토픽 이름으로 "어떤 종류의 메시지를
주고받는지"를 지정한다 — 물리적으로 어디에 어떻게 저장되는지는 다음 개념(파티션)이
담당한다.

### 2. 파티션(Partition)

토픽은 실제로 여러 개의 독립된 append-only 로그로 쪼개져 저장된다. 파티션 하나가
LOG004의 `EventLog`에 해당한다 — LOG004에서 "브로커는 로그만 순서대로 저장하고, 컨슈머가
자기 오프셋을 스스로 관리한다"고 배운 그 append-only 로그가, 실제 Kafka에서는 토픽 하나당
여러 개 병렬로 존재하는 것이다.

### 3. 순서 보장의 범위

순서는 **파티션 안에서만** 보장된다. 토픽 전체로 보면 순서가 보장되지 않는다 — 파티션이
늘어날수록 병렬 처리량은 늘지만 전역 순서는 포기하게 된다. 이 트레이드오프가 Chapter
10(이벤트 순서 보장, 파티션 키 설계)의 핵심 주제로 이어진다 — "순서가 중요한 이벤트는
같은 파티션에 모아야 한다"는 결론이 여기서 이미 예고된다.

### 4. 오프셋(Offset)

파티션 내 메시지 위치를 가리키는 정수다. LOG004의 `EventLog`와 결정적으로 다른 지점이
있는데, 거기서는 오프셋을 컨슈머 인스턴스가 메모리로만 들고 있어서 컨슈머가 죽으면
진행 위치 정보도 함께 사라졌다. 진짜 Kafka는 컨슈머가 커밋한 오프셋을 **브로커 자신이
기억한다**(`__consumer_offsets` 내부 토픽) — 컨슈머가 재시작해도 마지막 처리 위치부터
이어갈 수 있다. 이번 챕터의 진행 과정 4번(재시도 시 미커밋분이 되살아난 것)이 바로 이
차이를 실측으로 보여준 사례다.

### 5. 파티션 결정 방식

프로듀서가 키(key)를 지정하면 같은 키는 항상 같은 파티션으로 간다(해시 기반). 키가
없으면 파티셔너가 알아서 분배한다 — 이번 실습에서 그 분배가 "메시지 단위"가 아니라
"배치 단위"라는 걸 실측으로 확인했다(아래 진행 과정 참고). 키를 지정하는 이유는 단순
분산이 아니라 "같은 키의 메시지는 같은 파티션 → 같은 순서로 처리되게 하기 위함"이다 —
파티션 내 순서 보장(3번)과 곧바로 연결되는 실무적 이유다.

## 진행 과정

### 1. 토픽 생성

```
docker compose exec kafka kafka-topics --create \
  --topic order-events --partitions 3 --replication-factor 1 \
  --bootstrap-server localhost:9092
```

`--describe`로 파티션 3개, 각각 리더가 브로커 1번인 것을 확인.

### 2. Best-practice 코드 작성

Phase 1부터는 Spring Kafka라는 프레임워크의 관용적 사용법이 있는 영역이라, 완성된
예시 코드를 먼저 제공받고 그 설계 이유를 이해하는 방식으로 전환했다 (Phase 0의
"힌트만 주고 직접 설계"와 다른 접근 — CLAUDE.md에도 반영).

```yaml
# application-chapter06.yaml
spring:
  application:
    name: learning
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: chapter06-group
      auto-offset-reset: earliest
```

```java
// KafkaTopics.java — 토픽 이름을 상수화 (@KafkaListener(topics=...)가
// 컴파일 타임 상수만 허용하기 때문)
package io.hkarling.learning.kafka;

public final class KafkaTopics {

  public static final String ORDER_EVENTS = "order-events";

  private KafkaTopics() {
  }

}
```

```java
// OrderEventProducer.java — KafkaTemplate을 직접 여기저기서 주입받지 않고
// 전용 컴포넌트로 감싸서 발행 로직을 한 곳에 모음
package io.hkarling.learning.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
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

}
```

```java
// OrderEventConsumer.java — @KafkaListener + ConsumerRecord<String, String>으로
// partition()/offset()까지 확인 가능하게 구성
package io.hkarling.learning.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class OrderEventConsumer {

  @KafkaListener(topics = KafkaTopics.ORDER_EVENTS)
  public void listen(ConsumerRecord<String, String> consumerRecord) {
    log.info("partition={}, offset={}, key={}, value={}",
        consumerRecord.partition(), consumerRecord.offset(), consumerRecord.key(), consumerRecord.value());
  }
}
```

```java
// OrderEventProducerTest.java
package io.hkarling.learning.kafka;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest
@ActiveProfiles("chapter06")
@DisplayName("토픽, 파티션, 오프셋")
class OrderEventProducerTest {

  @Autowired
  OrderEventProducer producer;

  @Test
  @DisplayName("키 없이 발행하면 파티션에 고르게 분산된다")
  void sendWithoutKey() throws InterruptedException {
    for (int i = 1; i <= 6; i++) {
      producer.send("order-" + i);
    }
    Thread.sleep(5000);
  }

  @Test
  @DisplayName("같은 키로 발행하면 항상 같은 파티션으로 간다")
  void sendWithKey() throws InterruptedException {
    producer.send("order-A", "결제 시작");
    producer.send("order-A", "결제 완료");
    producer.send("order-B", "결제 시작");
    Thread.sleep(5000);
  }

}
```

### 3. 첫 실행(2초 대기) — 로그가 하나도 안 찍힘

`Thread.sleep(2000)` 후에도 컨슈머 로그가 전혀 안 찍혔다.

**원인**: `@KafkaListener` 컨테이너는 컨텍스트가 뜨자마자 바로 폴링을 시작하는 게
아니라, 코디네이터 발견 → 그룹 join → 파티션 할당의 과정을 거쳐야 한다. 이 과정이
콜드 스타트에서 2초보다 오래 걸려서, 컨슈머가 join하기 전에 테스트가 끝나버렸다.

### 4. 5초로 늘려 재실행 — 로그는 찍혔지만 메시지가 중복돼 보임

```
sendWithoutKey(): order-1~6이 partition=1(offset 0~5)과 partition=0(offset 0~5)
                  양쪽에 각각 찍힘 (총 12줄)
```

**원인 분석**: 1번 시도 때 프로듀서는 이미 `order-1`~`order-6`을 실제로 발행했었다
(컨슈머만 못 받았을 뿐). 그 메시지들은 파티션 1에 쌓인 채 **커밋되지 않고** 남아있었다.
2번 시도에서 같은 `chapter06-group`으로 다시 join하니, 커밋된 오프셋이 없어서
`auto-offset-reset: earliest`에 따라 파티션 1을 처음부터 다시 읽었다 — 그게 1번
시도의 미처리분이다. 동시에 2번 시도에서 새로 보낸 메시지는 파티션 0으로 갔다. 즉
메시지가 중복 발행된 게 아니라 **"1번 시도의 미커밋분 + 2번 시도분"이 합쳐져서
보인 것**이다.

이건 LOG004의 `EventLog`(오프셋을 컨슈머 메모리로만 관리, 컨슈머가 죽으면 그 오프셋
정보도 사라짐)와 실제 Kafka의 핵심 차이를 그대로 보여준다 — 진짜 Kafka는 컨슈머가
늦거나 죽어도 커밋 안 된 메시지가 유실되지 않고 브로커에 남아있다가, 그 그룹이 다시
join하면 이어서 처리된다.

### 5. 깨끗한 결과를 위해 토픽 재생성 — 파티션 옵션을 빠뜨림

`kafka-topics --delete`로 토픽을 지우고 `--create`로 다시 만들었는데, 이번엔
`--partitions 3`을 빠뜨려서 기본값인 **1개**로 생성됐다. `--describe`로
`PartitionCount: 1`을 확인하고 다시 `--partitions 3`을 명시해서 재생성했다.

### 6. 최종 실행 — 파티션 3개, 깨끗한 상태

```
partition=2, offset=0~5: order-1~order-6 (키 없음, 전부 partition 2에 몰림)
partition=2, offset=6: order-A "결제 시작"
partition=2, offset=7: order-A "결제 완료"
partition=0, offset=0: order-B "결제 시작"
```

**해설**:
- 키 없는 6개가 여러 파티션에 흩어지지 않고 **전부 partition 2에** 몰렸다 — Kafka의
  기본 파티셔너(Sticky/Uniform Sticky Partitioner)가 메시지 하나하나를 라운드로빈
  하는 게 아니라 **배치 단위로 파티션을 고정**하기 때문이다. 대신 그 파티션 안에서는
  보낸 순서(order-1~6)와 offset 순서(0~5)가 정확히 일치했다 — 파티션 내 순서 보장은
  그대로 지켜졌다.
- `order-A`는 두 번 다 partition 2로 갔다 — 같은 키는 같은 파티션이라는 게 확인됐다.
- `order-B`는 partition 0으로 갔다 — 다른 키는 다른 파티션으로 갈 수 있다는 것도
  확인됐다.

## 시행착오 / Q&A

**Q. 2초 기다렸는데 로그가 하나도 안 찍힌 이유는?**
A. 컨슈머 그룹 join(코디네이터 발견, join/sync, 파티션 할당)이 sleep 시간보다 오래
걸렸기 때문. 5초로 늘려서 해결했다.

**Q. 재시도했더니 같은 메시지가 파티션 두 곳에 나온 이유는?**
A. 1번 시도가 발행까지는 성공했지만 컨슈머가 커밋하기 전에 테스트가 끝나 미커밋
상태로 남았고, 2번 시도가 같은 group-id로 재join하면서 `earliest` 설정에 따라
그 미처리분까지 함께 읽었다. 실제로는 메시지 중복이 아니라 두 번의 시도 결과가
합쳐진 것.

**Q. 토픽을 다시 만들었는데 왜 파티션이 1개였나?**
A. `--partitions 3` 옵션을 빠뜨려서 기본값(1)으로 생성됐다. `--describe`로 확인하는
습관이 없었다면 그대로 진행해서 "파티션 분산"을 확인하지 못한 채 챕터를 끝낼 뻔했다.

## 트레이드오프 / 실무 함정 / 안티패턴

**트레이드오프**: 파티션을 늘리면 처리량(병렬성)은 늘지만 전역 순서를 포기하게
된다. 또한 한번 늘린 파티션 수는 줄일 수 없고, 늘리면 기존 키의 파티션 매핑이
깨질 수 있다(해시 기반이라 파티션 수가 바뀌면 같은 키도 다른 파티션으로 갈 수 있음).

**실무 함정**: 같은 `group-id`로 반복 테스트하면, 이전 실행에서 커밋되지 않은/커밋된
상태가 다음 실행에 그대로 영향을 준다 (이번 4번 과정에서 직접 겪음). 로컬에서
재현성 있는 결과를 보려면 토픽을 리셋하거나, 테스트마다 다른 group-id를 쓰는 게
낫다. 또한 토픽 재생성 시 파티션 수를 명시하지 않으면 조용히 기본값(1)으로
생성된다 — 반드시 `--describe`로 확인하는 습관이 필요하다.

**안티패턴**: "키 없이 보내면 여러 파티션에 골고루 분산된다"고 단정하는 것. 이번
실습에서 확인했듯 기본 파티셔너는 배치 단위로 파티션을 고정하기 때문에, 짧은 시간
안에 보낸 메시지들은 오히려 한 파티션에 몰릴 수 있다. "파티션 3개 = 병렬 3배"라고
단순하게 기대하면 안 된다.

## 더 생각해볼 것

키 없는 배치가 파티션 하나에 몰린 게 이번엔 오히려 "파티션 내 순서 보장"을 깔끔하게
보여줬다. 그렇다면 이 배치가 파티션 경계를 넘어설 만큼 커지거나 시간 간격을 두고
발행되면(예: 메시지 사이에 `Thread.sleep`을 넣는다면), 파티셔너가 다른 파티션으로
전환하는 시점은 언제일까? — Chapter 7(Producer 동작 원리 — 배치, acks, 재시도)에서
이 "배치"라는 개념 자체를 제대로 다룬다.

## 최종 구성

`learning` 모듈에 `io.hkarling.learning.kafka` 패키지로 `KafkaTopics`,
`OrderEventProducer`, `OrderEventConsumer`를 추가하고 테스트
`OrderEventProducerTest`를 작성했다. `application-chapter06.yaml` 프로파일을
추가했다. Docker Compose의 Kafka에 `order-events` 토픽(파티션 3개)을 생성했다.

## ADR

### Decision
로컬 실습에서 컨슈머 그룹의 잔존 상태(미커밋 오프셋 등)로 인한 비결정적 결과를
인위적으로 격리하지 않고, 실제로 겪은 그대로 진행하고 원인을 분석해서 문서화한다.

### Drivers
테스트 격리를 위해 매번 그룹을 리셋하거나 고유 group-id를 쓰면 결과는 깔끔해지지만,
"왜 오프셋이 브로커에 남아있고, 그게 왜 중요한지"를 체감할 기회 자체가 사라진다.
이번 챕터의 핵심 개념(영속적 오프셋)을 실습 과정에서 우연히 더 강하게 체감했다.

### Alternatives
매 테스트마다 고유 group-id를 자동 생성(예: UUID 접미사)해서 완전히 격리 — 재현성은
좋아지지만 오프셋 영속성을 체감하기 어려워 기각. Phase 3(멱등성/exactly-once)
근처에서 필요해지면 다시 고려.

### Consequences
이후 챕터에서도 같은 토픽/그룹을 재사용하면 비슷한 잔존 상태 문제가 생길 수 있다 —
필요할 때마다 토픽을 리셋하거나 describe로 상태를 확인하는 습관을 유지한다.

### Follow-ups
Chapter 7 — Producer 동작 원리 (배치, acks, 재시도). 이번에 발견한 "배치 단위 파티션
고정" 현상이 배치 메커니즘과 직접 연결된다.
