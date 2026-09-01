# LOG010 — 이벤트 순서 보장: 파티션 키 설계

## 배경 / 목표

Phase 1 마지막 챕터. LOG009 마지막 질문 — "같은 주문 ID의 이벤트들이 항상 같은
파티션(=같은 컨슈머)으로 가서 순서가 보장되게 하려면 어떻게 설계해야 할까?" —
를 실측으로 확인한다. 컨슈머 그룹 없이, 프로듀서가 반환하는 `RecordMetadata`만으로
검증해서 Chapter 8~9에서 계속 부딪혔던 "컨슈머 그룹 잔존 상태" 문제를 피했다.

## 개념 정리

### 1. 순서는 파티션 안에서만 보장된다 (Chapter 6 복습)

토픽 전체의 순서는 보장되지 않는다 — 토픽은 여러 파티션(독립된 로그)의
묶음이고, 컨슈머는 파티션마다 별개의 오프셋으로 병렬 처리하기 때문에
파티션을 가로지르는 순서는 애초에 정의되지 않는다. Kafka가 보장하는 건
"같은 파티션 안에서는 쓰인 순서 = 읽히는 순서"뿐이다. 그래서 순서가 중요한
이벤트들은 항상 같은 파티션으로 보내야 하고, 그걸 강제하는 유일한 수단이
파티션 키다.

### 2. 파티션 키 = "어떤 단위로 순서를 보장할 것인가"

`order-events`라면 한 주문(`orderId`)의 이벤트(주문생성 → 결제완료 →
배송시작 → 배송완료) 순서가 중요하지, 서로 다른 주문끼리는 순서가 섞여도
상관없다. 즉 파티션 키를 정하는 질문은 "무엇을 빠르게 처리할까"가 아니라
"어떤 단위로 순서가 깨지면 안 되는가"다. 키를 `orderId`로 잡으면 같은
주문의 이벤트는 항상 같은 파티션에 쌓이고, 서로 다른 주문은 자유롭게 여러
파티션에 분산되어 병렬성도 함께 챙길 수 있다.

### 3. 키 선택의 트레이드오프 — Hot Partition

기본 파티셔너는 `hash(key) % numPartitions`로 파티션을 정한다(정확히는
murmur2 해시). 같은 키는 항상 같은 파티션으로 결정론적으로 매핑되므로,
그 키의 모든 메시지가 파티션 하나로 몰린다. 트래픽이 소수의 키에 쏠리면
(예: VIP 고객 하나가 전체 주문의 대부분을 차지), 그 키가 매핑된 파티션만
과부하되고 나머지 파티션은 논다. 이건 "파티션이 부족해서" 생기는 문제가
아니라서, 파티션 수를 아무리 늘려도 그 하나의 hot key는 여전히 파티션
하나에만 갇혀 있다 — 근본적으로 병목을 풀려면 키 설계 자체를 바꿔야 한다
(예: `orderId` 대신 `orderId + 샤드번호`처럼 쪼개는 것도 한 방법이지만,
그러면 이번엔 "같은 주문의 순서 보장"이라는 원래 목적과 충돌한다).

### 4. 파티션 수를 늘리면 매핑이 깨진다 (LOG006 ADR 연결)

`hash(key) % numPartitions` 공식에서 `numPartitions`가 분모다. 파티션
수가 바뀌면 나머지 연산 결과 자체가 달라지므로, 어제까지 파티션 1로
가던 키가 오늘은 파티션 2로 갈 수 있다 — 새로 추가되는 키뿐 아니라
**기존에 이미 쌓여있던 키의 매핑까지 흔들린다.** 순서 보장이 필요한
시스템에서 운영 중 파티션 수를 늘리는 건 사실상 "그 시점부터 순서 보장이
리셋"되는 것과 같다. 그래서 파티션 수는 운영 중 쉽게 못 바꾸는 값으로
취급하고 처음 설계할 때 여유 있게 잡는 게 정석이다.

## 진행 과정

### 1. 코드 준비

```yaml
# application-chapter10.yaml
spring:
  application:
    name: learning
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: chapter10-default-group
      auto-offset-reset: earliest
    producer:
      acks: all
```

`consumer.group-id`는 이번 챕터에서 직접 쓰지 않지만, 그룹 미지정인
`OrderEventConsumer`가 여전히 컴포넌트 스캔되어 yaml 기본값을 상속받기 때문에
필요하다 — Chapter 9에서 배운 "그룹 이름은 항상 챕터 전용으로" 원칙 그대로.

```java
// PartitionKeyTest.java
package io.hkarling.learning.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;

@Slf4j
@SpringBootTest
@ActiveProfiles("chapter10")
@DisplayName("이벤트 순서 보장 — 파티션 키 설계")
class PartitionKeyTest {

  @Autowired
  KafkaTemplate<String, String> kafkaTemplate;

  @Test
  @DisplayName("같은 키로 보낸 이벤트는 항상 같은 파티션에 쌓인다")
  void sameKeyStaysInSamePartition() throws Exception {
    String orderId = "order-test-key";
    String[] events = {"주문생성", "결제 완료", "배송 시작", "배송 완료"};

    Set<Integer> partitions = new HashSet<>();
    for (String event : events) {
      RecordMetadata metadata = kafkaTemplate.send("order-events", orderId, event).get().getRecordMetadata();
      log.info("partition={}, offset={}, value={}", metadata.partition(), metadata.offset(), event);
      partitions.add(metadata.partition());
    }
    assertThat(partitions).hasSize(1); // 전부 같은 파티션 = 순서 보장됨
  }

  @Test
  @DisplayName("트래픽이 소수의 키에 쏠리면 특정 파티션이 과부하된다 (hot partition)")
  void skewedKeyCausesHotPartition() throws Exception {
    Map<Integer, Integer> partitionCounts = new ConcurrentHashMap<>();

    // VIP 계정 하나가 전체 트래픽의 80%를 차지하는 상황을 흉내
    for (int i = 0; i < 24; i++) {
      send("vip-account", "event-" + i, partitionCounts);
    }
    for (int i = 0; i < 6; i++) {
      send("account-" + i, "event-" + i, partitionCounts);
    }

    log.info("파티션별 메시지 수: {}", partitionCounts);
    int max = partitionCounts.values().stream().max(Integer::compareTo).orElseThrow();
    assertThat(max).isGreaterThanOrEqualTo(24); // vip-account 몫이 한 파티션에 몰림
  }

  @Test
  @DisplayName("키 없이 보내면 배치 단위로 파티션이 정해진다 (골고루 분산되지 않을 수 있음)")
  void noKeyDoesNotGuaranteeEvenDistribution() throws Exception {
    Map<Integer, Integer> partitionCounts = new ConcurrentHashMap<>();

    for (int i = 0; i < 30; i++) {
      RecordMetadata metadata = kafkaTemplate.send("order-events", "no-key-event-" + i).get().getRecordMetadata();
      partitionCounts.merge(metadata.partition(), 1, Integer::sum);
    }

    log.info("키 없이 보낸 파티션별 메시지 수: {}", partitionCounts);
  }

  private void send(String key, String value, Map<Integer, Integer> partitionCounts) throws Exception {
    RecordMetadata metadata = kafkaTemplate.send("order-events", key, value).get().getRecordMetadata();
    partitionCounts.merge(metadata.partition(), 1, Integer::sum);
  }
}
```

**설계 이유**: 컨슈머 없이 프로듀서가 반환하는 `RecordMetadata.partition()`만으로
검증한다 — 어느 파티션에 갔는지 즉시 알 수 있고, Chapter 8~9에서 계속 발목을
잡았던 "컨슈머 그룹 잔존 오프셋/백로그" 문제를 아예 피할 수 있다.

### 2. 같은 키 → 같은 파티션 확인

`sameKeyStaysInSamePartition()` 통과 — `orderId`로 보낸 4개 이벤트가 전부 같은
파티션에, 보낸 순서 그대로의 오프셋으로 쌓였다.

### 3. Hot Partition 실측

`skewedKeyCausesHotPartition()` 결과:
```
파티션별 메시지 수: {0=25, 1=2, 2=3}
```

`vip-account`(24건)가 파티션 0으로 몰렸고, 서로 다른 키 6개 중 하나가 우연히
같은 파티션 0으로 해시되면서 25가 됐다(24+1). `assertThat(max)
.isGreaterThanOrEqualTo(24)`로 이 쏠림을 assertion으로 잡아냈다.

### 4. 키 없이 보내면? — 예상보다 더 극단적인 결과

`noKeyDoesNotGuaranteeEvenDistribution()` 결과:
```
키 없이 보낸 파티션별 메시지 수: {1=30}
```

30건 **전부** 파티션 1 하나로 들어갔다. Chapter 6에서 6개가 한 파티션에 몰렸던
것보다 더 극단적인 결과지만 원인은 같다 — 두 가지가 겹쳤다:

1. **단일 브로커 환경**: `replication-factor=1`이라 파티션 0/1/2가 전부 같은
   브로커에 있다. Kafka 최신 클라이언트의 기본 파티셔너(Adaptive/Uniform Sticky
   Partitioner)는 브로커별 부하/응답 속도로 파티션을 고르는데, 브로커가 하나뿐이면
   "이 파티션이 더 낫다"고 판단할 근거 자체가 없다.
2. **Sticky 파티셔너의 재선택 방식**: 파티션은 배치가 완료될 때만 재선택되고,
   재선택한다고 반드시 다른 파티션으로 바뀌는 것도 아니다 — 같은 파티션이 다시
   뽑힐 수 있다.

## 시행착오 / Q&A

**Q. 키 없이 보낸 30건이 전부 파티션 하나에 들어간 게 비정상 아닌가?**
A. 아니다. 단일 브로커 로컬 환경(파티션 간 부하 차이가 없음) + Sticky
파티셔너의 재선택 방식이 겹치면 충분히 나올 수 있는 결과다. 오히려 "키 없이
보내도 알아서 골고루 나뉜다"는 가정이 틀렸다는 걸 더 확실하게 보여준 사례다.

## 트레이드오프 / 실무 함정 / 안티패턴

**트레이드오프**: 키를 쓰면 순서는 보장되지만 그 키의 모든 이벤트가 한
컨슈머(정확히는 그 파티션을 배정받은 컨슈머 인스턴스)에 묶인다. 키 카디널리티가
파티션 수보다 충분히 크고 고르게 분포하면 손실이 미미하지만, 트래픽이 쏠리면
파티션을 늘려도 그 hot partition은 여전히 병목이다 — 이번 챕터의
`skewedKeyCausesHotPartition()`이 그걸 25:2:3으로 눈에 보이게 만들었다.

**실무 함정**: 순서 보장이 필요한 시스템에서 운영 중 파티션 수를 늘리는 것 —
해시 매핑이 깨져서 특정 시점부터 같은 키의 이벤트가 다른 파티션으로 갈 수
있다. 문제는 이게 "그 순간부터 갑자기 안전"해지는 게 아니라, **파티션 수를
늘리기 이전에 이미 쌓여있던 이벤트와, 늘린 이후 들어오는 같은 키의 이벤트가
서로 다른 파티션에 걸쳐 있게 된다는 것**이다 — 컨슈머 입장에선 "이 주문의
과거 이벤트는 파티션 A에, 최신 이벤트는 파티션 B에" 있는 상태가 되어, 어느
한쪽 컨슈머만 봐서는 전체 순서를 알 수 없다. 그래서 파티션 수는 처음 설계할
때부터 여유 있게 잡아야 하고, 부득이하게 늘려야 한다면 "순서 보장이 필요한
구간 동안은 트래픽을 멈추거나, 늘리는 시점 이후의 신규 키만 새 파티션 수
기준으로 순서를 신뢰한다"는 식의 명시적 마이그레이션 전략이 필요하다.

**안티패턴**: 이벤트마다 새로운 UUID를 키로 쓰는 것 — 키를 쓰긴 했지만
사실상 키 없는 것과 같은 분산 효과이면서, "키를 썼으니 순서가 보장된다"는
착각을 하게 만든다. 반대로 모든 이벤트에 동일한 상수 키를 쓰는 것도 안전하지만
파티션을 사실상 하나만 쓰는 것과 같아 병렬성을 완전히 포기하는 셈이다.

## 더 생각해볼 것

기본 해시 기반 파티셔너 대신, "VIP 고객은 전용 파티션으로"처럼 명시적 라우팅
규칙이 필요하다면 `org.apache.kafka.clients.producer.Partitioner`를 직접
구현할 수 있다. Chapter 9의 `concurrency`/Consumer Group과 이번 챕터의 파티션
키 설계를 합치면, "특정 컨슈머 인스턴스가 항상 특정 키(고객군)를 전담 처리"
하게 만들 수 있다 — Phase 2(KafkaTemplate + @KafkaListener 기본)로 이어지는
실전 조합.

## 최종 구성

`application-chapter10.yaml` 추가. 테스트 `PartitionKeyTest`
(`sameKeyStaysInSamePartition`, `skewedKeyCausesHotPartition`,
`noKeyDoesNotGuaranteeEvenDistribution`) 작성. 프로덕션 코드 변경 없음(기존
`OrderEventProducer.send(key, value)` 그대로 사용).

## ADR

### Decision
컨슈머를 띄우지 않고 프로듀서가 반환하는 `RecordMetadata.partition()`만으로
파티션 배정을 검증한다.

### Drivers
Chapter 8~9에서 컨슈머 그룹의 잔존 오프셋/백로그가 결과를 계속 어지럽혔다.
이번 챕터의 핵심(키 → 파티션 매핑)은 프로듀서 응답만으로 완전히 검증 가능해서,
굳이 컨슈머를 얹어 같은 문제를 반복할 이유가 없었다.

### Alternatives
Chapter 6처럼 `OrderEventConsumer`로 실제 소비까지 확인하는 방식 — 검증
자체는 더 "실전"에 가깝지만, 이 챕터의 목표(파티션 매핑 확인)에는 과했다고
판단해 기각.

### Consequences
컨슈머 없이 검증하는 패턴은 프로듀서 동작만 확인하면 되는 이후 챕터에서도
재사용할 수 있다. 다만 "컨슈머가 실제로 순서대로 처리하는가"까지 보려면
여전히 컨슈머를 띄운 검증이 필요하다.

### Follow-ups
Phase 1 완료. Phase 2 — Chapter 11: KafkaTemplate + @KafkaListener 기본.
