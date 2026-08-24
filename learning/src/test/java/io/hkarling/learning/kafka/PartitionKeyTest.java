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