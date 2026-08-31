package io.hkarling.learning.idempotency;

import static org.assertj.core.api.Assertions.assertThat;

import io.hkarling.learning.kafka.KafkaTopics;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;

@Slf4j
@SpringBootTest
@ActiveProfiles("chapter17")
@DisplayName("컨슈머 멱등성 — 멱등키 체크로 재전달/재시도 중복을 막는다")
class IdempotentConsumerTest {

  private static final String GROUP_ID = "chapter17-idempotent-group";

  @Autowired
  KafkaTemplate<String, String> kafkaTemplate;

  @Autowired
  IdempotentOrderLogRepository logRepository;

  @Test
  @DisplayName("A. 같은 이벤트가 재전달돼도 멱등키 체크로 한 번만 처리된다")
  void duplicateRedeliveryIsProcessedOnce() throws Exception {
    String orderId = "order-idem-dup-" + UUID.randomUUID();

    kafkaTemplate.send(KafkaTopics.ORDER_EVENTS_IDEMPOTENT, orderId, "EVENT:" + orderId).get();
    RecordMetadata second = kafkaTemplate
        .send(KafkaTopics.ORDER_EVENTS_IDEMPOTENT, orderId, "EVENT:" + orderId)
        .get().getRecordMetadata();

    waitUntilCommittedPast(GROUP_ID, second, Duration.ofSeconds(10));

    assertThat(logRepository.countByOrderId(orderId)).isEqualTo(1);
  }

  @Test
  @DisplayName("B. 처리(멱등키 기록) 후 커밋 전 크래시가 나도 재시도 시 중복 없이 안전하다")
  void crashBeforeAckIsSafeOnRetry() throws Exception {
    String orderId = "order-idem-crash-" + UUID.randomUUID();

    RecordMetadata metadata = kafkaTemplate
        .send(KafkaTopics.ORDER_EVENTS_IDEMPOTENT, orderId, "crash:" + orderId)
        .get().getRecordMetadata();

    waitUntilCommittedPast(GROUP_ID, metadata, Duration.ofSeconds(15));

    assertThat(logRepository.countByOrderId(orderId)).isEqualTo(1);
  }

  private void waitUntilCommittedPast(String groupId, RecordMetadata metadata, Duration timeout)
      throws Exception {
    Map<String, Object> adminProps = Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    long deadline = System.currentTimeMillis() + timeout.toMillis();

    try (AdminClient admin = AdminClient.create(adminProps)) {
      while (System.currentTimeMillis() < deadline) {
        Map<TopicPartition, OffsetAndMetadata> offsets =
            admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();
        OffsetAndMetadata committed = offsets.get(new TopicPartition(metadata.topic(), metadata.partition()));
        if (committed != null && committed.offset() > metadata.offset()) {
          return;
        }
        Thread.sleep(300);
      }
      throw new AssertionError("타임아웃까지 오프셋이 커밋되지 않음: " + metadata);
    }
  }
}