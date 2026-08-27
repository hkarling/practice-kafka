package io.hkarling.learning.semantics;

import static org.assertj.core.api.Assertions.assertThat;

import io.hkarling.learning.kafka.KafkaTopics;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest
@ActiveProfiles("chapter16")
@DisplayName("배달 보장 — at-most-once / at-least-once / 프로듀서·트랜잭션 EOS의 경계")
class DeliverySemanticsTest {

  @Autowired
  KafkaTemplate<String, String> kafkaTemplate;

  @Autowired
  OrderProcessingLogRepository logRepository;

  @Autowired
  RedeliveryTransactionService redeliveryTransactionService;

  @Test
  @DisplayName("A. at-most-once: 커밋을 먼저 하면 크래시 시 메시지가 영구 유실된다")
  void atMostOnceLosesMessageOnCrash() throws Exception {
    String orderId = "order-lost-" + UUID.randomUUID();
    RecordMetadata metadata = kafkaTemplate
        .send(KafkaTopics.ORDER_EVENTS_SEMANTICS, orderId, "crash:" + orderId)
        .get().getRecordMetadata();

    Thread.sleep(3000);

    assertThat(logRepository.countByOrderId(orderId)).isZero();
    assertThat(alreadyCommittedPast("chapter16-at-most-once-group", metadata)).isTrue();
  }

  private boolean alreadyCommittedPast(String groupId, RecordMetadata metadata) throws Exception {
    Map<String, Object> adminProps = Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    try (AdminClient admin = AdminClient.create(adminProps)) {
      Map<TopicPartition, OffsetAndMetadata> offsets =
          admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();
      OffsetAndMetadata committed = offsets.get(new TopicPartition(metadata.topic(), metadata.partition()));
      return committed != null && committed.offset() > metadata.offset();
    }
  }

  @Test
  @DisplayName("B. at-least-once: 커밋을 나중에 하면 크래시 시 같은 메시지가 재전달되어 중복 처리된다")
  void atLeastOnceDuplicatesMessageOnCrash() throws Exception {
    String orderId = "order-dup-consumer-" + java.util.UUID.randomUUID();
    kafkaTemplate.send(KafkaTopics.ORDER_EVENTS_SEMANTICS, orderId, "crash:" + orderId).get();

    int count = waitForCountGreaterThan(orderId, 1, Duration.ofSeconds(60));

    assertThat(count).isGreaterThan(1);
  }

  private int waitForCountGreaterThan(String orderId, int threshold, java.time.Duration timeout)
      throws InterruptedException {
    long deadline = System.currentTimeMillis() + timeout.toMillis();
    int count;
    do {
      count = logRepository.countByOrderId(orderId);
      if (count > threshold) {
        return count;
      }
      Thread.sleep(500);
    } while (System.currentTimeMillis() < deadline);
    return count;
  }

  @Test
  @DisplayName("C. 프로듀서 멱등성은 앱 코드가 같은 이벤트를 두 번 보내는 것까지는 못 막는다")
  void producerIdempotenceDoesNotCoverApplicationLevelDoubleSend() throws Exception {
    String orderId = "order-dup-producer-" + UUID.randomUUID();

    // 앱이 "응답을 못 받아 실패한 줄 알고" 같은 이벤트를 두 번 보낸다고 가정
    kafkaTemplate.send(KafkaTopics.ORDER_EVENTS_SEMANTICS, orderId, "EVENT:" + orderId).get();
    kafkaTemplate.send(KafkaTopics.ORDER_EVENTS_SEMANTICS, orderId, "EVENT:" + orderId).get();

    assertThat(countRecordsForKey(orderId, "read_uncommitted")).isEqualTo(2);
  }

  private int countRecordsForKey(String key, String isolationLevel) {
    Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "verify-" + UUID.randomUUID());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolationLevel);

    try (Consumer<String, String> consumer = new DefaultKafkaConsumerFactory<String, String>(props).createConsumer()) {
      consumer.subscribe(List.of(KafkaTopics.ORDER_EVENTS_SEMANTICS));
      List<ConsumerRecord<String, String>> matched = new ArrayList<>();
      long deadline = System.currentTimeMillis() + 5000;
      while (System.currentTimeMillis() < deadline) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
        for (ConsumerRecord<String, String> record : records) {
          if (record.key().equals(key)) {
            matched.add(record);
          }
        }
      }
      return matched.size();
    }
  }

  @Test
  @DisplayName("D. Kafka 트랜잭션(EOS)은 같은 이벤트가 두 번 들어오는 것 자체는 못 막는다")
  void kafkaTransactionDoesNotDedupeAcrossSeparateInvocations() {
    String orderId = "order-dup-tx-" + UUID.randomUUID();

    // 업스트림이 같은 이벤트를 두 번 재전달했다고 가정 — 각각은 독립적으로 원자적(EOS)이다
    redeliveryTransactionService.recordEvent(orderId);
    redeliveryTransactionService.recordEvent(orderId);

    assertThat(logRepository.countByOrderId(orderId)).isEqualTo(2);   // DB에도 중복
    assertThat(countRecordsForKey(orderId, "read_committed")).isEqualTo(2); // 토픽에도 진짜 커밋된 중복 2건
  }

}