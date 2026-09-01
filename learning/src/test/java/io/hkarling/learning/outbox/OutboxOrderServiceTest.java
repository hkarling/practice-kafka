package io.hkarling.learning.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.hkarling.learning.kafka.KafkaTopics;
import io.hkarling.learning.transaction.OrderRepository;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.test.context.ActiveProfiles;

@Slf4j
@SpringBootTest
@ActiveProfiles("chapter18")
@DisplayName("Outbox 패턴 — DB 트랜잭션과 Kafka 발행의 원자성")
class OutboxOrderServiceTest {

  @Autowired
  OutboxOrderService outboxOrderService;

  @Autowired
  OrderRepository orderRepository;

  @Autowired
  OutboxEventRepository outboxEventRepository;

  @Autowired
  JdbcTemplate jdbcTemplate;

  @Test
  @DisplayName("정상 케이스: 주문과 outbox 기록이 같은 트랜잭션으로 커밋된다")
  void placeOrderCommitsOrderAndOutboxTogether() {
    String orderId = "outbox-order-" + System.currentTimeMillis();

    outboxOrderService.placeOrder(orderId, false);

    assertThat(orderRepository.countByOrderId(orderId)).isEqualTo(1);
    assertThat(outboxEventRepository.countByAggregateId(orderId)).isEqualTo(1);
  }

  @Test
  @DisplayName("실패 케이스: 예외가 나면 주문과 outbox 기록이 함께 롤백된다")
  void placeOrderRollsBackOrderAndOutboxTogether() {
    String orderId = "outbox-order-faile-" + System.currentTimeMillis();

    assertThatThrownBy(() -> outboxOrderService.placeOrder(orderId, true))
        .isInstanceOf(IllegalStateException.class);

    assertThat(orderRepository.countByOrderId(orderId)).isZero();
    assertThat(outboxEventRepository.countByAggregateId(orderId)).isZero();
  }

  @Test
  @DisplayName("Relay가 폴링으로 outbox 이벤트를 Kafka에 발행하고 PUBLISHED로 마킹한다")
  void relayPublishesPendingEventAndMarksPublished() {
    String orderId = "outbox-order-relay-" + System.currentTimeMillis();

    outboxOrderService.placeOrder(orderId, false);
    assertThat(waitForKafkaMessage(orderId, Duration.ofSeconds(5))).isTrue();
    assertThat(waitForPublishedStatus(orderId, Duration.ofSeconds(5))).isTrue();
  }

  private boolean waitForKafkaMessage(String orderId, Duration timeout) {
    Map<String, Object> consumerProps = Map.of(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
        ConsumerConfig.GROUP_ID_CONFIG, "outbox-verify-" + orderId,
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    try (Consumer<String, String> consumer =
        new DefaultKafkaConsumerFactory<String, String>(consumerProps).createConsumer()) {
      consumer.subscribe(List.of(KafkaTopics.ORDER_EVENTS_OUTBOX));
      long deadline = System.currentTimeMillis() + timeout.toMillis();

      while (System.currentTimeMillis() < deadline) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
        for (ConsumerRecord<String, String> consumerRecord : records) {
          log.info("Kafka에서 확인: value={}", consumerRecord.value());
          if (consumerRecord.key() != null && consumerRecord.key().equals(orderId)) {
            return true;
          }
        }
      }
      return false;
    }
  }

  private boolean waitForPublishedStatus(String orderId, Duration timeout) {
    long deadline = System.currentTimeMillis() + timeout.toMillis();
    while (System.currentTimeMillis() < deadline) {
      String status = jdbcTemplate.queryForObject(
          "SELECT status FROM outbox_event WHERE aggregate_id = ?", String.class, orderId);
      if ("PUBLISHED".equals(status)) {
        return true;
      }
      try {
        Thread.sleep(300);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    return false;
  }
}