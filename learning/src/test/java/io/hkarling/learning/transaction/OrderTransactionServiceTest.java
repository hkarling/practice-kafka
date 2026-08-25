package io.hkarling.learning.transaction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.hkarling.learning.kafka.KafkaTopics;
import java.time.Duration;
import java.util.HashMap;
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
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.test.context.ActiveProfiles;

@Slf4j
@SpringBootTest
@ActiveProfiles("chapter15")
@DisplayName("Kafka 트랜잭션과 DB 트랜잭션 동기화")
class OrderTransactionServiceTest {

  @Autowired
  OrderTransactionService orderTransactionService;

  @Autowired
  OrderRepository orderRepository;


  @Test
  @DisplayName("정상 케이스: DB 저장과 Kafka 발행이 함께 커밋된다")
  void placeOrderSucceeds() {
    String orderId = "tx-order-" + System.currentTimeMillis();

    orderTransactionService.placeOrder(orderId, false);

    assertThat(orderRepository.countByOrderId(orderId)).isEqualTo(1);
    assertThat(orderEventCommitted(orderId)).isTrue();
  }

  @Test
  @DisplayName("실패 케이스: 예외가 나면 DB 저장도 롤백된다")
  void placeOrderRollsBackOnFailure() {
    String orderId = "tx-order-fail-" + System.currentTimeMillis();

    assertThatThrownBy(() -> orderTransactionService.placeOrder(orderId, true))
        .isInstanceOf(IllegalStateException.class);

    assertThat(orderRepository.countByOrderId(orderId)).isZero();
    assertThat(orderEventCommitted(orderId)).isFalse();
  }

  private boolean orderEventCommitted(String orderId) {
    Map<String, Object> consumerProps = new HashMap<>();
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "tx-verify-" + orderId);
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

    try (Consumer<String, String> consumer =
        new DefaultKafkaConsumerFactory<String, String>(consumerProps).createConsumer()) {
      consumer.subscribe(List.of(KafkaTopics.ORDER_EVENTS));
      long deadline = System.currentTimeMillis() + 5000;
      while (System.currentTimeMillis() < deadline) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
        for (ConsumerRecord<String, String> record : records) {
          log.info("read_committed 컨슈머로 확인: orderId={}, value={}", orderId, record.value());
          if (orderId.equals(record.key())) {
            return true;
          }
        }
      }
      return false;
    }
  }
}