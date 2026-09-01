package io.hkarling.learning.cqrs;

import static org.assertj.core.api.Assertions.assertThat;

import io.hkarling.learning.kafka.KafkaTopics;
import io.hkarling.learning.outbox.OutboxOrderService;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest
@ActiveProfiles("chapter20")
@DisplayName("CQRS — 이벤트로 읽기 모델(order_summary) 동기화")
class OrderSummaryProjectorTest {

  @Autowired
  OutboxOrderService outboxOrderService;

  @Autowired
  KafkaTemplate<String, String> kafkaTemplate;

  @Autowired
  JdbcTemplate jdbcTemplate;

  @Test
  @DisplayName("주문 생성이 Outbox를 거쳐 읽기 모델에 반영된다")
  void orderCreationProjectsToReadModel() {
    String orderId = "cqrs-order-" + System.currentTimeMillis();
    
    outboxOrderService.placeOrder(orderId, false);

    assertThat(waitForStatus(orderId, "ORDER_CREATED", Duration.ofSeconds(10))).isTrue();
  }

  @Test
  @DisplayName("같은 이벤트가 재전달돼도 UPSERT라 읽기 모델이 깨지지 않는다")
  void duplicateEventIsIdempotentViaUpsert() throws Exception {
    String orderId = "cqrs-dup-" + System.currentTimeMillis();

    kafkaTemplate.send(KafkaTopics.ORDER_EVENTS_OUTBOX, orderId, "ORDER_CREATED: " + orderId).get();
    kafkaTemplate.send(KafkaTopics.ORDER_EVENTS_OUTBOX, orderId, "ORDER_CREATED: " + orderId).get();

    assertThat(waitForStatus(orderId, "ORDER_CREATED", Duration.ofSeconds(10))).isTrue();

    Integer count = jdbcTemplate.queryForObject(
        "SELECT COUNT(*) FROM order_summary WHERE order_id = ?", Integer.class, orderId);
    assertThat(count).isEqualTo(1);
  }

  private boolean waitForStatus(String orderId, String expected, Duration timeout) {
    long deadline = System.currentTimeMillis() + timeout.toMillis();
    while (System.currentTimeMillis() < deadline) {
      List<String> rows = jdbcTemplate.query(
          "SELECT status FROM order_summary WHERE order_id = ?",
          (rs, rowNum) -> rs.getString("status"), orderId);
      if (!rows.isEmpty() && expected.equals(rows.getFirst())) {
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