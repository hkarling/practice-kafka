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