package io.hkarling.learning.kafka;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@Slf4j
@SpringBootTest
@ActiveProfiles("chapter11")
@DisplayName("KafkaTemplate + @KafkaListener 기본 — 헤더, 배치")
class KafkaListenerFeaturesTest {

  @Autowired
  OrderEventProducer producer;

  @Test
  @DisplayName("커스텀 헤더를 실어 보내고, 유연한 파라미터로 수신한다")
  void sendAndReceiveWithHeader() throws InterruptedException {
    producer.sendWithHeader("order-header-test", "결제 완료", "payment-service");
    Thread.sleep(3000);
  }

  @Test
  @DisplayName("배치 리스너는 여러 레코드를 한 번에 묶어서 받는다")
  void batchListenerReceivesMultipleRecords() throws InterruptedException {
    for (int i = 0; i < 10; i++) {
      producer.send("batch-test-" + i);
    }
    Thread.sleep(3000);
  }
}