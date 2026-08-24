package io.hkarling.learning.kafka;

import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;

@Slf4j
@SpringBootTest
@ActiveProfiles("chapter08")
@DisplayName("Consumer 동작 원리 — 커밋/오프셋")
class ManualAckConsumerTest {

  @Autowired
  KafkaTemplate<String, String> kafkaTemplate;

  @Test
  @DisplayName("정상 메시지는 즉시 커밋되고, fail 메시지는 재시도 후 포기된다")
  void manualAckAndRetry() throws InterruptedException, ExecutionException {
    kafkaTemplate.send("order-events", "manual-ok-1").get();
    kafkaTemplate.send("order-events", "fail-1").get();
    kafkaTemplate.send("order-events", "manual-ok-2").get();

    Thread.sleep(15000); // 콘솔 로그로 처리/재시도/포기 흐름 관찰
  }
}
