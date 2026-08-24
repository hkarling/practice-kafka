package io.hkarling.learning.kafka;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;

@Slf4j
@SpringBootTest
@ActiveProfiles("chapter13")
@DisplayName("에러 처리 — 재시도, DLQ")
class DlqTest {

  @Autowired
  KafkaTemplate<String, String> kafkaTemplate;

  @Test
  @DisplayName("재시도를 다 소진한 메시지는 DLQ 토픽으로 옮겨진다")
  void failedMessageGoesToDlq() throws Exception {
    kafkaTemplate.send(KafkaTopics.ORDER_EVENTS, "dlq-key", "fail-dlq-test").get();
    kafkaTemplate.send(KafkaTopics.ORDER_EVENTS, "dlq-key-2", "normal-message").get();

    Thread.sleep(30000); // 지수 백오프 재시도 + DLQ 발행/수신까지 관찰할 시간
  }

}