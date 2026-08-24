package io.hkarling.learning.kafka;

import java.time.Instant;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;

@Slf4j
@SpringBootTest
@ActiveProfiles("chapter12")
@DisplayName("직렬화/역직렬화 — JSON")
class JsonSerializationTest {

  @Autowired
  KafkaTemplate<String, OrderEvent> jsonKafkaTemplate;

  @Autowired
  KafkaTemplate<String, String> kafkaTemplate;


  @Test
  @DisplayName("OrderEvent를 JSON으로 발행하고 같은 타입으로 역직렬화해 받는다")
  void sendAndReceiveJson() throws Exception {
    log.info("kafkaTemplate acks 설정: {}", kafkaTemplate.getProducerFactory().getConfigurationProperties().get("acks"));
    OrderEvent event = new OrderEvent("order-json-1", "ORDER_PLACED", Instant.now());
    jsonKafkaTemplate.send(KafkaTopics.ORDER_EVENTS_JSON, event.orderId(), event).get();
    Thread.sleep(3000);
  }

  @Test
  @DisplayName("깨진 JSON(poison pill)을 보내도 컨슈머가 죽지 않고 다음 메시지를 받는다")
  void poisonPillDoesNotKillConsumer() throws Exception {
    kafkaTemplate.send(KafkaTopics.ORDER_EVENTS_JSON, "poison-key", "this-is-not-json").get();

    OrderEvent event = new OrderEvent("order-json-2", "ORDER_PLACED", Instant.now());
    jsonKafkaTemplate.send(KafkaTopics.ORDER_EVENTS_JSON, event.orderId(), event).get();

    Thread.sleep(10000);
  }
}