package io.hkarling.learning.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@Profile("chapter12")
public class OrderEventJsonConsumer {

  @KafkaListener(
      id = "order-event-json-listener",
      topics = KafkaTopics.ORDER_EVENTS_JSON,
      groupId = "chapter12-json-group",
      containerFactory = "jsonKafkaListenerContainerFactory")
  public void listen(OrderEvent event) {
    log.info("수신: {}", event);
  }
}
