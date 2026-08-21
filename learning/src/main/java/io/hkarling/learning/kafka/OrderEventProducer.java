package io.hkarling.learning.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@RequiredArgsConstructor
@Component
public class OrderEventProducer {

  private final KafkaTemplate<String, String> kafkaTemplate;

  public void send(String value) {
    kafkaTemplate.send(KafkaTopics.ORDER_EVENTS, value);
  }

  public void send(String key, String value) {
    kafkaTemplate.send(KafkaTopics.ORDER_EVENTS, key, value);
  }

}
