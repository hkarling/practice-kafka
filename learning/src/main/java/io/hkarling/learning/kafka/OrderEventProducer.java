package io.hkarling.learning.kafka;

import java.nio.charset.StandardCharsets;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
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

  public void sendWithHeader(String key, String value, String source) {
    ProducerRecord<String, String> producerRecord
        = new ProducerRecord<>(KafkaTopics.ORDER_EVENTS, key, value);
    producerRecord.headers().add("source", source.getBytes(StandardCharsets.UTF_8));
    kafkaTemplate.send(producerRecord);
  }

}
