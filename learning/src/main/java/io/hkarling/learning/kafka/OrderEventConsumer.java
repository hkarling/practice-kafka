package io.hkarling.learning.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class OrderEventConsumer {

  @KafkaListener(topics = KafkaTopics.ORDER_EVENTS)
  public void listen(ConsumerRecord<String, String> consumerRecord) {
    log.info("partition={}, offset={}, key={}, value={}",
        consumerRecord.partition(), consumerRecord.offset(), consumerRecord.key(), consumerRecord.value());
  }
}
