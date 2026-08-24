package io.hkarling.learning.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@Profile("chapter09")
public class OrderEventGroupConsumer {

  @KafkaListener(
      id = "orderEventGroupListener",
      topics = KafkaTopics.ORDER_EVENTS,
      groupId = "chapter09-group",
      concurrency = "2")
  public void listen(ConsumerRecord<String, String> consumerRecord) {
    log.info("[{}] partition={}, offset={}, value={}",
        Thread.currentThread().getName(),
        consumerRecord.partition(), consumerRecord.offset(), consumerRecord.value());
  }

}
