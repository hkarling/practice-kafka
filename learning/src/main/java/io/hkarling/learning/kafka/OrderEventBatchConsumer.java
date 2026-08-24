package io.hkarling.learning.kafka;

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@Profile("chapter11")
public class OrderEventBatchConsumer {

  @KafkaListener(
      id = "order-event-batch-listener",
      topics = KafkaTopics.ORDER_EVENTS,
      groupId = "chapter11-batch-group",
      containerFactory = "batchKafkaListenerContainerFactory")
  public void listen(List<ConsumerRecord<String, String>> records) {
    log.info("배치 크기: {} 건", records.size());
    for (ConsumerRecord<String, String> record : records) {
      log.info("  partition={}, offset={}, value={}", record.partition(), record.offset(), record.value());
    }
  }

}
