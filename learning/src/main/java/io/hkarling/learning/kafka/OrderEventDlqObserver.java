package io.hkarling.learning.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@Profile("chapter13")
public class OrderEventDlqObserver {

  @KafkaListener(
      id = "order-event-dlt-listener",
      topics = KafkaTopics.ORDER_EVENTS_DLT,
      groupId = "chapter13-dlt-observer-group")
  public void listen(ConsumerRecord<String, String> consumerRecord) {
    log.warn("DLQ 수신: partition={}, offset={}, value={}, headers={}",
        consumerRecord.partition(), consumerRecord.offset(), consumerRecord.value(), consumerRecord.headers());
  }

}
