package io.hkarling.learning.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@Profile("chapter13")
public class OrderEventDlqConsumer {

  @KafkaListener(
      id = "order-event-dlq-listener",
      topics = KafkaTopics.ORDER_EVENTS,
      groupId = "chapter13-dlq-group",
      containerFactory = "dlqKafkaListenerContainerFactory")
  public void listen(ConsumerRecord<String, String> consumerRecord) {
    if (consumerRecord.value() != null && consumerRecord.value().startsWith("fail")) {
      log.warn("처리 실패: partition={}, offset={}, value={}",
          consumerRecord.partition(), consumerRecord.offset(), consumerRecord.value());
      throw new IllegalStateException("처리 실패: " + consumerRecord.value());
    }
    log.info("처리 완료: partition={}, offset={}, value={}",
        consumerRecord.partition(), consumerRecord.offset(), consumerRecord.value());
  }

}
