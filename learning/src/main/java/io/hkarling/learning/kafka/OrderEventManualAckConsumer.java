package io.hkarling.learning.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@Profile("chapter08")
public class OrderEventManualAckConsumer {

  @KafkaListener(
      topics = KafkaTopics.ORDER_EVENTS,
      groupId = "chapter08-manual-group",
      containerFactory = "manualAckKafkaListenerContainerFactory")
  public void listen(ConsumerRecord<String, String> consumerRecord, Acknowledgment acknowledgment) {
    if (consumerRecord.value() != null && consumerRecord.value().startsWith("fail")) {
      log.warn("처리 실패 시뮬레이션: partition={}, offset={}, value={}",
          consumerRecord.partition(), consumerRecord.offset(), consumerRecord.value());
      throw new IllegalStateException("처리 실패: " + consumerRecord.value());
    }
    log.info("처리 완료 → 커밋: partition={}, offset={}, value={}",
        consumerRecord.partition(), consumerRecord.offset(), consumerRecord.value());
    acknowledgment.acknowledge();
  }
}
