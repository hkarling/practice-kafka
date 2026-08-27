package io.hkarling.learning.semantics;

import io.hkarling.learning.kafka.KafkaTopics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Slf4j
@RequiredArgsConstructor
@Component
@Profile("chapter16")
public class AtLeastOnceConsumer {

  private final OrderProcessingLogRepository logRepository;

  @KafkaListener(
      id = "at-least-once-listener",
      topics = KafkaTopics.ORDER_EVENTS_SEMANTICS,
      groupId = "chapter16-at-least-once-group",
      containerFactory = "manualAckKafkaListenerContainerFactory")
  public void listen(ConsumerRecord<String, String> consumerRecord, Acknowledgment ack) {
    logRepository.insert(consumerRecord.key());
    log.info("처리(DB 기록) 완료, 커밋 전: orderId = {}", consumerRecord.key());

    if (consumerRecord.value() != null && consumerRecord.value().startsWith("crash:")) {
      throw new IllegalStateException("크래시 시뮬레이션 — 커밋 전에 실패, 재전달된다");
    }

    ack.acknowledge(); // 처리 성공을 학인한 뒤에야 커밋
  }
}
