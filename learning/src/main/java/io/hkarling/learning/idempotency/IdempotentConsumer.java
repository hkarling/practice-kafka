package io.hkarling.learning.idempotency;

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
@Profile("chapter17")
public class IdempotentConsumer {

  private final IdempotentOrderLogRepository logRepository;

  @KafkaListener(
      id = "idempotent-listener",
      topics = KafkaTopics.ORDER_EVENTS_IDEMPOTENT,
      groupId = "chapter17-idempotent-group",
      containerFactory = "manualAckKafkaListenerContainerFactory")
  public void listen(ConsumerRecord<String, String> consumerRecord, Acknowledgment ack) {
    String orderId = consumerRecord.key();

    if (!logRepository.tryMarkProcessed(orderId)) {
      log.info("이미 처리된 이벤트 — skip: orderId={}", orderId);
      ack.acknowledge();
      return;
    }
    log.info("신규 이벤트 처리 완료: orderId={}", orderId);

    if (consumerRecord.value() != null && consumerRecord.value().startsWith("crash:")) {
      throw new IllegalStateException("크래시 시뮬레이션 — 멱등키는 이미 커밋됐다, 재전달돼도 안전한지 확인");
    }

    ack.acknowledge();
  }

}
