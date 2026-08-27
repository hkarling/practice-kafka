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
public class AtMostOnceConsumer {

  private final OrderProcessingLogRepository logRepository;

  @KafkaListener(
      id = "at-most-once-listener",
      topics = KafkaTopics.ORDER_EVENTS_SEMANTICS,
      groupId = "chapter16-at-most-once-group",
      containerFactory = "manualAckKafkaListenerContainerFactory")
  public void listen(ConsumerRecord<String, String> consumerRecord, Acknowledgment ack) {
    ack.acknowledge(); // 처리도 안했는데 먼저 커밋 - '받았다'와 '처리했다'를 구분하지 않는 방식

    if (consumerRecord.value() != null && consumerRecord.value().startsWith("crash:")) {
      log.warn("크래시 시뮬레이션 — 오프셋은 이미 커밋되어 이 메시지는 다시 오지 않는다: orderId={}", consumerRecord.key());
      // DB 반영 없이 종료
      return;
    }

    logRepository.insert(consumerRecord.key());
    log.info("처리 완료: orderId={}", consumerRecord.key());
  }
}
