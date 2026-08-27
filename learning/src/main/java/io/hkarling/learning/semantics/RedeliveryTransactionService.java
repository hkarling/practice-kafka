package io.hkarling.learning.semantics;

import io.hkarling.learning.kafka.KafkaTopics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@RequiredArgsConstructor
@Service
@Profile("chapter16")
public class RedeliveryTransactionService {

  private final OrderProcessingLogRepository logRepository;
  private final KafkaTemplate<String, String> transactionalKafkaTemplate;

  @Transactional
  public void recordEvent(String orderId) {
    logRepository.insert(orderId);
    transactionalKafkaTemplate.send(KafkaTopics.ORDER_EVENTS_SEMANTICS, orderId, "EVENT RECORDED: " + orderId);
    log.info("이벤트 기록(DB) + 발행 완료: orderId={}", orderId);
  }

}
