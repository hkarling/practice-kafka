package io.hkarling.learning.cqrs;

import io.hkarling.learning.kafka.KafkaTopics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@RequiredArgsConstructor
@Component
@Profile("chapter20")
public class OrderSummaryProjector {

  private final OrderSummaryRepository orderSummaryRepository;

  @KafkaListener(
      id = "order-summary-projector",
      topics = KafkaTopics.ORDER_EVENTS_OUTBOX,
      groupId = "chapter20-order-summary-group")
  public void listen(ConsumerRecord<String, String> consumerRecord) {
    String orderId = consumerRecord.key();
    String status = consumerRecord.value().split(":")[0];
    orderSummaryRepository.upsert(orderId, status);
    log.info("읽기 모델 갱신: orderId={}, status={}", orderId, status);
  }
  
}
