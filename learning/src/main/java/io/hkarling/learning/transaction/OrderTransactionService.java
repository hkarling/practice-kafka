package io.hkarling.learning.transaction;

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
@Profile("chapter15")
public class OrderTransactionService {

  private final OrderRepository orderRepository;
  private final KafkaTemplate<String, String> transactionalKafkaTemplate;

  @Transactional
  public void placeOrder(String orderId, boolean simulateFailure) {
    orderRepository.save(orderId, "CREATED");
    transactionalKafkaTemplate.send(KafkaTopics.ORDER_EVENTS, orderId, "ORDER_CREATED:" + orderId);
    log.info("주문 저장 + 이벤트 발행 완료: orderId={}", orderId);

    if (simulateFailure) {
      throw new IllegalStateException("강제 실패 — DB, Kafka 둘 다 롤백되는지 확인");
    }
  }
}
