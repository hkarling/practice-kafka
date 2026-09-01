package io.hkarling.learning.outbox;

import io.hkarling.learning.kafka.KafkaTopics;
import io.hkarling.learning.transaction.OrderRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@RequiredArgsConstructor
@Service
@Profile({"chapter18", "chapter20"})
public class OutboxOrderService {

  private final OrderRepository orderRepository;
  private final OutboxEventRepository outboxEventRepository;

  @Transactional
  public void placeOrder(String orderId, boolean simulateFailure) {
    orderRepository.save(orderId, "CREATED");
    outboxEventRepository.save(orderId, KafkaTopics.ORDER_EVENTS_OUTBOX, "ORDER_CREATED: " + orderId);
    log.info("주문 저장 + outbox 기록 완료 (같은 DB 트랜잭션): orderId={}", orderId);
    if (simulateFailure) {
      throw new IllegalStateException("강제 실패 — orders, outbox_event 둘 다 롤백되는지 확인");
    }
  }
}
