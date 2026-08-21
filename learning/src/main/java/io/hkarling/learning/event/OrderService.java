package io.hkarling.learning.event;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

@Slf4j
@RequiredArgsConstructor
@Service
public class OrderService {

  private final ApplicationEventPublisher publisher;

  void placeOrder(String orderId) {
    long start = System.currentTimeMillis();
    publisher.publishEvent(new OrderPlacedEvent(orderId));
    log.info("주문 접수 완료, 소요시간: {}ms", System.currentTimeMillis() - start);
  }
}
