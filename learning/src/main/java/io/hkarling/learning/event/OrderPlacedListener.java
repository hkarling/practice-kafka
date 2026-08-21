package io.hkarling.learning.event;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class OrderPlacedListener {

  @Async
  @EventListener
  void handle(OrderPlacedEvent event) throws InterruptedException {
    Thread.sleep(500); // 이메일 발송 같은 걸 흉내
//    throw new RuntimeException("에러 발생");
    log.info("{} 확인 메일 발송 완료", event.orderId());
  }
}
