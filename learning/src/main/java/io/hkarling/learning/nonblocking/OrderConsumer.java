package io.hkarling.learning.nonblocking;

import static java.lang.Thread.sleep;

import java.util.concurrent.BlockingQueue;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OrderConsumer implements Runnable {

  private final BlockingQueue<String> queue;

  public OrderConsumer() {
    this.queue = QueueManager.getInstance().getQueue(); // 기존 P2P용
  }

  public OrderConsumer(BlockingQueue<String> queue) {
    this.queue = queue; // Pub/Sub처럼 특정 큐를 직접 지정
  }

  public void run() {
    while (true) {
      String orderId = null; // 블로킹은 여기서만 일어남 (컨슈머 자기 스레드)
      try {
        orderId = queue.take();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      try {
        sleep(200); // 재고 확인
        sleep(300); // 결제 처리
        // 여기서 일부러 실패시켜보세요: throw new RuntimeException("PG 타임아웃");
        log.info("{} 처리 완료", orderId);
      } catch (Exception e) {
        log.warn("{} 처리 실패: {}", orderId, e.getMessage());
        // 프로듀서는 이미 리턴한 뒤라 이 예외를 전혀 모른다
      }
    }
  }
}
