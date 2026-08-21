package io.hkarling.learning.nonblocking;

import static java.lang.Thread.sleep;

import java.util.concurrent.BlockingQueue;

public class OrderConsumer implements Runnable {

  private final BlockingQueue<String> queue = QueueManager.getInstance().getQueue();

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
        System.out.println(orderId + " 처리 완료");
      } catch (Exception e) {
        System.out.println(orderId + " 처리 실패: " + e.getMessage());
        // 프로듀서는 이미 리턴한 뒤라 이 예외를 전혀 모른다
      }
    }
  }
}
