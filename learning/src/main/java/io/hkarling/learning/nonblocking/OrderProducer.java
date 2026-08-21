package io.hkarling.learning.nonblocking;

import java.util.concurrent.BlockingQueue;

public class OrderProducer {

  private final BlockingQueue<String> queue = QueueManager.getInstance().getQueue();

  void placeOrder(String orderId) throws InterruptedException {
    long start = System.currentTimeMillis();
    queue.put(orderId); // 발행만 하고 즉시 리턴 — 컨슈머를 기다리지 않음
    System.out.println("주문 접수 완료, 소요시간: " + (System.currentTimeMillis() - start) + "ms");
  }

}
