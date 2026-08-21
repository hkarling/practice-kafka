package io.hkarling.learning.nonblocking;

import org.junit.jupiter.api.Test;

class OrderNonBlockingTest {

  @Test
  void placeOrderWithoutBlocking() throws InterruptedException {
    OrderConsumer consumer = new OrderConsumer();
    OrderProducer producer = new OrderProducer();

    // 컨슈머를 아직 안 띄운 상태에서 먼저 주문 몇 개를 접수 (시간적 분리 확인)
    producer.placeOrder("order-1");
    producer.placeOrder("order-2");

    // 이제 컨슈머 시작
    Thread consumerThread = new Thread(() -> {
      consumer.run();
    });
    consumerThread.setDaemon(true);
    consumerThread.start();

    producer.placeOrder("order-3");

    Thread.sleep(2000); // 컨슈머가 처리할 시간을 벌어줌 (daemon이라 테스트 끝나면 같이 죽음)
  }
}