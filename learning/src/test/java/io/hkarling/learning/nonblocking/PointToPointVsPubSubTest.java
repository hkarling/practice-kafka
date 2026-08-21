package io.hkarling.learning.nonblocking;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class PointToPointVsPubSubTest {

  @Test
  @DisplayName("P2P: 컨슈머끼리 메시지를 나눠 가져간다")
  void pointToPoint_MessagesAreDistributedAmongConsumers() throws InterruptedException {
    // 컨슈머 2개가 QueueManager의 공유 큐를 바라봄 (기본 생성자)
    startDaemon(new OrderConsumer());
    startDaemon(new OrderConsumer());

    OrderProducer producer = new OrderProducer();
    producer.placeOrder("order-1");
    producer.placeOrder("order-2");
    producer.placeOrder("order-3");

    Thread.sleep(2000);
    // 기대: order-1/2/3이 두 컨슈머(스레드)에게 나뉘어 처리됨 — 총 3건이 딱 한 번씩만 처리
  }

  @Test
  @DisplayName("Pub/Sub: 구독자 전원이 메시지를 전부 받는다")
  void pubSub_AllSubscribersReceiveEveryMessage() throws InterruptedException {
    PublishManager publishManager = new PublishManager();

    // 구독자 2개가 각자 자기 큐를 가짐
    startDaemon(new OrderConsumer(publishManager.subscribe()));
    startDaemon(new OrderConsumer(publishManager.subscribe()));

    publishManager.publish("order-1");
    publishManager.publish("order-2");
    publishManager.publish("order-3");

    Thread.sleep(2000);
    // 기대: order-1/2/3이 "구독자당 한 번씩, 총 두 번씩(구독자 2개분)" 처리됨
  }

  private Thread startDaemon(OrderConsumer consumer) {
    Thread t = new Thread(() -> {
      consumer.run();
    });
    t.setDaemon(true);
    t.start();
    return t;
  }

}
