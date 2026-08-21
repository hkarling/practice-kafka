package io.hkarling.learning.relay;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ReplayConsumerTest {

  @Test
  @DisplayName("ReplayConsumer 테스트")
  void testReplayConsumer() throws InterruptedException {
    EventLog eventLog = new EventLog();
    eventLog.append("order-1");
    eventLog.append("order-2");
    eventLog.append("order-3"); // 컨슈머 없이 먼저 3건 다 쌓아둠

    startDaemon(new ReplayConsumer(eventLog, "consumer-1"));
    Thread.sleep(3000);
    startDaemon(new ReplayConsumer(eventLog, "consumer-2")); // 훨씬 늦게 시작
    Thread.sleep(3000);
  }

  private Thread startDaemon(Runnable consumer) {
    Thread thread = new Thread(consumer);
    thread.setDaemon(true);
    thread.start();
    return thread;
  }
}