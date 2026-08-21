package io.hkarling.learning.relay;

import static java.lang.Thread.sleep;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ReplayConsumer implements Runnable {

  private final EventLog eventLog;
  private final String name;
  private int offset = 0;

  public ReplayConsumer(EventLog eventLog, String name) {
    this.eventLog = eventLog;
    this.name = name;
  }

  @Override
  public void run() {
    while (true) {
      if (offset < eventLog.size()) {
        String event = eventLog.read(offset);
        log.info(name + " -> offset " + offset + ": " + event + " 처리");
        offset++;
      } else {
        try {
          sleep(100); // 폴링 (Kafka Consumer의 poll()과 개념적으로 동일)
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }
}
