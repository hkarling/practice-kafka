package io.hkarling.learning.nonblocking;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;

public class PublishManager {

  private final List<BlockingQueue<String>> subscriberQueues = new CopyOnWriteArrayList<>();

  BlockingQueue<String> subscribe() {
    BlockingQueue<String> queue = new LinkedBlockingQueue<>();
    subscriberQueues.add(queue);
    return queue;
  }

  void publish(String message) {
    subscriberQueues.forEach(q -> q.offer(message)); // 구독자 전부에게 각각 전달
  }
}
