package io.hkarling.learning.nonblocking;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class QueueManager {

  private final BlockingQueue<String> queue;

  private QueueManager() {
    this.queue = new LinkedBlockingQueue<>(1000);
  }

  public static QueueManager getInstance() {
    return LazyHolder.INSTANCE;
  }

  public BlockingQueue<String> getQueue() {
    return queue;
  }

  private static class LazyHolder {

    private static final QueueManager INSTANCE = new QueueManager();
  }

}
