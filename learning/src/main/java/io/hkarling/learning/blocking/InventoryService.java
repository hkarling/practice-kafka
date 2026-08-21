package io.hkarling.learning.blocking;

import static java.lang.Thread.sleep;

public class InventoryService {

  private final PaymentService paymentService = new PaymentService();

  boolean reserve() throws InterruptedException {
    sleep(200);
    return paymentService.charge(); // 동기 호출 = 블로킹 대기
  }

}
