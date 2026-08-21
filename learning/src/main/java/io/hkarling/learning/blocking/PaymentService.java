package io.hkarling.learning.blocking;

import static java.lang.Thread.sleep;

public class PaymentService {

  public boolean charge() throws InterruptedException {
    sleep(300); // 네트워크 호출 시뮬레이션
    // 여기서 일부러 실패시켜보세요: throw new RuntimeException("PG 타임아웃");
    return true;
  }

}
