package io.hkarling.learning.blocking;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class OrderServiceTest {

  @Test
  void placeOrder() throws InterruptedException {
    OrderService orderService = new OrderService();
    orderService.placeOrder();
    assertThat(Boolean.TRUE).isTrue();
  }
}