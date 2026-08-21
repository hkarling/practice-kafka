package io.hkarling.learning.event;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@Slf4j
@SpringBootTest
class OrderServiceTest {

  @Autowired
  OrderService orderService;

  @Test
  void orderPlaced() {
    orderService.placeOrder("order-1");
    orderService.placeOrder("order-2");
    orderService.placeOrder("order-3");
  }
}