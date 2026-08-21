package io.hkarling.learning.blocking;

public class OrderService {

  private final InventoryService inventoryService = new InventoryService();

  void placeOrder() throws InterruptedException {
    long start = System.currentTimeMillis();
    inventoryService.reserve();
    System.out.println("총 소요시간: " + (System.currentTimeMillis() - start) + "ms");
  }

}
