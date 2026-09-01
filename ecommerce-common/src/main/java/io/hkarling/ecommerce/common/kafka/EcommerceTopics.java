package io.hkarling.ecommerce.common.kafka;

import lombok.experimental.UtilityClass;

@UtilityClass
public final class EcommerceTopics {

  public static final String ORDER_CREATED = "order.created";
  public static final String INVENTORY_RESERVED = "inventory.reserved";
  public static final String INVENTORY_RESERVATION_FAILED = "inventory.reservation-failed";
  public static final String PAYMENT_COMPLETED = "payment.completed";
  public static final String PAYMENT_FAILED = "payment.failed";
  public static final String DELIVERY_STARTED = "delivery.started";

}
