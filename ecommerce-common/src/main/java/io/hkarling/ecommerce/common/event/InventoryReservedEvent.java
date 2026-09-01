package io.hkarling.ecommerce.common.event;

import java.time.Instant;

public record InventoryReservedEvent(
    String orderId,
    String productId,
    int quantity,
    Instant occurredAt
) {

}
