package io.hkarling.ecommerce.common.event;

import java.time.Instant;

public record InventoryReservationFailedEvent(
    String orderId,
    String productId,
    int quantity,
    String reason,
    Instant occurredAt
) {

}
