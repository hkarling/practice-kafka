package io.hkarling.ecommerce.common.event;

import java.time.Instant;

public record OrderCreatedEvent(
    String orderId,
    String productId,
    int quantity,
    Instant occurredAt
) {

}
