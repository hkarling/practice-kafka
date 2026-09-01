package io.hkarling.ecommerce.common.event;

import java.time.Instant;

public record PaymentFailedEvent(
    String orderId,
    String reason,
    Instant occurredAt
) {

}
