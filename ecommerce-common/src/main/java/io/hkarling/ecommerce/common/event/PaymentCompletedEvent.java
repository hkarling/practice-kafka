package io.hkarling.ecommerce.common.event;

import java.time.Instant;

public record PaymentCompletedEvent(
    String orderId,
    long amount,
    Instant occurredAt
) {

}
