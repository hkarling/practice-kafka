package io.hkarling.learning.kafka;

import java.time.Instant;

public record OrderEvent(
    String orderId,
    String eventType,
    Instant occurredAt
) {

}
