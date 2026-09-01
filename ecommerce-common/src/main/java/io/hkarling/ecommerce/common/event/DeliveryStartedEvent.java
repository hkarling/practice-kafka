package io.hkarling.ecommerce.common.event;

import java.time.Instant;

public record DeliveryStartedEvent(
    String orderId,
    Instant occurredAt
) {

}
