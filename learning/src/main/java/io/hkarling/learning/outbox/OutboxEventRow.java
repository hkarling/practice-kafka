package io.hkarling.learning.outbox;

public record OutboxEventRow(
    Long id,
    String topic,
    String aggregateId,
    String payload
) {

}
