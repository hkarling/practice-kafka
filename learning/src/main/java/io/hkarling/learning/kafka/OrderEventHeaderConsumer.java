package io.hkarling.learning.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@Profile("chapter11")
public class OrderEventHeaderConsumer {

  @KafkaListener(
      id = "order-event-header-listener",
      topics = KafkaTopics.ORDER_EVENTS,
      groupId = "chapter11-header-group")
  public void listen(
      @Payload String value,
      @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
      @Header(KafkaHeaders.OFFSET) long offset,
      @Header(value = "source", required = false) String source
  ) {
    log.info("partition={}, offset={}, source={}, value={}", partition, offset, source, value);
  }

}
