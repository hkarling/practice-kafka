package io.hkarling.learning.kafka;

import org.springframework.kafka.retrytopic.RetryTopicConstants;

public final class KafkaTopics {

  public static final String ORDER_EVENTS = "order-events";
  public static final String ORDER_EVENTS_JSON = "order-events-json";
  public static final String ORDER_EVENTS_DLT = ORDER_EVENTS + RetryTopicConstants.DEFAULT_DLT_SUFFIX;
  public static final String ORDER_EVENTS_SEMANTICS = "order-events-semantics";
  public static final String ORDER_EVENTS_IDEMPOTENT = "order-events-idempotent";

  private KafkaTopics() {
  }

}
