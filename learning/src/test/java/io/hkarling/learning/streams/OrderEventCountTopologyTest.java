package io.hkarling.learning.streams;

import static org.assertj.core.api.Assertions.assertThat;

import io.hkarling.learning.kafka.KafkaTopics;
import java.time.Instant;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Kafka Streams — 이벤트 타입별 10초 윈도우 카운트")
class OrderEventCountTopologyTest {

  private TopologyTestDriver testDriver;
  private TestInputTopic<String, String> inputTopic;
  private TestOutputTopic<String, String> outputTopic;

  @BeforeEach
  void setUp() {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-order-event-count");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

    testDriver = new TopologyTestDriver(OrderEventCountTopology.build(), props);
    inputTopic = testDriver.createInputTopic(
        KafkaTopics.ORDER_EVENTS_STREAMS, new StringSerializer(), new StringSerializer());
    outputTopic = testDriver.createOutputTopic(
        KafkaTopics.ORDER_EVENT_COUNTS, new StringDeserializer(), new StringDeserializer());
  }

  @AfterEach
  void tearDown() {
    testDriver.close();
  }

  @Test
  @DisplayName("같은 윈도우 안의 이벤트는 누적 집계된다")
  void countsAccumulateWithinSameWindow() {
    Instant base = Instant.parse("2026-01-01T00:00:00Z");

    inputTopic.pipeInput("order-1", "ORDER_CREATED", base);
    inputTopic.pipeInput("order-2", "ORDER_CREATED", base.plusSeconds(2));
    inputTopic.pipeInput("order-3", "ORDER_CANCELLED", base.plusSeconds(3));

    String lastCreatedCount = outputTopic.readKeyValuesToList().stream()
        .filter(kv -> kv.key.equals("ORDER_CREATED@" + base.toEpochMilli()))
        .reduce((first, second) -> second)
        .map(kv -> kv.value)
        .orElseThrow();

    assertThat(lastCreatedCount).isEqualTo("2");
  }

  @Test
  @DisplayName("윈도우 경계를 넘어가면 새 윈도우로 별도 집계된다")
  void differentWindowsAreCountedSeparately() {
    Instant base = Instant.parse("2026-01-01T00:00:00Z");

    inputTopic.pipeInput("order-1", "ORDER_CREATED", base);
    inputTopic.pipeInput("order-2", "ORDER_CREATED", base.plusSeconds(15)); // 다음 10초 윈도우

    long distinctWindowStarts = outputTopic.readKeyValuesToList().stream()
        .map(kv -> kv.key)
        .filter(key -> key.startsWith("ORDER_CREATED@"))
        .distinct()
        .count();

    assertThat(distinctWindowStarts).isEqualTo(2);
  }
}