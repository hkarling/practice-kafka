package io.hkarling.learning.streams;

import io.hkarling.learning.kafka.KafkaTopics;
import java.time.Duration;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class OrderEventCountTopology {

  public static Topology build() {
    StreamsBuilder builder = new StreamsBuilder();
    KStream<Object, Object> events = builder.stream(KafkaTopics.ORDER_EVENTS_STREAMS);

    KTable<Windowed<Object>, Long> counts = events
        .groupBy((orderId, eventType) -> eventType)
        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(10)))
        .count();

    counts.toStream()
        .map((windowedKey, count) -> KeyValue.pair(
            windowedKey.key() + "@" + windowedKey.window().start(),
            String.valueOf(count)))
        .to(KafkaTopics.ORDER_EVENT_COUNTS, Produced.with(Serdes.String(), Serdes.String()));

    return builder.build();
  }

}
