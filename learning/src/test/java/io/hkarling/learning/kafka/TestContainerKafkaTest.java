package io.hkarling.learning.kafka;


import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;

@Slf4j
@SpringBootTest
@Testcontainers
@ActiveProfiles("chapter14")
@DisplayName("Testcontainers로 격리된 Kafka 통합 테스트")
class TestContainerKafkaTest {

  @Container
  static ConfluentKafkaContainer kafkaContainer = new ConfluentKafkaContainer("confluentinc/cp-kafka:7.6.1");

  @Autowired
  KafkaTemplate<String, String> kafkaTemplate;

  @DynamicPropertySource
  static void kafkaProperties(DynamicPropertyRegistry registry) {
    registry.add("spring.kafka.bootstrap-servers", kafkaContainer::getBootstrapServers);
  }

  @Test
  @DisplayName("컨테이너로 띄운 Kafka에 메시지를 보내고 받을 수 있다")
  void sendAndReceive() throws Exception {
    kafkaTemplate.send("testcontainers-topic", "key", "hello").get();

    Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(
        kafkaContainer.getBootstrapServers(),
        "testcontainers-verify-group",
        true);
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

    try (Consumer<String, String> consumer =
        new DefaultKafkaConsumerFactory<String, String>(consumerProps).createConsumer()) {
      consumer.subscribe(List.of("testcontainers-topic"));
      ConsumerRecord<String, String> record = KafkaTestUtils.getSingleRecord(consumer, "testcontainers-topic");

      assertThat(record.key()).isEqualTo("key");
      assertThat(record.value()).isEqualTo("hello");
    }
  }

}