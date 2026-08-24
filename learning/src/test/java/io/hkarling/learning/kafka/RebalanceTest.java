package io.hkarling.learning.kafka;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;

@Slf4j
@SpringBootTest
@ActiveProfiles("chapter09")
@DisplayName("Consumer Group — 리밸런싱")
class RebalanceTest {

  @Autowired
  KafkaTemplate<String, String> kafkaTemplate;

  @Test
  @DisplayName("안정된 그룹에 새 컨슈머가 조인하면 리밸런싱이 일어난다")
  void newConsumerJoinsAndTriggersRebalance() throws InterruptedException {
    log.info("=== 5초 대기: chapter09-group이 안정될 시간 ===");
    Thread.sleep(5000);

    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "chapter09-group"); // 같은 그룹!
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    log.info("=== raw KafkaConsumer가 chapter09-group에 조인합니다 ===");
    try (KafkaConsumer<String, String> rawConsumer = new KafkaConsumer<>(props)) {
      rawConsumer.subscribe(List.of("order-events"));
      rawConsumer.poll(Duration.ofSeconds(3)); // poll을 해야 실제로 join이 진행됨

      log.info("=== 5초 대기: 조인으로 인한 리밸런싱 로그 관찰 ===");
      Thread.sleep(5000);

      log.info("=== raw KafkaConsumer가 그룹을 떠납니다 ===");
    } // try-with-resources가 close() 호출 → 그룹에서 정상 탈퇴

    log.info("=== 5초 대기: 탈퇴로 인한 리밸런싱 로그 관찰 ===");
    Thread.sleep(5000);
  }
}