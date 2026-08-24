package io.hkarling.learning.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.ActiveProfiles;

@Slf4j
@SpringBootTest
@ActiveProfiles("chapter09")
@DisplayName("Consumer Group — 파티션 할당")
class ConsumerGroupTest {

  @Autowired
  KafkaTemplate<String, String> kafkaTemplate;

  @Autowired
  KafkaListenerEndpointRegistry registry;

  @Test
  @DisplayName("concurrency=3인 컨슈머 그룹의 파티션 배정을 관찰한다")
  void observePartitionAssignment() throws Exception {
    MessageListenerContainer container = registry.getListenerContainer("orderEventGroupListener");

    ContainerTestUtils.waitForAssignment(container, 3); // order-events 파티션 3개가 배정될 때까지 대기(폴링), sleep 대체

    for (int i = 0; i < 9; i++) {
      kafkaTemplate.send("order-events", "group-test-" + i).get();
    }

    assertThat(container.getContainerProperties().getTopicPartitions()).hasSize(3);
  }
}
