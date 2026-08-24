package io.hkarling.learning.kafka;

import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;

@Configuration
public class KafkaConfig {

  @Bean
  public ProducerFactory<String, String> acksZeroProducerFactory(KafkaProperties properties) {
    Map<String, Object> props = properties.buildProducerProperties();
    props.put(ProducerConfig.ACKS_CONFIG, "0");
    return new DefaultKafkaProducerFactory<>(props);
  }

  @Bean
  public KafkaTemplate<String, String> acksZeroKafkaTemplate(
      ProducerFactory<String, String> acksZeroProducerFactory) {
    return new KafkaTemplate<>(acksZeroProducerFactory);
  }

  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, String> manualAckKafkaListenerContainerFactory(
      ConsumerFactory<String, String> consumerFactory) {
    ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumerFactory);
    factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
    return factory;
  }

  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, String> batchKafkaListenerContainerFactory(
      ConsumerFactory<String, String> consumerFactory) {
    ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumerFactory);
    factory.setBatchListener(true);
    return factory;
  }

}
