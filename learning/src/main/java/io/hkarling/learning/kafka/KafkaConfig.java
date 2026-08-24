package io.hkarling.learning.kafka;

import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

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

  @Bean
  public ProducerFactory<String, OrderEvent> jsonProducerFactory(KafkaProperties properties) {
    Map<String, Object> props = properties.buildProducerProperties();
    return new DefaultKafkaProducerFactory<>(props, new StringSerializer(), new JacksonJsonSerializer<>());
  }


  @Bean
  public KafkaTemplate<String, OrderEvent> jsonKafkaTemplate(ProducerFactory<String, OrderEvent> jsonProducerFactory) {
    return new KafkaTemplate<>(jsonProducerFactory);
  }

  @Bean
  public ConsumerFactory<String, String> stringConsumerFactory(KafkaProperties properties) {
    Map<String, Object> props = properties.buildConsumerProperties();
    return new DefaultKafkaConsumerFactory<>(props);
  }

  @Bean
  public ConsumerFactory<String, OrderEvent> jsonConsumerFactory(KafkaProperties properties) {
    Map<String, Object> props = properties.buildConsumerProperties();
    JacksonJsonDeserializer<OrderEvent> jacksonDeserializer = new JacksonJsonDeserializer<>(OrderEvent.class);
    jacksonDeserializer.addTrustedPackages("io.hkarling.learning.kafka");
    return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(),
        new ErrorHandlingDeserializer<>(jacksonDeserializer));
  }

  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, OrderEvent> jsonKafkaListenerContainerFactory(
      ConsumerFactory<String, OrderEvent> jsonConsumerFactory) {
    ConcurrentKafkaListenerContainerFactory<String, OrderEvent> factory = new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(jsonConsumerFactory);
    return factory;
  }

  @Bean
  public ProducerFactory<String, String> stringProducerFactory(KafkaProperties properties) {
    Map<String, Object> props = properties.buildProducerProperties();
    return new DefaultKafkaProducerFactory<>(props);
  }

  @Bean
  public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> stringProducerFactory) {
    return new KafkaTemplate<>(stringProducerFactory);
  }

}
