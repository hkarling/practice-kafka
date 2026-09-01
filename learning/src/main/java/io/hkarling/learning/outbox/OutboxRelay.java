package io.hkarling.learning.outbox;

import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@RequiredArgsConstructor
@Component
@Profile({"chapter18", "chapter20"})
public class OutboxRelay {

  private final OutboxEventRepository outboxEventRepository;
  private final KafkaTemplate<String, String> kafkaTemplate;

  @Scheduled(fixedDelay = 1000)
  public void relay() {
    List<OutboxEventRow> pending = outboxEventRepository.findPending(50);
    for (OutboxEventRow event : pending) {
      kafkaTemplate.send(event.topic(), event.aggregateId(), event.payload());
      outboxEventRepository.markPublished(event.id());
      log.info("outbox 이벤트 전송 완료: id={}, topic={}, payload={}", event.id(), event.topic(), event.payload());
    }
  }

}
