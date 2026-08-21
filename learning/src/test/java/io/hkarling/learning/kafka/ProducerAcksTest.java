package io.hkarling.learning.kafka;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;

@Slf4j
@SpringBootTest
@ActiveProfiles("chapter07")
@DisplayName("Producer 동작 원리 — acks")
class ProducerAcksTest {

  @Autowired
  KafkaTemplate<String, String> kafkaTemplate; // 기본 설정 (acks=all, yaml에서 지정)

  @Autowired
  KafkaTemplate<String, String> acksZeroKafkaTemplate;

  @Test
  @DisplayName("acks=all과 acks=0의 발행 소요시간을 비교한다")
  void compareAcks() throws Exception {
    long start1 = System.currentTimeMillis();
    for (int i = 0; i < 20; i++) {
      kafkaTemplate.send("order-events", "acks-all-" + i).get();
    }
    log.info("acks=all 총 소요시간: {}ms", System.currentTimeMillis() - start1);

    long start2 = System.currentTimeMillis();
    for (int i = 0; i < 20; i++) {
      acksZeroKafkaTemplate.send("order-events", "acks-zero-" + i).get();
    }
    log.info("acks=0 총 소요시간: {}ms", System.currentTimeMillis() - start2);
  }

  @Test
  @DisplayName("브로커가 다운된 동안 발행하면 재시도 후 복구 시 성공한다")
  void retryWhenBrokerRecovers() throws InterruptedException {
    for (int i = 0; i < 3; i++) {
      String value = "retry-test-" + i;
      kafkaTemplate.send("order-events", value)
          .whenComplete((result, ex) -> {
            if (ex != null) {
              log.error("{} 발행 실패: {}", value, ex.getMessage());
            } else {
              log.info("{} 발행 성공: partition={}, offset={}",
                  value, result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
            }
          });
    }
    log.info("발행 요청 3건 제출 완료 (즉시 리턴, 브로커 상태와 무관)");

    Thread.sleep(60000); // 이 시간 안에 브로커를 복구시켜야 함
  }

}
