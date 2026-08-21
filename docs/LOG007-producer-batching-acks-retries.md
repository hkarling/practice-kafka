# LOG007 — Producer 동작 원리: 배치, acks, 재시도

## 배경 / 목표

Phase 1 챕터 7. LOG006에서 관찰만 했던 현상 — "키 없는 메시지 6개가 왜 파티션 하나에
몰렸는가" — 의 진짜 메커니즘(배치)을 이해하고, 프로듀서가 "발행 성공"을 판단하는
기준(acks)과 장애 상황에서의 재시도 동작을 실제 브로커를 내렸다 올리며 확인한다.

## 개념 정리

- **배치(Batching)**: 프로듀서는 메시지를 파티션별로 메모리에 모아뒀다가(`batch.size`
  또는 `linger.ms` 조건에 걸리면) 한 번에 네트워크로 보낸다. LOG006에서 짧은 시간
  안에 보낸 메시지들이 한 파티션에 몰렸던 이유가 이것 — Sticky Partitioner가 "배치
  하나 = 파티션 하나"로 묶기 때문이다.
- **acks**: 프로듀서가 발행 성공으로 간주하는 기준. `0`(응답 안 기다림) /
  `1`(리더만 기다림) / `all`(모든 ISR을 기다림). `replication-factor=1`인 지금
  환경에서는 `1`과 `all`의 차이가 사실상 없다 (복제할 대상이 없음).
- **재시도와 멱등성**: Spring Boot는 기본으로 멱등성 프로듀서를 만든다
  (`enable.idempotence=true`, `acks=all`일 때). 재시도로 인한 중복을 브로커가
  시퀀스 번호로 감지해서 막아준다.

## 진행 과정

### 1. 토픽/프로파일/코드 준비

- `application-chapter07.yaml`에 `producer.acks: all` 추가.
- `KafkaConfig`에 `acks=0`용 `ProducerFactory`/`KafkaTemplate` 빈을 추가로 구성 —
  기본 빈과 별도로 acks만 다른 프로듀서를 비교하기 위함.

**시행착오**: `KafkaProperties.buildProducerProperties(null)`가 컴파일 에러였다.
Spring Boot 4.1.0에서 `KafkaProperties`가 `org.springframework.boot.autoconfigure.kafka`
에서 `org.springframework.boot.kafka.autoconfigure`로 패키지가 이동했고,
`buildProducerProperties()`도 파라미터 없는 메서드로 바뀌었다 (예전엔
`SslBundles` 파라미터를 받았음). import 경로와 메서드 호출 둘 다 수정해서
해결 — Chapter 6의 아티팩트 이름 착각에 이어, Spring Boot 4.x의 새 관례를 또
한 번 놓친 사례.

### 2. acks=all vs acks=0 발행 속도 비교

20건씩 `.get()`으로 완료를 기다리며 발행:
```
acks=all 총 소요시간: 427ms  (약 21.4ms/건)
acks=0   총 소요시간: 312ms  (약 15.6ms/건)
```

**해설**: `acks=0`이 더 빠르지만 차이가 크지 않다. `replication-factor=1`이라
`acks=all`도 사실상 리더 하나의 응답만 기다리면 되기 때문 — 진짜 멀티 브로커
클러스터에서 여러 replica의 응답을 기다려야 하는 상황이었다면 차이가 훨씬 컸을
것이다.

### 3. 브로커 다운 중 발행 → 재시도 → 복구 후 전달

`.get()` 대신 `.whenComplete()` 콜백으로 비동기 확인하도록 구성:
```java
kafkaTemplate.send("order-events", value)
    .whenComplete((result, ex) -> {
        if (ex != null) log.error("{} 발행 실패: {}", value, ex.getMessage());
        else log.info("{} 발행 성공: partition={}, offset={}", value,
                result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
    });
```

**절차**: ① `docker compose stop kafka` → ② 테스트 실행(3건 발행 시도) → ③ 10~15초
관찰 → ④ `docker compose start kafka` → ⑤ 60초 대기 동안 복구 확인.

**실제 결과**: 브로커가 죽어있는 동안 `Connection to node -1 could not be
established`, `Rebootstrapping with [...]` 로그가 계속 반복됐다. 브로커가
복구되자 `LEADER_NOT_AVAILABLE`(일시적 오류) 몇 번을 거쳐 클러스터 ID를
다시 확인하고, 컨슈머 그룹이 재join한 뒤, 3건 모두 최종적으로 성공 콜백이
찍혔다. 컨슈머도 이 메시지들을 정상 offset(41~43)에서 읽어갔다 — **브로커
장애 중 발행 시도한 메시지가 유실되지 않고, 복구 후 정상 전달**된다는 게
확인됐다.

**예상 밖의 발견**: `"발행 요청 3건 제출 완료"` 로그가 테스트 시작 직후가 아니라
**컨슈머 그룹이 코디네이터를 다시 찾은 직후**에야 찍혔다 — `for` 루프의
`kafkaTemplate.send()` 호출 자체가 그때까지 블로킹되어 있었다는 뜻이다. 원인은
`max.block.ms`(기본 60초): 프로듀서가 어떤 토픽에 처음 메시지를 보낼 때 그
토픽의 파티션 메타데이터를 먼저 가져와야 하는데, 브로커가 죽어있어 메타데이터를
못 가져오는 동안 `send()` 자체가 내부적으로 멈춰 있었던 것. "메타데이터가 이미
캐시된 상태에서 브로커가 죽는 경우"와 "메타데이터가 아직 없는 상태에서 브로커가
죽는 경우"가 다르게 동작한다는 걸 실측으로 확인 — 처음에 "`send()`는 브로커
상태와 무관하게 즉시 리턴된다"고 안내했던 게 부정확했다.

## 시행착오 / Q&A

**Q. `buildProducerProperties(null)`이 왜 컴파일 에러였나?**
A. Spring Boot 4.1.0에서 `KafkaProperties`의 패키지 위치와 메서드 시그니처가
바뀌었다 (위 진행 과정 1번 참고). 실제 jar를 javap로 까봐서 확인했다.

**Q. `send()`는 항상 즉시 리턴되는 거 아니었나?**
A. 아니다. 토픽 메타데이터가 캐시되어 있지 않으면 `max.block.ms`까지 내부적으로
블로킹될 수 있다. 이번 실습에서 실제로 겪었다 (위 3번 참고).

**미해결 관찰**: `acks=all`로 설정된 `kafkaTemplate`으로 보낸 `retry-test-*`
메시지들의 발행 성공 콜백에서 `offset=-1`이 찍혔다. `-1`은 보통 `acks=0`일 때만
나오는 값으로 알려져 있는데, YAML 설정도 `acks: all`이 맞았고,
`KafkaProperties.buildProducerProperties()`가 매 호출마다 새 `HashMap`을 반환하는
것도 바이트코드로 확인해서 두 프로듀서 빈 사이에 설정이 섞였을 가능성도 배제했다.
다만 컨슈머는 이 메시지들을 실제 정상 offset(41~43)에서 읽어갔으므로, 메시지
자체의 유실이나 오작동은 아니었다 — 콜백에 찍힌 메타데이터만 이상했던 것으로
보인다. 정확한 원인은 못 찾았고, 재시도 메커니즘 자체의 신뢰성 검증(핵심 목표)에는
영향이 없다고 판단해 더 파고들지 않고 넘어갔다.

## 트레이드오프 / 실무 함정 / 안티패턴

**트레이드오프**: `acks=0`은 빠르지만 유실 가능성을 감수하는 것이고, `acks=all`은
느리지만 안전하다. 로그성 이벤트(분석용)는 `acks=1`도 충분하지만, 결제/주문 같은
이벤트는 `acks=all`이 기본이어야 한다.

**실무 함정**: `retries`를 무작정 높이면 안심하기 쉬운데, `delivery.timeout.ms`
(기본 2분)를 넘기면 결국 실패로 끝난다. 재시도 설정만 믿고 애플리케이션 레벨의
실패 처리(DLQ, 알림)를 안 만들면, 브로커가 오래 다운됐을 때 조용히 메시지가
유실될 수 있다. 또한 `max.block.ms`처럼 "생각보다 블로킹될 수 있는" 설정을
모르면, 브로커 장애 시 프로듀서 스레드(특히 웹 요청 스레드라면)가 예상치 못하게
오래 멈출 수 있다.

**안티패턴**: 멱등성 프로듀서가 기본으로 켜져 있다고 "Kafka에 보내기만 하면
중복이 절대 안 생긴다"고 오해하는 것. 프로듀서의 멱등성은 같은 프로듀서 세션
안에서 재시도로 인한 중복만 막아준다 — 애플리케이션이 재시작되거나 로직에서
같은 메시지를 두 번 `send()`하면 못 막는다. 진짜 end-to-end 멱등성은 LOG017에서
다룬다.

## 더 생각해볼 것

`acks=all`이어도 여전히 "컨슈머가 그 메시지를 실제로 처리했다"는 보장은 아니다.
프로듀서의 `acks`는 "브로커가 잘 받았다"까지만 보장한다. 그럼 "컨슈머가 처리를
완료했다"는 어떻게 확인할까? — Chapter 8(Consumer 동작 원리 — 폴링, 커밋, 오프셋
관리)로 이어지는 질문.

## 최종 구성

`application-chapter07.yaml` 추가. `io.hkarling.learning.kafka` 패키지에
`KafkaConfig`(acks=0용 프로듀서 빈) 추가. 테스트 `ProducerAcksTest`
(`compareAcks`, `retryWhenBrokerRecovers`) 작성.

## ADR

### Decision
`acks` 비교용 프로듀서는 별도 `@Bean`(`acksZeroProducerFactory`/
`acksZeroKafkaTemplate`)으로 구성하고, 클래스/필드 이름에 `chapter07` 같은
챕터 번호를 넣지 않는다.

### Drivers
프로덕션 코드(코드 자체)는 챕터가 아니라 기능으로 이름 붙여야 나중에 재사용/확장하기
쉽다 — 챕터 추적성은 이미 테스트 클래스 분리(LOG003 ADR)로 충분히 확보된다.

### Alternatives
클래스명에 `Chapter07`을 접두어로 붙이는 방식 — 처음 이렇게 했다가 사용자 피드백으로
`KafkaConfig`로 변경. 코드 자체의 의미보다 "언제 만들었는지"가 이름에 드러나는 게
어색하다고 판단.

### Consequences
챕터가 늘어날수록 `KafkaConfig`, `OrderEventProducer` 같은 공유 클래스에 설정/기능이
계속 추가될 수 있다 — 필요하면 나중에 책임별로 분리한다.

### Follow-ups
Chapter 8 — Consumer 동작 원리 (폴링, 커밋, 오프셋 관리).
