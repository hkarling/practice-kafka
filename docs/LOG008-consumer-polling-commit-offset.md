# LOG008 — Consumer 동작 원리: 폴링, 커밋, 오프셋 관리

## 배경 / 목표

Phase 1 챕터 8. LOG007의 마지막 질문 — "`acks=all`은 브로커가 잘 받았다는 것까지만
보장한다. 그럼 컨슈머가 실제로 처리를 완료했다는 건 어떻게 확인하는가?" — 를 이어서,
Consumer가 메시지를 가져오는 방식(폴링)과 "여기까지 처리했다"를 기록하는 방식(오프셋
커밋)을 실제 재시도/장애 상황을 만들어가며 확인한다.

## 개념 정리

- **폴링(Poll loop)**: Kafka Consumer는 브로커가 push하는 게 아니라 `poll()`을 반복
  호출해서 pull하는 구조. `@KafkaListener` 뒤의 `KafkaMessageListenerContainer`가
  내부적으로 이 루프를 돌린다.
- **오프셋 커밋**: 컨슈머 그룹이 파티션별로 "여기까지 처리했다"고 기록하는 것. 브로커의
  `__consumer_offsets` 내부 토픽에 저장되며, **파티션마다 독립된 정수 하나**로만
  관리된다 — 레코드 단위로 성공/실패를 따로 기록하지 않는다.
- **auto-commit vs manual commit**: auto-commit(`enable.auto.commit=true`, 기본값)은
  `poll()` 시점마다 주기적으로 백그라운드 커밋되는데, "처리 완료"보다 커밋이 먼저
  일어날 수 있어 유실 가능성이 있다. manual commit은 애플리케이션이 커밋 시점을
  직접 결정한다.
- **Spring Kafka AckMode**: `RECORD`(레코드마다 자동 커밋) / `BATCH`(기본값, 배치
  끝나면 자동 커밋) / `MANUAL`(다음 poll 시점에 커밋) / `MANUAL_IMMEDIATE`
  (`acknowledge()` 호출 즉시 동기 커밋).
- **오프셋은 (그룹, 파티션) 단위로 독립적**: 같은 토픽이어도 컨슈머 그룹이 다르면
  읽기 위치가 완전히 별개다. 새 그룹은 커밋 기록이 없으므로 `auto-offset-reset`
  설정(여기서는 `earliest`)에 따라 처음부터 다시 읽는다 — 다른 그룹이 이미 다
  읽었는지와 무관하다.

## 진행 과정

### 1. 코드 구성

기존 `OrderEventConsumer`(Chapter 6~7용, BATCH ack)는 그대로 두고, `KafkaConfig`에
`manualAckKafkaListenerContainerFactory` 빈(AckMode=`MANUAL_IMMEDIATE`)을 추가하고,
`OrderEventManualAckConsumer`를 별도 그룹(`chapter08-manual-group`)으로 붙여서 두
커밋 전략을 같은 애플리케이션 안에서 비교했다. `fail`로 시작하는 값은 예외를 던져
재시도를 유도했다.

### 2. 첫 번째 삽질 — `@KafkaListener` 속성값 오류

`OrderEventManualAckConsumer`를 처음 작성했을 때 `chapter08-manual-group` 컨슈머
그룹이 브로커에 아예 생기지 않았다. 로그를 보니 `groupId`가 yaml 기본값
(`chapter08-group`)으로 fallback되어 있었고, 구독 토픽도 `order-events`가 아니라
문자열 `"chapter08-manual-group"`으로 되어 있었다 — `topics`/`groupId` 값이 서로
바뀐 것. `@KafkaListener(topics = KafkaTopics.ORDER_EVENTS, groupId =
"chapter08-manual-group", ...)`로 고치자 정상 동작했다.

### 3. 새 컨슈머 그룹의 전체 재생 관찰

`chapter08-manual-group`은 커밋 기록이 없는 새 그룹이라, `auto-offset-reset:
earliest` 설정에 따라 `order-events` 토픽 3개 파티션 전체를 처음부터 읽었다 —
Chapter 6~7에서 쌓인 과거 메시지(`order-1`, `결제 시작`, `acks-all-0` 등)까지 전부
다시 소비했다. `chapter08-group`이 이미 그 메시지들을 읽고 커밋해뒀다는 사실은
`chapter08-manual-group` 입장에서 전혀 상관없었다 — 오프셋이 그룹별로 독립이기
때문.

### 4. 재시도 → 포기 → 이어지는 레코드 커밋 흐름 (정상 케이스)

`fail-1` 레코드를 만나면:
```
WARN 처리 실패 시뮬레이션 → Seeking to offset N (같은 레코드로 복귀) → Record in retry and not yet recovered
```
가 9번 반복되고(`FixedBackOffExecution[interval=0, maxAttempts=9]`, 약 5초 소요),
`Backoff ... exhausted` 후 `Seeking to offset N+1`로 다음 레코드로 넘어갔다. 그
다음 레코드가 정상 처리되어 `acknowledge()`가 호출되면, 커밋된 오프셋이 그 레코드
위치까지 올라가면서 **실패했던 레코드도 함께 "지나간 것"으로 커밋**됐다 — 오프셋이
레코드 단위가 아니라 파티션당 정수 하나이기 때문이다.

### 5. sleep 시간 부족으로 중간에 끊긴 케이스

테스트의 `Thread.sleep(15000)`이 만료되면서 Spring 컨텍스트가 종료됐는데, 하필
세 번째로 만난 `fail-1`이 9번 재시도를 다 채우기 전(2~3번째 시도)에 컨테이너가
멈췄다. 그 결과 커밋된 오프셋이 그 미완료 `fail-1` 자리에 그대로 남았다
(`kafka-consumer-groups --describe` 결과 `LAG=2`).

**처음 세운 가설(틀림)**: "MANUAL/MANUAL_IMMEDIATE 모드에서는 에러 복구 후에도
`acknowledge()`가 안 불려서 오프셋이 원래 멈춰있는 구조다." 실제로는 4번에서
확인했듯 복구 자체는 정상 동작한다 — 이번 케이스는 순수히 "재시도 사이클(9회 ×
약 0.55초)이 끝나기 전에 테스트가 먼저 끝났다"는 시간 문제였다. 로그로 재검증해서
가설을 정정했다.

### 6. 재기동 시 미완료 레코드부터 재개

같은 테스트를 다시 실행하자, `chapter08-manual-group`은 지난번 커밋 위치(offset
98)부터 재개했다 — 그 자리가 바로 지난번 미완료였던 `fail-1`이라, 재시작하자마자
`처리 완료` 로그 하나 없이 바로 `WARN 처리 실패 시뮬레이션`부터 찍혔다. 이번엔
9회 재시도가 시간 안에 끝나서 정상적으로 다음 레코드(leftover `manual-ok-2`, 이어서
이번 실행이 새로 보낸 3건)까지 전부 처리·커밋됐다. 최종적으로
`kafka-consumer-groups --describe`에서 `LAG=0` 확인.

### 7. 완전히 캐치업된 상태에서 재실행

두 그룹 다 최신 오프셋까지 커밋된 상태에서 같은 테스트를 한 번 더 실행하자,
이번엔 새로 보낸 3건(`manual-ok-1`/`fail-1`/`manual-ok-2`, offset 103~105)만
처리됐다 — 더 이상 과거 백로그를 다시 훑지 않았다. `fail-1` 하나가 재시도 때문에
로그 여러 줄(WARN 9줄 + ERROR 1줄)을 남기지만, **처리 대상 자체는 3개 메시지뿐**
이라는 걸 확인했다.

## 시행착오 / Q&A

**Q. `chapter08-manual-group`이 브로커에 안 보인다.**
A. `@KafkaListener`의 `topics`/`groupId` 값이 서로 바뀌어 있었다 (위 진행 과정
2번). 로그의 `Assignment(partitions=[...])`에 찍힌 파티션 이름으로 실제 구독
토픽이 뭔지 확인해서 잡았다.

**Q. 재시도 소진 후에도 오프셋이 안 넘어가는 게 MANUAL 모드의 결함 아닌가?**
A. 아니다. 실제로는 복구가 정상 동작했고, 막혔던 건 테스트 sleep 시간이 재시도
사이클을 다 못 기다렸기 때문이었다 (위 진행 과정 5번). 처음엔 결함으로 추측했다가
로그로 재검증해서 정정했다.

**Q. 스킵(포기)된 레코드는 컨슈머가 재기동될 때마다 다시 읽히나?**
A. 경우에 따라 다르다. 뒤이어 들어오는 레코드가 정상 처리되어 커밋되면, 그 커밋에
실패 레코드도 함께 묻혀 넘어가므로 다시 안 읽는다. 하지만 실패한 레코드가 그
순간 파티션의 마지막 레코드였다면 뒤에 커밋을 유발할 레코드가 없으므로, 재기동
시 커밋된 오프셋이 여전히 그 자리를 가리켜 **똑같이 재시도 9번 → 포기를 반복**
하게 된다.

**Q. 파티션별로 오프셋 숫자가 왜 이렇게 다른가(0번은 100대, 1/2번은 한 자리~10대)?**
A. 오프셋은 파티션마다 독립된 카운터라 원래 다르다. 여기선 키 없이 발행해서
Sticky Partitioner가 짧은 시간에 몰아 보낸 메시지들을 한 파티션에 묶은 영향도
크다 — 파티션 키 설계는 Chapter 10에서 다룬다.

## 트레이드오프 / 실무 함정 / 안티패턴

**트레이드오프**: auto-commit은 구현이 간단하지만 "먼저 커밋, 나중에 처리"가 될
수 있어 유실 가능성이 있다. `MANUAL_IMMEDIATE`는 유실을 막지만 레코드마다 동기
커밋이라 처리량이 떨어진다. `BATCH`는 개별 제어는 안 되지만 커밋 횟수가 적어
효율적이다.

**실무 함정**: 이번 실습에서 직접 겪은 함정 — **오프셋 커밋은 레코드 단위가
아니라 파티션당 정수 하나**라서, "실패한 레코드를 건너뛰었다"는 사실 자체는
그 뒤에 오는 레코드가 커밋될 때 슬쩍 묻혀 넘어간다. 만약 실패한 레코드가 파티션의
마지막 레코드라면(뒤에 새 메시지가 한동안 안 들어오는 상황), 커밋이 그 자리에서
멈춘 채로 남아 컨슈머가 재기동될 때마다 같은 레코드를 계속 재시도하게 된다. 이건
버그가 아니라 설계상 당연한 동작이지만, 모르면 "왜 자꾸 같은 메시지가 반복
처리되지?"로 헤매기 쉽다.

또한 새 컨슈머 그룹 + `auto-offset-reset: earliest` 조합은 공유된 장기 토픽에서
예상보다 훨씬 많은 과거 데이터를 재생시킬 수 있다 — 이번 실습에서 "3개만 보냈는데
왜 이렇게 많이 처리되지?"로 실제로 헷갈렸던 부분이다.

**안티패턴**: 모든 리스너에 무조건 `MANUAL_IMMEDIATE`를 적용하는 것. 유실을 못
견디는 정도가 낮은 컨슈머(단순 로깅 등)까지 매 레코드 동기 커밋을 강제하면
불필요한 성능 손실이다. 커밋 전략은 컨슈머별 요구사항에 맞춰 골라야 한다.

## 더 생각해볼 것

지금은 컨슈머가 그룹당 하나뿐이라 파티션 3개를 혼자 다 가져간다. 컨슈머 인스턴스가
여러 개면 파티션이 어떻게 나뉘고, 그 중 하나가 죽으면 무슨 일이 일어날까? →
Chapter 9(Consumer Group, 리밸런싱)로 이어지는 질문.

## 최종 구성

`application-chapter08.yaml` 추가 (`enable-auto-commit: false`). `KafkaConfig`에
`manualAckKafkaListenerContainerFactory` 빈(AckMode=`MANUAL_IMMEDIATE`) 추가.
`OrderEventManualAckConsumer` 신규 (그룹 `chapter08-manual-group`, `fail` 접두
값에서 예외 발생). 테스트 `ManualAckConsumerTest`(`manualAckAndRetry`) 작성.

## ADR

### Decision
기존 `OrderEventConsumer`(BATCH ack, Chapter 6~7용)는 건드리지 않고, MANUAL_IMMEDIATE
용 컨테이너 팩토리·컨슈머·컨슈머 그룹을 전부 별도로 구성해서 나란히 비교했다.

### Drivers
`spring.kafka.listener.ack-mode`를 전역으로 바꾸면 같은 프로파일을 쓰는 다른
리스너(`OrderEventConsumer`)에도 영향을 준다 — 그 리스너는 `Acknowledgment`를
안 받으므로 MANUAL 모드에서 영원히 커밋이 안 되는 상황이 생긴다. 컨테이너 팩토리
빈 단위로 ack 모드를 분리하면 이런 간섭을 피할 수 있다.

### Alternatives
`listener.ack-mode`를 yaml에 전역으로 지정하는 방식 — 검토했지만 위 이유로 기각.

### Consequences
같은 토픽에 그룹이 여러 개 붙게 되면서, 그룹별 오프셋 격리·새 그룹의 전체 재생
현상을 실습 중 실제로 마주쳐 예상보다 디버깅 시간이 늘었다. 다만 그 과정 자체가
이 챕터의 핵심 개념(오프셋은 그룹×파티션 단위)을 몸으로 확인하는 좋은 계기가 됐다.

### Follow-ups
Chapter 9 — Consumer Group (파티션 할당, 리밸런싱).
