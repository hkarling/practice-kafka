# LOG004 — 메시지 큐 vs 이벤트 스트림

## 배경 / 목표

Phase 0 챕터 4. LOG003 마지막에 남긴 질문 — "뒤늦게 구독하면 과거 메시지를 받을 수
있는가" — 에 답하면서, 전통적 메시지 큐(RabbitMQ류)와 Kafka(이벤트 스트림)의 구조적
차이를 코드로 재현해서 확인한다.

## 개념 정리

- **전통적 메시지 큐 (RabbitMQ류) — "소비하면 사라진다"**: 메시지는 컨슈머가 ack하는
  순간 큐에서 삭제된다. 브로커가 "누가 가져갔는지, 처리했는지"를 추적하는 책임을 진다.
  LOG002~003에서 만든 `BlockingQueue`/`PublishManager`가 이 방식 — 구독 이전 메시지는
  못 받는다.
- **이벤트 스트림 (Kafka) — "기록은 남고, 읽은 위치만 이동한다"**: 메시지는 토픽 로그에
  그대로 보관되고(retention 기간 동안), 컨슈머는 메시지를 지우지 않고 자신의 **오프셋**만
  이동시킨다. 브로커는 "누가 읽었는지" 신경 쓰지 않고 로그만 순서대로 저장 — "어디까지
  읽었나"는 컨슈머 책임이다. 그래서 뒤늦게 시작한 컨슈머도 오프셋을 0부터 시작해서
  과거 메시지를 전부 재생(replay)할 수 있다.
- **Push vs Pull**: RabbitMQ는 브로커가 컨슈머에게 메시지를 밀어준다(push). Kafka는
  컨슈머가 브로커에게 주기적으로 가지러 간다(pull, polling) — 컨슈머가 자기 처리 속도에
  맞춰 가져오는 양을 스스로 조절할 수 있다.
- **철학의 차이**: 큐는 "작업을 안전하게 한 번 처리하기"에, 스트림은 "일어난 사실의
  이력을 영속적으로 남기고 여러 소비자가 각자 재해석하기"에 최적화되어 있다. 이 차이가
  나중에 Outbox 패턴(LOG018), CQRS(LOG020)의 근거가 된다.

## 진행 과정

### 1. `EventLog` — 삭제되지 않는 append-only 로그

```java
public class EventLog {
    private final List<String> log = new CopyOnWriteArrayList<>();

    void append(String event) {
        log.add(event);
    }

    String read(int offset) {
        return offset < log.size() ? log.get(offset) : null;
    }

    int size() {
        return log.size();
    }
}
```

**해설**: 큐와 달리 `read()`는 항목을 꺼내서 없애는 게 아니라 인덱스로 조회만 한다 —
같은 오프셋을 몇 번을 읽어도 로그 자체는 변하지 않는다.

### 2. `ReplayConsumer` — 생성자 누락 발견 및 보완

처음 작성한 버전은 `final EventLog eventLog` 필드를 초기화하는 생성자가 없어 컴파일이
안 되는 상태였다. 생성자를 추가해서 `EventLog`와 컨슈머 식별용 `name`을 주입받도록
완성했다.

```java
public class ReplayConsumer implements Runnable {
    private final EventLog eventLog;
    private final String name;
    private int offset = 0; // 이 컨슈머만의 읽기 위치

    public ReplayConsumer(EventLog eventLog, String name) {
        this.eventLog = eventLog;
        this.name = name;
    }

    @Override
    public void run() {
        while (true) {
            if (offset < eventLog.size()) {
                String event = eventLog.read(offset);
                log.info("{} -> offset {}: {} 처리", name, offset, event);
                offset++;
            } else {
                sleep(100); // 폴링 — Kafka Consumer의 poll()과 개념적으로 동일
            }
        }
    }
}
```

**해설**: `offset`이 `EventLog`가 아니라 `ReplayConsumer` **인스턴스 필드**로 존재한다는
게 핵심이다 — 브로커(`EventLog`)는 어떤 컨슈머가 어디까지 읽었는지 전혀 모르고, 각
컨슈머가 자기 위치를 스스로 들고 다닌다.

### 3. 지연 구독 시나리오 실행

```java
EventLog eventLog = new EventLog();
eventLog.append("order-1");
eventLog.append("order-2");
eventLog.append("order-3"); // 컨슈머 없이 먼저 3건 다 쌓아둠

startDaemon(new ReplayConsumer(eventLog, "consumer-1"));
Thread.sleep(1000);
startDaemon(new ReplayConsumer(eventLog, "consumer-2")); // 1초 늦게 시작
Thread.sleep(1000);
```

실제 실행 결과:
```
[Thread-3] consumer-1 -> offset 0: order-1 처리
[Thread-3] consumer-1 -> offset 1: order-2 처리
[Thread-3] consumer-1 -> offset 2: order-3 처리
[Thread-4] consumer-2 -> offset 0: order-1 처리
[Thread-4] consumer-2 -> offset 1: order-2 처리
[Thread-4] consumer-2 -> offset 2: order-3 처리
```

**해설**: `consumer-2`가 1초 늦게 시작했는데도 `order-1`부터 `order-3`까지
`consumer-1`과 완전히 동일하게 전부 받았다. LOG003의 `PublishManager`(큐 방식)였다면
`consumer-2`는 아무것도 못 받았을 상황 — 오프셋 기반 로그 구조가 재생을 가능하게
한다는 게 정확히 확인됐다.

## 시행착오 / Q&A

**Q. `ReplayConsumer`가 처음에 왜 컴파일이 안 됐나?**
A. `final EventLog eventLog` 필드를 선언만 하고 초기화하는 생성자가 없었다. 자바에서
`final` 필드는 생성자(또는 필드 선언 시점)에서 반드시 초기화돼야 하는데 그 부분이
비어 있었던 것 — 생성자를 추가해서 해결했다.

## 트레이드오프 / 실무 함정 / 안티패턴

**트레이드오프**: 로그를 무한정 보관할 수는 없다 — Kafka도 retention 기간/용량 제한이
있어 결국 오래된 메시지는 삭제된다. "영구 보관"이 아니라 "큐보다 훨씬 오래, 여러
소비자가 독립적으로 재생 가능한 기간 동안" 보관하는 것에 가깝다. 그만큼 저장 공간과
운영 부담이 커진다 — RabbitMQ는 처리 후 바로 지워지니 이 부담이 없다.

**실무 함정**: "메시지가 안 사라지니까 언제든 다시 읽으면 된다"고 믿고 컨슈머 쪽
멱등성 처리를 소홀히 하는 경우가 있다. 재생 가능하다는 것과 같은 이벤트를 두 번
처리해도 안전하다는 것은 별개 문제다 — LOG017(멱등성 설계)에서 다룬다.

**안티패턴**: Kafka를 RabbitMQ처럼 "처리 후 브로커가 알아서 지워주는 큐"로 취급하고
오프셋 커밋을 소홀히 하는 것. 오프셋 관리는 브로커가 아니라 컨슈머(그룹)의 책임이라,
이걸 소홀히 하면 재시작 시 전체를 다시 읽거나(중복) 중간을 건너뛰는(유실) 문제가
생긴다.

## 더 생각해볼 것

지금은 `consumer-1`, `consumer-2`가 완전히 독립된 오프셋을 갖고 각자 진행한다. 만약 한
컨슈머가 처리에 실패해서 자기 오프셋을 되돌리고 재시도한다면, 다른 컨슈머의 진행에
영향을 줄까? (오프셋이 컨슈머별로 완전히 독립적이라는 게 왜 중요한지 — Chapter 8~9로
이어지는 질문)

## 최종 구성

`relay` 패키지에 `EventLog`(append-only 로그), `ReplayConsumer`(오프셋 기반 폴링
컨슈머, 생성자 보완), 테스트 `ReplayConsumerTest`를 추가했다.

## ADR

### Decision
오프셋을 브로커(`EventLog`)가 아니라 컨슈머(`ReplayConsumer`) 인스턴스 필드로
독립 관리한다.

### Drivers
실제 Kafka에서 "어디까지 읽었는가"를 추적하는 책임이 컨슈머(정확히는 컨슈머 그룹)에
있다는 구조를 정확히 반영하고 싶었다.

### Alternatives
`EventLog`가 컨슈머별 오프셋을 맵으로 들고 관리 — 브로커가 컨슈머 상태를 알아야 하는
구조가 되어 실제 Kafka의 책임 소재와 달라지므로 기각.

### Consequences
`EventLog`는 완전히 무상태(stateless)라 재사용이 쉽지만, 실제 Kafka가 지원하는
"커밋된 오프셋을 브로커가 기억해서 컨슈머 재시작 시 이어 읽기" 같은 동작은 이 미니
구현으로는 재현하지 못한다 — 필요해지면 이후 챕터에서 확장.

### Follow-ups
Chapter 5 — ApplicationEventPublisher와 실제 브로커의 차이 (Phase 0 마지막 챕터).
