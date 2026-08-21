# LOG003 — Point-to-Point vs Pub/Sub

## 배경 / 목표

Phase 0 챕터 3. LOG002 마지막에 남긴 질문 — "컨슈머가 여러 개면 메시지를 나눠 가져갈까,
각자 다 가져갈까" — 를 두 가지 구조를 직접 구현해서 실측으로 확인한다. 이 구분은 Kafka의
Consumer Group(LOG009)을 이해하는 기초가 된다.

## 개념 정리

- **Point-to-Point (P2P, 큐 모델)**: 메시지 하나는 여러 컨슈머 중 **정확히 하나**만
  가져간다. 컨슈머를 늘리는 목적은 처리량 분산 — 워커 풀(worker pool) 패턴과 동일하다.
- **Pub/Sub (발행-구독 모델)**: 메시지 하나를 **구독 중인 모든 컨슈머가 각자 전부**
  받는다. 컨슈머를 늘리는 목적은 처리량 분산이 아니라, 같은 이벤트에 서로 다른 관심사를
  가진 소비자들에게 전부 알리기 위해서다.

Kafka는 이 둘을 Consumer Group 개념으로 동시에 구현한다 — 같은 그룹에 속한 컨슈머들끼리는
P2P처럼 파티션을 나눠 갖고, 서로 다른 그룹은 Pub/Sub처럼 각자 토픽 전체를 독립적으로
구독한다. 이 이중성이 Kafka가 "메시지 큐"이자 "이벤트 스트림" 둘 다로 불리는 이유다
(LOG004에서 더 다룬다).

## 진행 과정

### 1. `OrderConsumer`가 큐를 주입받을 수 있도록 변경

기존 `OrderConsumer`는 `QueueManager`의 공유 큐(P2P용)에 고정되어 있었다. Pub/Sub
실험에서는 구독자마다 **자기만의 큐**가 필요해서, 생성자를 오버로드했다.

```java
public class OrderConsumer {
    private final BlockingQueue<String> queue;

    public OrderConsumer() {
        this.queue = QueueManager.getInstance().getQueue(); // 기존 P2P용
    }

    public OrderConsumer(BlockingQueue<String> queue) {
        this.queue = queue; // Pub/Sub처럼 특정 큐를 직접 지정
    }
}
```

### 2. Pub/Sub용 `PublishManager` 작성

```java
public class PublishManager {
    private final List<BlockingQueue<String>> subscriberQueues = new CopyOnWriteArrayList<>();

    BlockingQueue<String> subscribe() {
        BlockingQueue<String> queue = new LinkedBlockingQueue<>();
        subscriberQueues.add(queue);
        return queue;
    }

    void publish(String message) {
        subscriberQueues.forEach(q -> q.offer(message)); // 구독자 전부에게 각각 전달
    }
}
```

### 3. 두 시나리오를 별도 테스트로 분리해서 실행

`PointToPointVsPubSubTest`에 두 테스트를 작성했다 — 챕터별로 테스트 파일을 나눠서 LOG
문서와 1:1로 대응되게 했다 (ADR 참고).

```java
@Test
@DisplayName("P2P: 컨슈머끼리 메시지를 나눠 가져간다")
void pointToPoint_MessagesAreDistributedAmongConsumers() throws InterruptedException {
    startDaemon(new OrderConsumer());
    startDaemon(new OrderConsumer());

    OrderProducer producer = new OrderProducer();
    producer.placeOrder("order-1");
    producer.placeOrder("order-2");
    producer.placeOrder("order-3");

    Thread.sleep(2000);
}

@Test
@DisplayName("Pub/Sub: 구독자 전원이 메시지를 전부 받는다")
void pubSub_AllSubscribersReceiveEveryMessage() throws InterruptedException {
    PublishManager publishManager = new PublishManager();

    startDaemon(new OrderConsumer(publishManager.subscribe()));
    startDaemon(new OrderConsumer(publishManager.subscribe()));

    publishManager.publish("order-1");
    publishManager.publish("order-2");
    publishManager.publish("order-3");

    Thread.sleep(2000);
}
```

실제 실행 결과 (두 테스트 순서대로 실행):

```
# Pub/Sub 테스트 (OrderProducer 로그가 없음 — publish()로 발행)
[Thread-4] order-1 처리 완료
[Thread-3] order-1 처리 완료
[Thread-4] order-2 처리 완료
[Thread-3] order-2 처리 완료
[Thread-3] order-3 처리 완료
[Thread-4] order-3 처리 완료

# P2P 테스트 (OrderProducer 로그 3번 등장)
주문 접수 완료, 소요시간: 0ms  (x3)
[Thread-6] order-2 처리 완료
[Thread-5] order-1 처리 완료
[Thread-6] order-3 처리 완료
```

**해설**:
- Pub/Sub 테스트에서는 `order-1`/`order-2`/`order-3` 각각이 **두 스레드(구독자) 모두**에서
  로그가 찍혔다 — 총 6줄 (3건 × 구독자 2명). 구독자 전원이 전체 메시지를 받는다는 게
  확인됐다.
- P2P 테스트에서는 각 주문이 **정확히 한 번씩만** 처리됐다 — 총 3줄. `Thread-6`이 먼저
  끝나서 `order-2` 처리 후 바로 `order-3`을 이어 가져갔다 — LOG002에서 봤던 경쟁
  컨슈머(competing consumers) 패턴이 컨슈머 2개로 늘어나도 동일하게 재현됐다.

## 시행착오 / Q&A

**Q. `OrderConsumer`가 왜 처음부터 큐를 주입받는 구조가 아니었나?**
A. LOG002에서는 P2P(공유 큐)만 다뤄서 `QueueManager` 싱글턴에 고정해도 충분했다. Pub/Sub이
필요해지면서 "컨슈머가 어떤 큐를 바라보는가"가 더 이상 고정된 게 아니라 실행 시점에
결정돼야 하는 값이라는 게 드러났다 — 생성자 오버로드로 해결했지만, 실제로는 이게
처음부터 생성자 주입이었어야 할 설계였다는 걸 보여주는 사례.

## 트레이드오프 / 실무 함정 / 안티패턴

**트레이드오프**: P2P는 처리량은 늘릴 수 있지만 "이 메시지를 여러 관심사가 각자 봐야
하는" 요구를 못 채운다. Pub/Sub은 반대로 여러 소비자에게 다 전달은 되지만, 한 소비자가
느리다고 처리량이 자동으로 분산되진 않는다 (구독자 각자가 자기 몫을 스케일링해야 함).
실무에서는 "Pub/Sub 토픽 + 각 구독자 내부에서 P2P로 워커 풀 스케일링"을 조합해서 쓴다 —
정확히 Kafka Consumer Group의 설계다.

**실무 함정**: "컨슈머를 늘리면 무조건 처리량이 늘어난다"고 착각하고 Pub/Sub 구조에
컨슈머를 추가하는 경우가 있다. 실제로는 각 구독자가 전체 메시지를 다 받기 때문에, 한
구독자의 처리 속도는 그 구독자 자신의 워커 수에 달려있지 다른 구독자를 늘린다고
빨라지지 않는다.

**안티패턴**: 하나의 큐/토픽에 서로 다른 관심사를 가진 컨슈머들을 P2P로 묶는 것.
예를 들어 "재고 차감"과 "알림 발송"이 같은 큐를 P2P로 나눠 가지면, 어떤 주문은 재고만
차감되고 알림은 아예 안 가는 상황이 생긴다 — 처리량 분산이 필요한 그룹과 전체 통지가
필요한 그룹을 섞으면 안 된다.

## 더 생각해볼 것

Pub/Sub에서 구독자가 발행이 다 끝난 뒤에 뒤늦게 `subscribe()`하면, 그 구독자는 이미
지나간 메시지를 받을 수 있을까? 지금 `PublishManager` 구조로는 못 받는다 (구독 시점
이후의 `publish()`만 받음) — 이게 다음 챕터(메시지 큐 vs 이벤트 스트림)에서 다룰
"메시지가 소비되면 사라지는 큐 방식"과 "메시지가 보관되어 재구독/재생이 가능한 스트림
방식"의 차이로 이어진다.

## 최종 구성

`nonblocking` 패키지에 `OrderConsumer` 생성자 오버로드(큐 직접 주입)를 추가하고,
`PublishManager`를 신규 작성했다. 테스트는 `PointToPointVsPubSubTest`로 챕터 2의
`OrderNonBlockingTest`와 분리했다.

## ADR

### Decision
챕터별로 테스트 클래스를 분리해서 관리한다.

### Drivers
챕터마다 실험 주제가 다르고, LOG 문서와 테스트 파일이 1:1로 대응되면 나중에 "이 챕터
실습 코드가 어디 있더라"를 찾기 쉽다.

### Alternatives
하나의 테스트 클래스에 챕터별 테스트 메서드를 계속 추가 — 챕터가 늘어날수록 파일이
비대해지고 서로 관련 없는 챕터의 실험 코드가 한 파일에 섞여서 기각.

### Consequences
테스트 파일 수가 챕터 수만큼 늘어난다. Phase 0처럼 학습용 실험 코드에서는 허용 가능한
트레이드오프.

### Follow-ups
Chapter 4 — 메시지 큐 vs 이벤트 스트림 (RabbitMQ vs Kafka 차이).
