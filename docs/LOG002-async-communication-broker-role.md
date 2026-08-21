# LOG002 — 비동기 통신과 메시지 브로커의 역할

## 배경 / 목표

Phase 0 챕터 2. LOG001에서 확인한 동기 호출의 세 가지 문제(강결합, 응답 대기, 장애 전파)를
메시지 브로커가 어떻게 완화하는지 인메모리 큐로 직접 대조해서 체감하는 게 목표다. 아직 실제
Kafka는 쓰지 않는다 — Phase 1부터 진짜 브로커를 붙이고, 지금은 "분리(Decoupling)"라는
핵심 개념 자체를 코드로 확인하는 단계.

## 개념 정리

메시지 브로커의 역할은 결국 프로듀서와 컨슈머를 세 가지 축으로 **분리**하는 것이다.

- **공간적 분리 (Spatial Decoupling)**: 프로듀서는 컨슈머가 누구인지, 어디 있는지 몰라도
  된다. 브로커에 발행만 하면 끝. → LOG001의 강결합 문제를 해결.
- **시간적 분리 (Temporal Decoupling)**: 프로듀서와 컨슈머가 동시에 살아있을 필요가 없다.
  브로커가 메시지를 들고 있으니, 컨슈머가 늦게 뜨거나 잠깐 죽어도 프로듀서는 영향받지 않는다.
- **동기화 분리 (Synchronization Decoupling)**: 프로듀서는 발행 후 응답을 기다리지 않고
  바로 리턴한다. → LOG001의 응답 대기(블로킹) 문제를 해결.

이 세 가지가 합쳐지면 장애 전파도 자연스럽게 끊긴다 — 컨슈머 쪽 예외는 프로듀서가 이미
리턴한 뒤라 그 실행 흐름과 완전히 분리된다.

이건 "문제가 사라진다"가 아니라 **"책임이 브로커로 이동한다"**는 뜻이다. 가용성, 순서
보장, 재처리 같은 부담을 이제 브로커(그리고 그걸 운영하는 쪽)가 진다. LOG005
(ApplicationEventPublisher와 실제 브로커의 차이)에서 "브로커가 이 책임을 얼마나 진지하게
지느냐"의 스펙트럼을 더 다룬다 — in-process 이벤트는 이 책임을 거의 안 지고, Kafka 같은
실제 브로커는 영속성까지 지는 식.

## 진행 과정

### 1. 인메모리 미니 브로커 구현

`BlockingQueue`를 브로커 대역으로 써서 `QueueManager`(싱글턴으로 큐 하나 공유),
`OrderProducer`, `OrderConsumer`를 작성했다.

```java
public class OrderProducer {
    private final BlockingQueue<String> queue = QueueManager.getInstance().getQueue();

    void placeOrder(String orderId) throws InterruptedException {
        long start = System.currentTimeMillis();
        queue.put(orderId); // 발행만 하고 즉시 리턴 — 컨슈머를 기다리지 않음
        System.out.println("주문 접수 완료, 소요시간: " + (System.currentTimeMillis() - start) + "ms");
    }
}

public class OrderConsumer {
    private final BlockingQueue<String> queue = QueueManager.getInstance().getQueue();

    public void run() throws InterruptedException {
        while (true) {
            String orderId = queue.take(); // 블로킹은 컨슈머 자기 스레드 안에서만 일어남
            try {
                sleep(200); // 재고 확인
                sleep(300); // 결제 처리
                System.out.println(orderId + " 처리 완료");
            } catch (Exception e) {
                System.out.println(orderId + " 처리 실패: " + e.getMessage());
            }
        }
    }
}
```

**해설**: `queue.put()`은 프로듀서 스레드에서 실행되고 큐가 가득 차지 않는 한 즉시
리턴된다. `queue.take()`는 컨슈머 스레드에서만 블로킹된다 — 블로킹이 사라진 게 아니라
**컨슈머 쪽으로 옮겨간 것**이라는 게 핵심.

### 2. `Runnable`로 못 넘기는 문제

`new Thread(consumer)`로 바로 넘기려 했으나 컴파일 에러 발생. `OrderConsumer.run()`이
`throws InterruptedException`을 선언하는데, `Runnable.run()`은 checked exception을 던질
수 없어서 시그니처가 안 맞았다.

**해결**: 스레드를 띄우는 쪽에서 람다로 감싸서 처리.
```java
Thread consumerThread = new Thread(() -> {
    try {
        consumer.run();
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
    }
});
consumerThread.setDaemon(true);
consumerThread.start();
```

**해설**: 이 문제 자체가 "왜 컨슈머는 자기만의 스레드/생명주기 관리가 필요한지"를 보여준다.
실제 Kafka Consumer 내부에서도 인터럽트/종료 시그널 처리를 이런 식으로 하고 있다.

### 3. 시간적 분리 + 즉시 응답 확인

테스트에서 컨슈머를 아직 안 띄운 상태로 주문 2건을 먼저 접수하고, 그다음 컨슈머를
시작한 뒤 주문 1건을 추가로 접수했다.

```java
producer.placeOrder("order-1");
producer.placeOrder("order-2");
// (컨슈머 시작)
producer.placeOrder("order-3");
Thread.sleep(2000);
```

실제 실행 결과:
```
주문 접수 완료, 소요시간: 0ms
주문 접수 완료, 소요시간: 0ms
주문 접수 완료, 소요시간: 0ms
order-1 처리 완료
order-2 처리 완료
order-3 처리 완료
```

**해설**: 세 번의 `placeOrder()` 모두 0ms로 즉시 리턴됐다 — LOG001에서 같은 시나리오가
약 500ms 걸렸던 것과 대비된다. 컨슈머를 늦게 띄웠는데도 먼저 들어온 `order-1`,
`order-2`가 유실되지 않고 큐에 쌓여있다가 순서대로 처리됐다 — 시간적 분리가 실제로
동작함을 확인.

### 4. 장애 격리 확인

`OrderConsumer`에서 `RuntimeException("PG 타임아웃")`을 던지도록 바꾸고 동일 시나리오
재실행:
```
주문 접수 완료, 소요시간: 0ms
주문 접수 완료, 소요시간: 1ms
주문 접수 완료, 소요시간: 0ms
order-1 처리 실패: PG 타임아웃
order-2 처리 실패: PG 타임아웃
order-3 처리 실패: PG 타임아웃
```

**해설**: 컨슈머 쪽에서 세 건 모두 처리 실패했는데도, 프로듀서의 접수 응답시간은
여전히 0~1ms로 전혀 영향받지 않았다. LOG001에서 같은 실패가 `OrderService`까지
그대로 전파됐던 것과 정확히 대비되는 결과 — 장애 전파가 차단됨을 실측으로 확인.

## 시행착오 / Q&A

**Q. `while (true)`에 종료 조건이 없는데 문제 아닌가?**
A. 실제 Kafka Consumer도 `while (running) { poll(); process(); }` 형태의 무한 루프로
동작한다 — 컨슈머는 "처리할 게 없으면 끝난다"가 아니라 "명시적으로 종료시킬 때까지
계속 기다린다"가 정상 동작이라 루프 자체는 의도된 설계다. 실제 문제는 테스트 환경에서
이 무한 루프가 JVM 종료를 막는다는 것 — `Thread.setDaemon(true)`로 해결했다. 더
현실적인 방식(poison pill, `volatile running` 플래그)도 있지만, Phase 1에서 Spring이
컨슈머 생명주기를 대신 관리해주므로 지금 단계에서 정교하게 만들 필요는 없다고 판단.

## 트레이드오프 / 실무 함정 / 안티패턴

**트레이드오프**: 프로듀서는 "접수됐다"만 보장하지 "처리됐다"는 보장하지 못한다.
Chapter 1에서는 `placeOrder()`가 끝나면 결제까지 성공했다는 걸 보장했지만, 지금
구조에서는 그 확신이 사라진다 — 처리 결과를 알려면 별도의 상태 조회나 결과 이벤트가
필요하다 (CQRS로 이어지는 지점).

**실무 함정**: `BlockingQueue`는 인메모리라 프로세스가 죽으면 큐에 있던 메시지도
같이 사라진다. 지금 예제로 "시간적 분리"는 확인했지만, 이건 프로세스가 살아있는
동안만 유효한 분리다. 진짜 Kafka는 메시지를 디스크에 영속화해서 브로커나 컨슈머가
재시작해도 메시지가 안 사라진다는 게 핵심 차이 — LOG005에서 더 다룬다.

**안티패턴**: `OrderConsumer`의 `catch` 블록이 예외를 로그만 찍고 넘어간다 (지금
코드가 정확히 이렇게 되어 있다). 4번 실습에서 확인했듯, 세 건이 전부 실패했는데도
그 사실은 콘솔 로그로만 남고 어디에도 기록되지 않는다 — 재시도나 DLQ 없이 그냥
삼켜버리면, 실패한 주문이 있었는지조차 나중에 알 방법이 없다 (LOG013에서 재시도/DLQ
설계로 이 문제를 제대로 다룬다).

## 더 생각해볼 것

지금 큐는 컨슈머 하나만 메시지를 가져간다. 컨슈머를 두 개(스레드 두 개) 띄우면 같은
주문을 두 컨슈머가 동시에 처리할 수도 있을까, 아니면 하나씩 나눠 가져갈까 — Chapter 3
(Point-to-Point vs Pub/Sub)으로 이어지는 질문.

## 최종 구성

`learning` 모듈에 `io.hkarling.learning.nonblocking` 패키지로 `QueueManager`(싱글턴
공유 큐), `OrderProducer`, `OrderConsumer`를 추가하고, 테스트
`OrderNonBlockingTest`에서 컨슈머를 daemon 스레드로 띄워 시나리오를 실행했다.

## ADR

### Decision
컨슈머 스레드의 시작/종료를 프로덕션 코드가 아니라 테스트 코드에서 직접 관리한다.

### Drivers
Phase 1 전까지는 Spring 컨테이너의 빈 생명주기 관리가 없다. 지금 프로덕션 코드에
스레드 시작/종료 로직을 만들어두면, Phase 1에서 Spring Kafka의 `@KafkaListener`로
전환할 때 그대로 버려질 코드가 된다.

### Alternatives
`OrderConsumer`에 자체 `start()`/`stop()` 메서드를 만들어 생명주기를 캡슐화 —
지금 단계에서는 불필요한 복잡도라 기각.

### Consequences
Phase 1에서 실제 Kafka Consumer를 붙이면 지금의 수동 스레드 관리 코드는 자연스럽게
Spring 빈으로 대체된다. 지금 만든 `nonblocking` 패키지는 개념 확인용으로만 남는다.

### Follow-ups
Chapter 3 — Point-to-Point vs Pub/Sub.
