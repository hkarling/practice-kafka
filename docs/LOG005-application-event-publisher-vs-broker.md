# LOG005 — ApplicationEventPublisher와 실제 브로커의 차이

## 배경 / 목표

Phase 0 마지막 챕터. LOG001~004에서 다룬 동기/비동기, 분리(decoupling), 재생 가능성
개념을 Spring의 `ApplicationEventPublisher`에 대입해서, 이미 알고 있던 도구가 이
스펙트럼에서 어디에 위치하는지 확인한다. 이번이 프로젝트에서 처음으로 실제 Spring
컨텍스트를 띄운 챕터이기도 하다 — Phase 1부터 진짜 Kafka가 들어오기 직전 단계.

## 개념 정리

- **In-Process + 기본 동기**: `publisher.publishEvent(event)`는 구독 중인
  `@EventListener` 메서드들을 같은 스레드에서 순차적·동기적으로 호출한다. `@Async`를
  명시하지 않으면 LOG001의 동기 호출 체인과 본질적으로 같다.
- **영속성 없음**: 이벤트는 저장되지 않는다. `publishEvent()` 호출 순간 리스너가
  즉시 소비하고 끝 — LOG004에서 본 "오프셋 기반 재생"이 전혀 불가능하다.
- **프로세스 경계를 못 넘음**: 같은 JVM 안에서만 동작한다. 서비스 간 통신에는 못
  쓰고, 한 서비스 내부의 관심사 분리에만 쓴다.

| | ApplicationEventPublisher | Kafka |
|---|---|---|
| 프로세스 경계 | 못 넘음 (in-process) | 넘음 (out-of-process) |
| 기본 동작 | 동기 | 비동기 |
| 영속성 | 없음 | 있음 |
| 재생(Replay) | 불가능 | 가능 |
| 용도 | 한 서비스 내부 관심사 분리 | 서비스 간 통신 |

## 진행 과정

### 1. 기본 상태(동기) 확인

```java
@Service
class OrderService {
    private final ApplicationEventPublisher publisher;

    void placeOrder(String orderId) {
        long start = System.currentTimeMillis();
        publisher.publishEvent(new OrderPlacedEvent(orderId));
        log.info("주문 접수 완료, 소요시간: {}ms", System.currentTimeMillis() - start);
    }
}

@Component
class OrderPlacedListener {
    @EventListener
    void handle(OrderPlacedEvent event) throws InterruptedException {
        Thread.sleep(500);
        log.info("{} 확인 메일 발송 완료", event.orderId());
    }
}
```

실행 결과:
```
i.h.learning.event.OrderPlacedListener : order-1 확인 메일 발송 완료
i.hkarling.learning.event.OrderService : 주문 접수 완료, 소요시간: 506ms
```

**해설**: 소요시간이 500ms대(506/511/515ms)로 나왔고, 리스너의 "확인 메일 발송 완료"
로그가 서비스의 "주문 접수 완료" 로그보다 **먼저** 찍혔다 — `publishEvent()` 호출
안에서 리스너가 끝까지 실행된 뒤에야 `placeOrder()`로 제어가 돌아왔다는 뜻. 완전한
동기 호출임이 확인됐다.

### 2. `@Async` 추가했으나 변화 없음 — `@EnableAsync` 누락 발견

리스너에 `@Async`만 추가하고 재실행했는데 결과가 동일했다(510~515ms, 순서도 동일).

**원인**: `@Async`는 `@EnableAsync`가 등록하는 후처리기(`AsyncAnnotationBeanPostProcessor`)가
빈을 프록시로 감싸줘야 실제로 동작한다. `@EnableAsync`가 애플리케이션 어디에도 없어서
`@Async`가 **아무 에러 없이 조용히 무시**되고 있었다.

**해결**:
```java
@EnableAsync
@SpringBootApplication
public class LearningApplication { ... }
```

### 3. `@EnableAsync` 추가 후 재실행 — 진짜 비동기 확인

```
i.hkarling.learning.event.OrderService : 주문 접수 완료, 소요시간: 10ms
i.hkarling.learning.event.OrderService : 주문 접수 완료, 소요시간: 1ms
i.hkarling.learning.event.OrderService : 주문 접수 완료, 소요시간: 1ms
(0.5초 후)
[task-3] i.h.learning.event.OrderPlacedListener : order-3 확인 메일 발송 완료
[task-2] i.h.learning.event.OrderPlacedListener : order-2 확인 메일 발송 완료
[task-1] i.h.learning.event.OrderPlacedListener : order-1 확인 메일 발송 완료
```

**해설**: 응답시간이 10ms/1ms/1ms로 즉시 리턴됐고, 리스너는 `task-1`~`task-3`이라는
별도 스레드에서 0.5초 뒤에 실행됐다. `@EnableAsync` 하나로 LOG001 같던 동작이 LOG002
같은 동작으로 완전히 바뀌었다 — 실행 순서도 뒤집혔다(발행 로그가 먼저, 리스너 로그가
나중).

### 4. `@Async` 상태에서 예외 발생 시 동작 확인

리스너가 `RuntimeException("에러 발생")`을 던지도록 수정하고 실행:

```
java.lang.RuntimeException: 에러 발생
    at io.hkarling.learning.event.OrderPlacedListener.handle(...)
    at org.springframework.aop.interceptor.AsyncExecutionInterceptor.lambda$invoke$0(...)
    ...
ERROR ... .a.i.SimpleAsyncUncaughtExceptionHandler : Unexpected exception occurred invoking async method: ...
```

`placeOrder()` 호출 쪽(테스트)에는 아무 영향이 없었다 — 응답시간, 성공 여부 모두 정상.

**해설**: `void`를 리턴하는 `@Async` 메서드에서 예외가 나면 Spring의
`SimpleAsyncUncaughtExceptionHandler`가 그 예외를 잡아 **ERROR 로그만 남기고 끝낸다**.
호출자는 이 실패에 대해 알 방법이 전혀 없다 — 리턴 타입이 `void`라 `Future`처럼
예외를 담아 돌려줄 수단 자체가 없기 때문이다. LOG004 마지막에 남긴 "`@Async` 리스너의
실패를 어떻게 알아챌 수 있는가"에 대한 실측 답변이 여기서 나왔다.

동기 버전(`@Async` 없이)에서 같은 예외가 `placeOrder()`까지 그대로 전파되는지는 별도로
실행하지 않았다 — LOG001과 동일한 메커니즘(같은 스레드, 같은 호출 스택)이라 결과가
자명하다고 판단해 생략.

## 시행착오 / Q&A

**Q. `@Async`를 붙였는데 왜 계속 동기로 동작했나?**
A. `@EnableAsync`가 애플리케이션에 없었다. `@Async`는 그 자체로는 아무 힘이 없고,
`@EnableAsync`가 등록하는 프록시 생성 메커니즘이 있어야 실제로 별도 스레드에서
실행된다. 어노테이션만 붙이고 활성화를 빠뜨리면 컴파일도 되고 실행도 되지만 조용히
무시된다는 게 핵심 — 뒤에서 다룰 실무 함정과 직결된다.

**Q. `@Async` 메서드에서 발생한 예외는 어디로 가는가?**
A. 리턴 타입이 `void`면 `SimpleAsyncUncaughtExceptionHandler`가 잡아 로그만 남기고
끝난다. `Future`/`CompletableFuture`를 리턴하면 그 안에 예외가 담겨서 호출자가
`.get()`을 호출하는 시점에 확인할 수 있다 — 지금 예제는 `void`라 이 경로 자체가
없다.

## 트레이드오프 / 실무 함정 / 안티패턴

**트레이드오프**: `@Async`를 붙이면 즉시 응답과 장애 격리를 얻지만, 그 순간부터 "이
이벤트가 실제로 처리됐는지" 추적하기 어려워진다. 별도 스레드에서 실행되니 예외가 나도
호출자는 전혀 모르고, 로그를 보고 있지 않으면 조용히 사라진다.

**실무 함정**: `@Async`만 붙이고 `@EnableAsync`를 빠뜨리는 실수는 아무 에러도 안 나서
발견하기 어렵다 (이번 챕터에서 직접 겪음). 또한 `@TransactionalEventListener`를 쓰면
"트랜잭션 커밋 후 처리"가 되어 얼핏 Outbox 패턴과 비슷해 보이지만, 여전히 같은 프로세스
안이라는 근본적 한계는 그대로다 — 커밋 직후, 리스너 실행 전에 애플리케이션이 죽으면
이벤트는 그냥 유실된다. 진짜 Outbox 패턴(LOG018)은 이벤트를 DB 트랜잭션과 함께
영속화해서 이 문제를 없앤다.

**안티패턴**: 서비스 간 통신을 `ApplicationEventPublisher`로 흉내 내려는 것.
모놀리식 앱 안에서 "나중에 마이크로서비스로 쪼갤 거니까"라며 도메인 이벤트를
`ApplicationEventPublisher`로 설계하는 건 결합도를 낮추는 좋은 습관이지만, "이렇게
해두면 나중에 Kafka로 바꾸기 쉽다"고 착각하면 안 된다 — 영속성/비동기/프로세스
분리라는 근본적으로 다른 보장을 제공하는 도구로 교체하는 것이라, 실패 처리 전략부터
다시 설계해야 한다.

## 더 생각해볼 것

지금은 리스너 실패가 로그로만 남고 아무도 재시도하지 않는다. 진짜 Kafka Consumer는
"처리 성공/실패를 어떻게 확정하고, 실패 시 어떻게 재시도하는가"에 대해 훨씬 명시적인
메커니즘(오프셋 커밋, 재시도, DLQ)을 제공한다 — Phase 1 Chapter 7~8(Producer/Consumer
동작 원리)로 이어지는 질문.

## 최종 구성

`learning` 모듈에 `io.hkarling.learning.event` 패키지로 `OrderPlacedEvent`(record),
`OrderService`(퍼블리셔), `OrderPlacedListener`(리스너, `@Async`)를 추가하고, 테스트
`OrderServiceTest`를 작성했다. `LearningApplication`에 `@EnableAsync`를 추가했다.

## ADR

### Decision
`@Async` 예외 처리를 커스터마이징하지 않고 기본 동작(로그만 남기고 끝)을 그대로 둔다.

### Drivers
Phase 0의 목표는 개념 확인이지 프로덕션 수준의 에러 핸들링 설계가 아니다. 실패가
조용히 사라진다는 사실 자체를 보여주는 게 이번 챕터의 요점이었다.

### Alternatives
`AsyncConfigurer`를 구현해서 커스텀 `AsyncUncaughtExceptionHandler`를 등록, 실패를
별도로 기록/알림 — 지금 단계에서는 범위 밖이라 기각. (Spring이 이런 커스터마이징
지점을 제공한다는 것만 확인.)

### Consequences
이 예제 코드는 실패를 조용히 삼키는 상태로 의도적으로 남겨둔다 — 문제를 보여주기
위한 예제이기 때문에, 굳이 지금 고치지 않는다.

### Follow-ups
Phase 1 시작 — Chapter 6: 토픽, 파티션, 오프셋. 여기서부터 실제 Kafka를 직접 기동해서
동작을 확인한다.
