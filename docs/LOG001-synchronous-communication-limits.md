# LOG001 — 동기 통신의 한계

## 배경 / 목표

Phase 0 챕터 1. 비동기 메시징/Kafka로 넘어가기 전에, 동기 호출이 갖는 구조적 한계 (강결합, 응답 대기, 장애 전파)를 코드로 직접 재현해서 체감하는 게 목표다. 다음 챕터 (비동기 통신과 메시지
브로커의 역할)에서 이 문제들이 어떻게 완화되는지 비교할 기준점을 여기서 만든다.

## 개념 정리

동기 통신은 호출자가 피호출자의 존재/가용성/응답을 전제로 동작한다. 이 전제가 세 가지 구조적 문제를 만든다.

- **강결합 (Tight Coupling)**: 호출자가 피호출자의 주소·스키마·가용성을 전부 알아야 한다. 피호출자가 바뀌거나 잠깐 내려가면 호출자도 즉시 영향을 받는다.
- **응답 대기 (Blocking)**: 호출자는 응답이 올 때까지 스레드를 점유한 채 기다린다. 체인이 길어질수록 지연시간이 순차적으로 누적된다.
- **장애 전파 (Cascading Failure)**: 체인 하위 단계의 실패가 그대로 상위 단계까지 전파된다. Circuit Breaker 등으로 전파를 빨리 끊을 수는 있어도, 결합 자체를 없애지는 못한다.

`practice-db-performance`/`bank-transfer`에서 다룬 in-process SAGA의 "여러 단계가 순차적으로 실패할 수 있다"는 문제가, 여기서는 프로세스 경계를 넘어 네트워크 너머의
서비스 간으로 확장된 형태로 나타난다. 차이는 실패가 드러나는 방식 — in-process는 스택 트레이스로 즉시 보이지만, 네트워크 너머는 타임아웃/커넥션 리셋처럼 훨씬 모호하게 나타난다.

## 진행 과정

### 1. 동기 호출 체인 구현

`OrderService → InventoryService → PaymentService` 3단계 체인을 순수 Java 클래스로 작성했다 (Phase 0은 개념 중심이라 Spring 컨텍스트 없이 진행).

```java
class PaymentService {

  boolean charge() throws InterruptedException {
    sleep(300); // 네트워크 호출 시뮬레이션
    return true;
  }
}

class InventoryService {

  private final PaymentService paymentService = new PaymentService();

  boolean reserve() throws InterruptedException {
    sleep(200);
    return paymentService.charge(); // 동기 호출 = 블로킹 대기
  }
}

class OrderService {

  private final InventoryService inventoryService = new InventoryService();

  void placeOrder() throws InterruptedException {
    long start = System.currentTimeMillis();
    inventoryService.reserve();
    System.out.println("총 소요시간: " + (System.currentTimeMillis() - start) + "ms");
  }
}
```

**해설**: 각 서비스가 `sleep()`으로 네트워크 호출을 흉내 내고, 상위 서비스는 하위 서비스의
`sleep()`이 끝날 때까지 스레드를 점유한 채 대기한다 — 실제 동기 HTTP 호출과 동일한 블로킹 구조.

### 2. 지연시간 누적 측정

`sleep(300)` / `sleep(200)` 조합:

```
총 소요시간: 520ms
```

`sleep(1300)` / `sleep(1200)` 조합:

```
총 소요시간: 2524ms
```

**해설**: 각각 300+200=500ms, 1300+1200=2500ms에 근접한 값이 나왔다. 합계보다 20~24ms 정도 더 걸린 이유는 `Thread.sleep()`이 "최소 이 시간만큼 잠든다"는 보장이지
정확한 시간을 보장하지 않기 때문이다 (OS 스케줄러가 스레드를 깨우는 타이밍 오차 + 메서드 호출 오버헤드). 오차 자체보다 중요한 건 **병렬이 아니라 순차적으로 쌓인다**는 패턴이 그대로 재현됐다는 것 —
병렬이었다면 총 소요시간은 `max(300, 200)`에 가까웠을 것이다.

### 3. 장애 전파 확인

`PaymentService.charge()`가 `RuntimeException("PG 타임아웃")`을 던지도록 수정 후 실행:

```
java.lang.RuntimeException: PG 타임아웃
    at io.hkarling.learning.blocking.PaymentService.charge(PaymentService.java:13)
    at io.hkarling.learning.blocking.InventoryService.reserve(InventoryService.java:11)
    at io.hkarling.learning.blocking.OrderService.placeOrder(OrderService.java:9)
    at io.hkarling.learning.blocking.OrderServiceTest.placeOrder(OrderServiceTest.java:10)
```

**해설**: 중간 계층 어디에도 `catch`가 없어서 예외가 그대로 최상위 (테스트 코드)까지 전파됐다. 지금은 같은 JVM 안이라 스택 트레이스로 원인이 명확히 보이지만, 실제로 이 세 서비스가 별도
프로세스였다면 `OrderService`는 `PaymentService`가 왜 실패했는지 원인도 모른 채 타임아웃/커넥션 예외만 받게 된다.

## 시행착오 / Q&A

**Q. 총 소요시간이 각 단계 `sleep()` 값의 합과 정확히 일치하지 않고 조금 더 걸리는 이유는?**
A. `Thread.sleep()`은 "최소 이 시간만큼 잠든다"는 보장이지 정확한 시간을 보장하지 않는다. OS 스케줄러가 스레드를 다시 깨우는 타이밍에 오차가 있고, 메서드 호출 자체의 오버헤드도 약간 더해진다.
20~24ms 수준의 오차는 정상 범위.

## 트레이드오프 / 실무 함정 / 안티패턴

**트레이드오프**: 동기 호출이 항상 나쁜 건 아니다. 즉시 결과가 필요한 경우 (로그인 인증, 재고 확인 후 바로 결제 가능 여부 응답)엔 동기가 자연스럽고 디버깅도 쉽다. 비동기는 "지금 당장 결과를 몰라도
되는" 흐름에서만 이득이 있다.

**실무 함정**: Circuit Breaker나 타임아웃만 넣어두고 "장애 전파를 막았다"고 착각하기 쉽다. 실제로는 요청이 실패로 빨리 끝나게 만든 것뿐이고, 그 요청 자체 (주문, 결제 등)가 유실되거나 불일치
상태로 남는 문제는 여전히 남는다. 이 문제는 이후 Outbox 패턴에서 다시 다룬다.

**안티패턴**: 동기 체인이 3~4단계 이상 이어지는데 각 단계마다 재시도 로직을 개별적으로 넣는 것. 재시도가 체인을 타고 곱해지면 (A가 3번 재시도 → 그 안에서 B도 3번 재시도) 요청 하나가 최악의 경우
응답 지연을 기하급수적으로 늘릴 수 있다.

## 더 생각해볼 것

`InventoryService`가 `PaymentService` 호출 결과를 기다리지 않고 "일단 접수했다"고 즉시 응답한 뒤, 결과를 나중에 알려주는 방식으로 바꾼다면 무엇이 달라지는가 — 다음 챕터 (비동기
통신과 메시지 브로커의 역할)로 이어지는 질문.

## 최종 구성

`learning` 모듈에 `io.hkarling.learning.app` 패키지로 `PaymentService`, `InventoryService`,
`OrderService`와 테스트 `OrderServiceTest`를 추가했다. Spring 컨텍스트 없이 순수 Java 클래스로만 구성 — Phase 0은 개념 중심이라 프레임워크 설정 없이 최소한으로 유지.

## ADR

### Decision

Phase 0 챕터들은 Spring 컨텍스트/DI 없이 순수 Java로 개념 실습을 진행한다.

### Drivers

이 단계의 목표는 프레임워크 사용법이 아니라 동기/비동기 통신의 구조적 차이를 이해하는 것이라, Spring 설정 (Bean 등록, 컨텍스트 로딩 등)이 오히려 본질과 무관한 오버헤드로 작용할 수 있다.

### Alternatives

`@SpringBootTest` 기반으로 Bean을 등록해 실습 — 아직 Kafka/DI가 본격적으로 필요한 단계가 아니라 기각.

### Consequences

Phase 1부터는 실제 Kafka 연동이 필요해지므로 이 시점부터 Spring 설정이 자연스럽게 붙기 시작한다.

### Follow-ups

Chapter 2 — 비동기 통신과 메시지 브로커의 역할.
