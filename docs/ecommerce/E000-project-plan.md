# Phase 4-1 계획 — 이커머스 SAGA (Chapter 21)

## Context

Phase 1~3(Ch1~20)은 `learning` 단일 모듈 안에서 챕터별 프로파일로 개념을 하나씩 실습하는 방식이었다. Phase 4는 구조가 달라진다 — 독립적으로 실행 가능한 여러 서비스 모듈(`ecommerce-order`, `ecommerce-inventory`, `ecommerce-payment`, `ecommerce-delivery`, `ecommerce-notification`)이 Kafka로만 통신하며 하나의 SAGA(분산 트랜잭션)를 구성한다. CLAUDE.md에 포트(8081~8085)까지는 이미 정해져 있지만, 서비스 간 이벤트 흐름·보상 트랜잭션 설계·공통 모듈 구성·DB 전략은 아직 정해지지 않았다. 이커머스 도메인 착수 전에 구조를 먼저 확정한다.

이 프로젝트의 진행 방식(CLAUDE.md)은 그대로 유지된다 — 코드/설정 파일은 직접 쓰지 않고 개념 설명 + 코드 가이드만 제공하며, 단계별/서비스별로 직접 타이핑/실행한다. 이 문서는 "구현 계획"이 아니라 "어떤 순서로, 어떤 구조로 진행할지"에 대한 설계 문서다.

## 아키텍처 결정

### 1. Gradle 멀티모듈 구조

```
settings.gradle.kts
├── include("ecommerce-common")
├── include("learning")
├── include("ecommerce-order")        # 8081
├── include("ecommerce-inventory")    # 8082
├── include("ecommerce-payment")      # 8083
├── include("ecommerce-delivery")     # 8084
└── include("ecommerce-notification") # 8085
```

각 서비스는 자체 `build.gradle.kts`, `application.yml`, main 클래스, 포트를 가진다(CLAUDE.md 원칙 그대로).

### 2. 루트 `build.gradle.kts` 재도입

지금까지는 "서브모듈 공통 설정 필요해지면 재도입"이라며 루트 `build.gradle.kts` 없이 진행했다. 이제 6개 모듈(공통 3개 + 신규 5개)이 같은 Spring Boot 버전/Java 21 툴체인/Lombok을 반복해서 선언해야 한다. `subprojects { }` 블록으로 공통 plugin/repository/toolchain만 루트에 올리고, 모듈별 `dependencies`는 각자 유지한다.

### 3. `common` → `ecommerce-common` 모듈 — 무엇을 채우는가

당초 CLAUDE.md 구조상 `common`은 Phase 4 전체(ecommerce/logistics/finance)가 공유하는 단일 모듈로 잡혀 있었다. 그러나 실제로 채워보니 `OrderCreatedEvent` 등은 ecommerce 도메인 전용이라 logistics/finance가 그대로 재사용할 수 없고, `learning`도 이 모듈을 참조하지 않아 사실상 이미 ecommerce 전용으로 쓰이고 있었다. 지금 없는 도메인을 위해 패키지를 미리 서브패키지로 나눠두는 대신, **쓰임새 그대로 모듈명을 `ecommerce-common`으로 바꾸는 쪽을 선택**했다 — `ecommerce-order` → `io.hkarling.ecommerce.order`와 동일한 규칙으로 `ecommerce-common` → `io.hkarling.ecommerce.common`. logistics/finance 단계에서 실제로 공유 코드가 필요해지면 그때 `logistics-common` 등을 별도로 만든다(YAGNI).

채운 내용:

- **공유 이벤트 스키마**: `OrderCreatedEvent`, `InventoryReservedEvent`, `InventoryReservationFailedEvent`, `PaymentCompletedEvent`, `PaymentFailedEvent`, `DeliveryStartedEvent` — 서비스 간 주고받는 이벤트를 타입별 `record`로 정의(Ch12 JSON 직렬화 패턴을 각 서비스 `KafkaConfig`에서 재사용할 예정).
- **토픽 이름 상수**: `EcommerceTopics` 클래스(기존 `learning`의 `KafkaTopics` 패턴과 동일하게, private 생성자) — `order.created`, `inventory.reserved`, `inventory.reservation-failed`, `payment.completed`, `payment.failed`, `delivery.started`.
- 공통 Kafka 설정(Producer/Consumer 팩토리)은 넣지 않는다 — 서비스마다 필요한 조합(멱등성 설정, 에러 핸들러 등)이 다를 수 있어 각 서비스 자체 `KafkaConfig`로 유지하는 게 Ch13 LOG에서 반복 확인한 "설정은 필요할 때 분리" 원칙과 맞는다.

**상태**: 완료. `ecommerce-common` + 5개 서비스 모듈 컴파일 확인, 5개 서비스 각자 포트(8081~8085)로 독립 기동 확인.

### 4. DB 전략

로컬 `docker-compose.yml`엔 Postgres 컨테이너가 하나뿐이다(`practice_kafka` 단일 DB). 서비스별로 별도 DB 컨테이너를 새로 띄우는 대신, 같은 DB 안에서 **테이블명에 서비스 접두사를 붙이는 방식**(`order_orders`, `inventory_stock`, `payment_transactions` 등)을 쓴다. 다만 "실제 마이크로서비스라면 서비스마다 DB를 분리하는 게 원칙"이라는 걸 실무 함정으로 명시한다.

### 5. SAGA 설계 — Choreography 방식

Orchestrator(중앙 조정자) 없이, 각 서비스가 이전 서비스의 이벤트를 구독해서 다음 이벤트를 발행하는 **choreography 방식**을 쓴다 — "서비스 간 통신은 Kafka를 통해서만"이라는 프로젝트 원칙과 맞고, 지금까지 배운 발행/구독 패턴을 그대로 확장하는 게 자연스럽다.

실패 시나리오를 미리 다 설계해두지 않는다 — **① 각 서비스의 도메인/책임을 먼저 정의 → ② 이벤트 계약(누가 무엇을 발행/구독하는지)을 정의 → ③ 실제로 만들면서 드러나는 오류 케이스를 그때그때 SAGA 보상으로 커버**하는 순서로 간다.

#### 5-1. 서비스별 도메인 책임 정의

| 서비스 | 소유하는 상태 | 트리거(구독) | 처리 내용 | 발행하는 이벤트 |
|---|---|---|---|---|
| order | 주문 생명주기(`CREATED`/`INVENTORY_RESERVED`/`PAYMENT_COMPLETED`/`DELIVERY_STARTED`/`CANCELLED`) | (외부 트리거: UI 또는 테스트) + 하위 서비스들의 진행 이벤트 | 주문 생성, 하위 이벤트 수신 시 SAGA 진행 상태 갱신 | `OrderCreated` |
| inventory | 상품별 재고 수량 | `OrderCreated` | 재고 조건부 차감(동시성 고려) | `InventoryReserved` / `InventoryReservationFailed` |
| payment | 결제 트랜잭션 | `InventoryReserved` | 결제 승인 시뮬레이션(의도적 실패 케이스 포함) | `PaymentCompleted` / `PaymentFailed` |
| delivery | 배송 건 | `PaymentCompleted` | 배송 시작 기록 | `DeliveryStarted` |
| notification | 없음(무상태) | 주요 이벤트 다수 | 로그로 알림 시뮬레이션 | 없음 |

이 표 자체가 실제 챕터 진행 시 "개념 정리"의 출발점이 된다 — 각 서비스를 만들기 직전에 이 표의 해당 행을 다시 짚고 시작한다.

#### 5-2. 이벤트 흐름 그래프(정상 경로)

```
(트리거) → OrderCreated
             │
             ▼
        [inventory] 재고 차감
             │
    ┌────────┴────────┐
    ▼                  ▼
InventoryReserved   InventoryReservationFailed
    │                  │
    ▼                  ▼
[payment] 결제      [order] 상태 → CANCELLED (보상 1)
    │
┌───┴───┐
▼        ▼
PaymentCompleted   PaymentFailed
    │                  │
    ▼                  ▼
[delivery] 배송     (예상 보상 2 — 실제 필요성 확인되면 추가)
    │
    ▼
DeliveryStarted

[notification]은 OrderCreated / PaymentCompleted / DeliveryStarted / *Failed 계열을 모두 구독해 로그만 남긴다.
```

`InventoryReservationFailed → order 보상(CANCELLED)`은 가장 먼저 만들 가능성이 큰 보상 경로라 미리 이벤트 이름까지 잡아뒀다. `PaymentFailed`로 인한 재고 복원 보상은 지금 미리 설계하지 않는다 — payment 서비스를 실제로 만들면서 "결제 실패가 실제로 얼마나 흔한 케이스인지, 어떤 형태로 재현할지"가 명확해진 뒤에, 그 시점에 개념 설명부터 다시 잡고 추가한다.

### 6. 모니터링/조작 UI — Thymeleaf (order 서비스)

자동화된 테스트만으로는 SAGA가 실제로 어떻게 흘러가는지 체감하기 어렵다. `ecommerce-order`에 화면을 하나 두어, 직접 주문을 만들고 그 주문이 재고→결제→배송을 거치며 상태가 바뀌는 걸 눈으로 볼 수 있게 한다. (사용자 확인 완료 — order 서비스에 배치, polling 방식, 현재 상태만 표시)

- **위치**: 별도 모듈을 만들지 않고 `ecommerce-order`에 Thymeleaf를 붙인다 — order가 SAGA의 진입점이자 주문 상태의 대표 조회 지점이라 자연스럽다. 의존성 `spring-boot-starter-thymeleaf`를 `ecommerce-order`에만 추가한다.
- **order 서비스 역할 확장**: 지금까지는 order가 `OrderCreated`만 발행하고 보상 이벤트만 구독하는 그림이었다. 화면에 SAGA 전체 진행 상황을 보여주려면 order가 하위 서비스들의 진행 이벤트(`InventoryReserved`/`InventoryReservationFailed`/`PaymentCompleted`/`DeliveryStarted`, 추후 `PaymentFailed`)도 모두 구독해서 자신의 상태 보드(`order_orders.status` 컬럼, UPSERT)를 갱신해야 한다. 이건 정확히 Ch20 CQRS 패턴의 재사용이다 — 다만 이번엔 같은 서비스 내부 이벤트가 아니라 **다른 서비스들이 발행한 이벤트를 구독해서 자신의 읽기 모델을 갱신**하는 형태로 한 단계 더 나아간다.
- **화면 구성**:
  - 주문 생성 폼(상품/수량 입력 → POST로 주문 생성 트리거)
  - 주문 목록 — 각 주문의 현재 SAGA 상태(`CREATED`/`INVENTORY_RESERVED`/`PAYMENT_COMPLETED`/`DELIVERY_STARTED`/`CANCELLED`)를 표시
  - 짧은 간격(예: 3초) JS `fetch` 폴링으로 상태 갱신 — SSE/WebSocket 같은 진짜 push는 쓰지 않는다. 이 프로젝트에서 처음 등장하는 개념이라 범위를 단순하게 유지한다("더 생각해볼 것"에 남김).
  - 상태는 현재 값만 UPSERT로 보여준다(Ch20 `order_summary`와 동일한 패턴) — 단계별 도달 시각까지 보는 타임라인/이력 테이블은 이번 범위에서 제외한다.
- **서비스를 하나씩 만들 때마다 화면으로 확인**: inventory/payment/delivery를 순서대로 만들면서, 그때마다 order의 상태 프로젝터에 해당 이벤트 구독을 하나씩 추가한다. 서비스가 늘어날 때마다 화면에서 주문 상태가 한 단계씩 더 나아가는 걸 직접 보는 게 이번 챕터의 핵심 체감 포인트다.

### 7. 서비스별로 적용할 기존 패턴

| 서비스 | 적용 패턴 | 이유 |
|---|---|---|
| order | Outbox(Ch18) + CQRS 프로젝터(Ch20, 확장) | 주문 저장/발행의 원자성 + 하위 서비스 이벤트로 자기 상태 보드 갱신 |
| inventory/payment/delivery | Idempotent Consumer(Ch17, UPSERT 또는 멱등키) | 서비스 간 이벤트는 at-least-once |
| 전 서비스 | DLQ(Ch13) | 파싱 실패/포이즌 메시지 대비 |
| notification | 단순 `@KafkaListener` + 로그 | 부작용이 로그 출력뿐이라 멱등성 이슈 없음 |

### 8. 학습/구현 순서

1. ~~Gradle 스캐폴딩 — `settings.gradle.kts`, 루트 `build.gradle.kts`, `ecommerce-common`(이벤트+토픽 상수), 5개 서비스 모듈 뼈대(각자 `build.gradle.kts`/`application.yml`/main 클래스) → 5개 서비스가 각자 포트로 독립 기동되는지만 먼저 확인~~ **완료**
2. `ecommerce-order` — DB 스키마 + Outbox + `OrderCreated` 발행 + Thymeleaf 기본 화면(주문 생성 폼 + 목록, 상태는 아직 `CREATED`만 보임) → 테스트 + 화면으로 검증
3. `ecommerce-inventory` — 구독 + 재고 차감 + `InventoryReserved`/`InventoryReservationFailed` 발행 → order의 상태 프로젝터에 이 이벤트 구독 추가 → 화면에서 `INVENTORY_RESERVED` 상태가 뜨는지 확인
4. `ecommerce-payment` — 구독 + 결제 처리(시뮬레이션) + `PaymentCompleted` 발행 → order 프로젝터 확장 → 화면에서 `PAYMENT_COMPLETED` 확인
5. `ecommerce-delivery` — 구독 + `DeliveryStarted` 발행 → order 프로젝터 확장 → 화면에서 `DELIVERY_STARTED` 확인
6. `ecommerce-notification` — 여러 이벤트 구독 + 로그 알림 → 테스트
7. 전체 happy path e2e 확인(주문 하나가 5개 서비스를 순서대로 통과하는지, 화면에서도 마지막 상태까지 도달하는지)
8. 보상 트랜잭션 시나리오(재고 부족 → 주문 취소) 구현 및 검증 → 화면에서 `CANCELLED` 확인
9. LOG021 문서 작성(도메인 전체를 하나의 LOG로 — Phase 1~3처럼 세부 챕터 단위가 아니라 "21. 이커머스 SAGA" 자체가 하나의 커리큘럼 항목이므로)

## 검증 방법

각 서비스 완성 시점마다 Ch18/20에서 쓴 패턴대로(`@SpringBootTest` + 실제 로컬 Docker Kafka/Postgres + polling 기반 assertion) 개별 테스트로 검증한다. 전체 SAGA는 `ecommerce-order`에 주문 생성을 트리거하는 테스트를 하나 두고, 최종적으로 `ecommerce-notification`의 로그(또는 각 서비스 DB 상태)까지 폴링해서 전체 파이프라인이 끝까지 도달하는지 확인하는 e2e 테스트로 검증한다.

## 결정 사항 요약

- 루트 `build.gradle.kts`를 `subprojects {}` 블록으로 재도입한다.
- DB는 테이블명 접두사로 서비스 간 경계를 나눈다(단일 `practice_kafka` DB 재사용).
- 실패 시나리오(보상 트랜잭션)는 미리 전부 설계하지 않는다. §5-1(도메인 책임 정의)과 §5-2(이벤트 흐름 그래프)로 서비스 간 계약을 먼저 확정하고, 서비스를 실제로 만들면서 드러나는 오류 케이스를 그때그때 SAGA 보상으로 다룬다. `InventoryReservationFailed → order 취소`는 흐름 그래프 설계 단계에서 이미 필요성이 명확해서 1차 구현에 포함한다. `PaymentFailed` 이후의 재고 복원 보상은 payment 서비스 구현 시점에 다시 짚는다.
- 자동화된 테스트 검증에 더해, `ecommerce-order`에 Thymeleaf 기반 모니터링/조작 화면을 둔다. 짧은 간격 폴링으로 SAGA 진행 상태(현재 값만, UPSERT)를 표시한다. order가 하위 서비스들의 진행 이벤트까지 구독해서 자기 상태 보드를 갱신하도록 역할이 확장된다(Ch20 CQRS 패턴의 서비스 간 확장).

## 더 생각해볼 것 (Phase 4 착수 전 메모)

- 폴링 대신 SSE/WebSocket으로 진짜 실시간 push를 구현하는 것 — 이번 1차 구현에서는 범위를 단순하게 유지하려고 뒤로 미뤘다.
- 주문 상태를 현재 값만이 아니라 이력(타임라인)으로 보여주는 것 — 이력 테이블 설계와 화면 쿼리가 더 필요해서 1차 범위에서 제외했다.
