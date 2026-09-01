# E001 — Gradle 멀티모듈 스캐폴딩

## 배경 / 목표

E000 계획서 §8 순서 1번. Phase 4-1(이커머스 SAGA)의 첫 단계로, 실제 도메인 로직 없이 "5개 서비스가 각자 포트로 독립 기동되는지"만 먼저 확인하는 게 목표다. 이 단계가 끝나야 2단계(`ecommerce-order`)부터 실제 비즈니스 로직을 붙일 수 있다.

## 개념 정리

- **루트 `build.gradle.kts`를 `subprojects {}`로 재도입**: 지금까지는 `learning` 모듈 하나뿐이라 불필요했지만(CLAUDE.md: "서브모듈 공통 설정 필요해지면 재도입"), 6개 모듈(`ecommerce-common`, `learning`, `ecommerce-order/inventory/payment/delivery/notification`)이 같은 Java 21 툴체인 / Spring Boot 버전 / Lombok을 반복 선언해야 하는 시점이 됐다. `subprojects {}`는 plugin 적용 / repository / toolchain / Lombok 의존성까지만 공통화하고, 실제 스타터 의존성(`webmvc`, `kafka` 등)은 서비스마다 다르게 유지한다 — Ch13에서 확인한 "설정은 필요할 때만 분리" 원칙의 연장.
- **`common` → `ecommerce-common` 이름 결정**: E000 원안은 Phase 4 전체(ecommerce/logistics/finance)가 공유하는 도메인 중립적 `common` 모듈이었다. 그런데 실제로 채우면서 보니 `OrderCreatedEvent` 등은 ecommerce 도메인 전용이고, `learning`도 이 모듈을 참조하지 않아 이미 사실상 ecommerce 전용으로 쓰이고 있었다. 지금 없는 도메인(logistics/finance)을 위해 패키지를 미리 서브패키지로 나눠두는 대신, **쓰임새 그대로 모듈명을 `ecommerce-common`으로 바꾸는 쪽을 선택**했다 — `ecommerce-order` → `io.hkarling.ecommerce.order`와 동일한 규칙으로 `ecommerce-common` → `io.hkarling.ecommerce.common`. 근거와 대안은 하단 ADR 참고.
- **이벤트 스키마를 타입별 `record`로 분리**: `learning`에서는 `OrderEvent`(단일 타입 + `eventType` 필드)와 `OrderPlacedEvent`(타입별 record) 두 스타일을 다 써봤다. SAGA는 이벤트마다 페이로드가 다르므로(재고 실패엔 `reason`, 결제 완료엔 `amount`) 타입별 별도 record를 선택했다.
- **`EcommerceTopics` 상수 클래스**: `learning`의 `KafkaTopics` 패턴(private 생성자 + `public static final String`)을 그대로 재사용.

## 진행 과정

1. `settings.gradle.kts`에 `ecommerce-order/inventory/payment/delivery/notification` 5개 모듈 include, 루트 `build.gradle.kts`를 `subprojects {}` 블록으로 재구성.
2. 5개 서비스 모듈에 각자 `build.gradle.kts`(`webmvc` + `kafka` + `project(":ecommerce-common")`), `application.yaml`(포트 8081~8085), main 클래스(`@SpringBootApplication`) 생성.
3. **1차 점검에서 발견된 오류들** — 초기 작성본에 패키지명 오류(`io.hkarling.ecommerce` → 올바르게는 `io.hkarling.ecommerce.order`), `application.yaml`에서 `server.port`가 `spring:` 아래에 잘못 중첩된 것, 서비스별 `application.yaml` 내용이 서로 밀려서 섞인 것(예: `inventory`의 yaml에 `order`의 이름/포트가 들어있던 것), `ecommerce-delivery`가 빈 스켈레톤 상태로 남아있던 것 등을 확인. 전부 직접 수정해 5개 서비스 모두 올바른 포트/서비스명/패키지로 정리.
4. `common` 모듈을 `ecommerce-common`으로 리네임 — 디렉터리, `settings.gradle.kts`의 `include(...)`, 5개 서비스 `build.gradle.kts`의 `project(":common")` → `project(":ecommerce-common")`, 패키지 선언까지 전부 반영.
5. `ecommerce-common`에 이벤트 record 6종(`OrderCreatedEvent`, `InventoryReservedEvent`, `InventoryReservationFailedEvent`, `PaymentCompletedEvent`, `PaymentFailedEvent`, `DeliveryStartedEvent`)과 `EcommerceTopics` 상수 클래스 작성.
6. `EcommerceTopics`에서 `@NoArgsConstructor(access = PRIVATE)`와 `@UtilityClass`를 함께 사용 → 코드 리뷰에서 컴파일 충돌 가능성 지적, `@UtilityClass` 단독으로 정리.
7. `./gradlew :ecommerce-common:build`로 `ecommerce-common` + 5개 서비스 컴파일 확인, IntelliJ에서 5개 서비스 개별 기동(포트 8081~8085) 확인.

## 시행착오 / Q&A

**Q. `EcommerceTopics`에 `@NoArgsConstructor(access = AccessLevel.PRIVATE)`와 `@UtilityClass`를 같이 붙였는데 문제가 되나?**
Lombok `@UtilityClass`는 그 자체로 클래스를 `final`로 만들고, private 생성자를 자동 생성하고, 모든 멤버를 `static`으로 바꾼다. 여기에 생성자를 생성하는 다른 어노테이션(`@NoArgsConstructor`)을 더하면 생성자가 중복 선언되어 Lombok이 컴파일 에러를 낸다. `@UtilityClass` 하나만 남기거나, `learning`의 `KafkaTopics`처럼 순수 자바로 `private EcommerceTopics() {}`를 쓰는 것 중 하나를 선택해야 한다 — 이번엔 전자를 선택.

**Q. `./gradlew build`를 루트에서 실행했더니 `learning`까지 같이 빌드됐다. 막을 수 없나?**
Gradle 멀티모듈에서 프로젝트 경로 없이 태스크명만 주면(`build`), 그 태스크를 가진 **모든** 서브프로젝트에서 실행된다 — 설정 오류가 아니라 기본 동작. `./gradlew :ecommerce-common:build`처럼 프로젝트 경로를 붙이거나, `./gradlew build -x :learning:build`로 특정 프로젝트의 태스크만 제외하면 된다.

## 트레이드오프 / 실무 함정 / 안티패턴

- **공유 DB(`practice_kafka`) + 테이블명 프리픽스 전략**(E000 §4)은 로컬 학습 환경에서는 간단하지만, 실제 마이크로서비스라면 서비스마다 DB를 분리하는 게 원칙이다 — 스키마 변경 시 서비스 간 결합이 생기기 쉽다는 걸 염두에 둘 것.
- **`ecommerce-common`을 도메인마다 별도 모듈로 쪼개는 결정**은 지금은 명확하지만, 나중에 정말 도메인 간 공유 코드(예: 공통 이벤트 봉투 포맷, 공통 재시도 정책)가 필요해지면 그때는 진짜 도메인 중립적인 모듈을 새로 고민해야 한다 — 지금 결정이 "영원히 도메인 간 공유는 없다"는 뜻은 아니다.
- **`@UtilityClass`처럼 여러 책임을 가진 Lombok 어노테이션을 다른 생성자 어노테이션과 조합하는 것**은 흔한 실수 포인트다 — 어노테이션 하나가 이미 하는 일(생성자 생성, `static` 변환, `final` 처리)을 다른 어노테이션과 겹쳐 쓰지 않도록 주의.

## 더 생각해볼 것

- `ecommerce-common`의 이벤트 record들이 서비스 수가 늘어나며(2단계 이후) Jackson 직렬화 설정(예: `@JsonCreator`, 알 수 없는 필드 무시 등)이 필요해질 수 있다 — 지금은 기본 Jackson 동작에 맡기고 있다.
- logistics/finance 단계에 진입할 때 이번 `ecommerce-common` 리네임 결정을 그대로 반복할지, 아니면 그 시점에 실제로 ecommerce/logistics 간 공유가 필요한 게 드러나면 구조를 다시 볼지는 그때 가서 판단.

## 최종 구성

- `settings.gradle.kts`: `ecommerce-common`, `ecommerce-order`, `ecommerce-inventory`, `ecommerce-payment`, `ecommerce-delivery`, `ecommerce-notification` 6개 모듈 추가.
- 루트 `build.gradle.kts`: `subprojects {}` 블록으로 재구성(Java 21 툴체인, Lombok, `io.spring.dependency-management` 공통화).
- `ecommerce-common`: `event` 패키지에 이벤트 record 6종, `kafka` 패키지에 `EcommerceTopics`.
- 5개 서비스 모듈: 각자 `build.gradle.kts`(`webmvc` + `kafka` + `ecommerce-common` 의존), `application.yaml`(포트 8081~8085), `@SpringBootApplication` main 클래스만 존재 — 아직 도메인 로직/DB/Kafka 리스너 없음.
- DB 스키마, Kafka 설정(`KafkaConfig`), 실제 이벤트 발행/구독 로직은 2단계(`ecommerce-order`)부터 시작.

## ADR

- **Decision**: 공유 모듈명을 `common`이 아니라 `ecommerce-common`으로 정하고, 패키지도 `io.hkarling.ecommerce.common`으로 둔다.
- **Drivers**: (1) 이벤트 스키마가 실제로는 ecommerce 도메인 전용이라 다른 도메인이 그대로 재사용할 수 없음. (2) `learning`이 이 모듈을 참조하지 않아 이미 ecommerce 전용으로 쓰이고 있었음. (3) `ecommerce-order` 등 다른 모듈과 명명 규칙 일관성. (4) 지금 존재하지 않는 도메인(logistics/finance)을 위해 구조를 미리 설계하지 않는다(YAGNI).
- **Alternatives**:
  - (a) `common` 유지, 패키지도 `io.hkarling.common.event.*`로 완전히 평평하게 — 도메인이 늘어나면 한 폴더에 서로 다른 도메인 이벤트가 섞임.
  - (b) `common` 모듈은 유지하되 패키지 안에서 `io.hkarling.common.ecommerce.event.*`처럼 도메인 서브패키지로 분리 — 모듈은 하나로 유지되지만, 지금 시점엔 ecommerce 하나뿐이라 서브패키지 분리의 이점이 없음.
- **Consequences**: logistics/finance 단계에 진입하면 그 도메인 전용 `logistics-common`, `finance-common` 모듈을 각각 새로 만들어야 한다(반복 작업이 생기지만, 각 모듈이 무엇을 위한 것인지는 항상 명확하다).
- **Follow-ups**: logistics 단계(실습 22) 착수 시, 이 시점까지 정말 도메인 간 공유 코드가 필요했는지 다시 확인하고 필요하면 진짜 공유 모듈을 그때 도입한다.
