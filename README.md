# practice-kafka

## 왜 이 프로젝트를 시작했는가

MSA·클라우드 환경에서 서비스 경계를 넘는 비동기 통신을 실제 메시지 브로커로 처리해본 경험이 없었기에, Kafka를 통해 분산 환경의 비동기 통신·장애 격리·이벤트 기반 아키텍처를 직접 구현하며 체득하는 것을 목표로 한다. 메시징 기초 개념부터 Kafka 핵심 동작 원리, Spring Kafka 실전 활용, 트랜잭션·멱등성·Outbox 같은 심화 패턴을 거쳐, 여러 서비스가 Kafka만으로 통신하는 실전 시나리오까지 단계적으로 다룬다.

## 학습 목표

이 프로젝트를 통해 다음을 직접 구현하고 판단할 수 있게 되는 것을 목표로 한다:

- 단일 프로세스 이벤트와 분산 메시징의 근본적 차이를 설명하고, 언제 어떤 방식이 적합한지 판단할 수 있다
- 파티션/오프셋/컨슈머 그룹 구조를 이해하고, 순서 보장과 확장성 사이의 트레이드오프를 설계 시점에 고려할 수 있다
- at-least-once 환경에서 발생하는 중복 처리 문제를 멱등성 설계로 해결할 수 있다
- Outbox 패턴으로 DB 트랜잭션과 메시지 발행 사이의 원자성을 확보할 수 있다
- 여러 서비스가 Kafka만으로 통신하는 이커머스 SAGA를 직접 구현하며, 서비스 간 직접 호출 없이 결합도를 낮추는 감각을 체득한다

## 기술 스택

- Java 21
- Spring Boot 4.1.0
- Spring for Apache Kafka
- PostgreSQL (Outbox 패턴 실습용)
- Gradle 멀티모듈 (Kotlin DSL)
- Kafka: Docker Compose로 로컬 기동

## 아키텍처

현재 구조 (Phase 0~4):

```
practice-kafka/
├── settings.gradle.kts
├── docker-compose.yml # Kafka + PostgreSQL 기동
├── common/ # 공유 이벤트 스키마, 공통 설정
├── learning/ # Phase 0~3 실습, 독립 실행 가능
└── ecommerce-*/ # Phase 4 실습 모듈 (진행 중)
├── ecommerce-common
├── ecommerce-order
├── ecommerce-payment
├── ecommerce-inventory
├── ecommerce-delivery
└── ecommerce-notification
```
각 서비스 모듈은 자체 main 클래스/설정/포트를 가지며, 서비스 간 통신은 Kafka를 통해서만 이루어진다(직접 호출 없음). logistics-*, finance-* 모듈은 이후 단계에서 추가 예정.

환경 설정: 멀티모듈 전환 완료, Docker Compose 구성은 다음 단계 진행 예정.

## 로컬 실행

```
docker compose up -d
./gradlew :learning:bootRun # Phase 0~3 실습
./gradlew :ecommerce-order:bootRun # Phase 4 실습 모듈 (진행 중)
```

Kafka·PostgreSQL 인프라(Docker Compose)는 아직 구성 전이라, Kafka 연동 기능은 이후 단계에서 추가된다.

## 진행 상태

### Phase 0 — 메시징 기초
- [x] 1. [동기 통신의 한계](docs/LOG001-synchronous-communication-limits.md)
- [x] 2. [비동기 통신과 메시지 브로커의 역할](docs/LOG002-async-communication-broker-role.md)
- [x] 3. [Point-to-Point vs Pub/Sub](docs/LOG003-point-to-point-vs-pubsub.md)
- [x] 4. [메시지 큐 vs 이벤트 스트림](docs/LOG004-message-queue-vs-event-stream.md)
- [x] 5. [ApplicationEventPublisher와 실제 브로커의 차이](docs/LOG005-application-event-publisher-vs-broker.md)

### Phase 1 — Kafka 핵심 개념
- [x] 6. [토픽, 파티션, 오프셋](docs/LOG006-topic-partition-offset.md)
- [x] 7. [Producer 동작 원리](docs/LOG007-producer-batching-acks-retries.md)
- [x] 8. [Consumer 동작 원리](docs/LOG008-consumer-polling-commit-offset.md)
- [x] 9. [Consumer Group](docs/LOG009-consumer-group-rebalancing.md)
- [x] 10. [이벤트 순서 보장](docs/LOG010-partition-key-design.md)

### Phase 2 — Spring Kafka 활용
- [x] 11. [KafkaTemplate + @KafkaListener 기본](docs/LOG011-kafkatemplate-kafkalistener-basics.md)
- [x] 12. [직렬화/역직렬화](docs/LOG012-json-serialization.md)
- [x] 13. [에러 처리 (재시도, DLQ)](docs/LOG013-error-handling-retry-dlq.md)
- [x] 14. [Testcontainers Kafka 통합 테스트](docs/LOG014-testcontainers-kafka-integration-test.md)
- [x] 15. [트랜잭션 (Kafka + DB)](docs/LOG015-kafka-db-transaction.md)

### Phase 3 — 핵심 패턴 심화
- [x] 16. [at-least-once vs exactly-once](docs/LOG016-at-least-once-vs-exactly-once.md)
- [x] 17. [멱등성 설계](docs/LOG017-idempotent-consumer-design.md)
- [x] 18. [Outbox 패턴](docs/LOG018-outbox-pattern.md)
- [x] 19. [Kafka Streams 기초](docs/LOG019-kafka-streams-basics.md)
- [x] 20. [CQRS + Kafka](docs/LOG020-cqrs-kafka.md)

### Phase 4 — 실습 예제 (멀티모듈)
- [ ] 21. 이커머스 SAGA - 진행중
- [ ] 22. 물류 추적
- [ ] 23. 금융 거래
