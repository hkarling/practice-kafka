# practice-kafka

Kafka 학습을 위한 실습 프로젝트. 메시징 기초 개념부터 Kafka 핵심 동작 원리, Spring Kafka 활용, 트랜잭션/멱등성/Outbox 같은 패턴, 마지막으로 도메인별 멀티모듈 실습(이커머스/물류/금융)까지 단계적으로 진행한다.

## 기술 스택

- Java 21
- Spring Boot 4.1.0
- Spring for Apache Kafka
- PostgreSQL (Outbox 패턴 실습용)
- Gradle 멀티모듈 (Kotlin DSL)
- Kafka: Docker Compose로 로컬 기동

## 아키텍처

현재 구조 (Phase 0~3):

```
practice-kafka/
├── settings.gradle.kts
├── docker-compose.yml        # Kafka + Zookeeper + PostgreSQL (구성 예정)
├── common/                   # 공유 이벤트 스키마, 공통 설정 (스켈레톤)
└── learning/                 # Phase 0~3 실습, 독립 실행 가능
```

Phase 4에 진입하면 도메인별 모듈(`ecommerce-*`, `logistics-*`, `finance-*`)이 순차적으로 추가된다. 각 서비스 모듈은 자체 main 클래스/설정/포트를 가지며, 서비스 간 통신은 Kafka를 통해서만 이루어진다 (직접 호출 없음).

환경 설정: 멀티모듈 전환 완료, Docker Compose 구성은 다음 단계 진행 예정.

## 로컬 실행

```
./gradlew :learning:bootRun
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
- [ ] 18. Outbox 패턴
- [ ] 19. Kafka Streams 기초
- [ ] 20. CQRS + Kafka

### Phase 4 — 실습 예제 (멀티모듈)
- [ ] 21. 이커머스 SAGA
- [ ] 22. 물류 추적
- [ ] 23. 금융 거래
