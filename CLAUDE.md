# Kafka 학습 프로젝트 — practice-kafka

## 기술 스택
- Java 21
- Spring Boot 4.1.0
- Spring Kafka
- PostgreSQL (Outbox 패턴 실습용)
- Gradle 멀티모듈 (Kotlin DSL)
- jakarta.* 패키지 네임스페이스 기준
- Kafka: Docker Compose로 로컬 기동

## 프로젝트 구조
멀티모듈이되 각 서비스 모듈은 독립적으로 실행 가능하게 설계한다.
- 각 서비스 모듈은 자체 main 클래스, application.yml, 포트를 가진다
- common 모듈은 공유 이벤트 스키마, 공통 설정만 담는다
- 서비스 간 통신은 Kafka를 통해서만 이루어진다 (직접 호출 없음)
- 전체 실행은 루트의 Docker Compose로 묶는다

현재 구조 (Phase 0~3):
```
practice-kafka/
├── build.gradle.kts (없음 — 서브모듈 공통 설정 필요해지면 재도입)
├── settings.gradle.kts
├── docker-compose.yml        # Kafka + Zookeeper + PostgreSQL
├── common/                   # 공유 이벤트 스키마, 공통 설정 (스켈레톤)
└── learning/                 # Phase 0~3 실습 (독립 실행 가능)
```

Phase 4 진입 시 도메인별로 순차 추가 (각각 독립 실행 가능):
```
# Phase 4-1 이커머스
├── ecommerce-order/          # 포트 8081
├── ecommerce-inventory/      # 포트 8082
├── ecommerce-payment/        # 포트 8083
├── ecommerce-delivery/       # 포트 8084
├── ecommerce-notification/   # 포트 8085

# Phase 4-2 물류 추적
├── logistics-pickup/         # 포트 8091
├── logistics-transport/      # 포트 8092
├── logistics-delivery/       # 포트 8093
├── logistics-tracking/       # 포트 8094

# Phase 4-3 금융 거래
├── finance-transaction/      # 포트 8101
├── finance-fraud/            # 포트 8102
├── finance-approval/         # 포트 8103
└── finance-settlement/       # 포트 8104
```

## 패키지 구조 원칙
헥사고날 기본 컨셉 유지하되 불필요한 depth는 만들지 않는다.
- domain  : 엔티티, 핵심 비즈니스 규칙
- app     : 유스케이스, 서비스
- infra   : Kafka Producer/Consumer, Repository 등 외부 연동
common은 실제 공통 코드가 생길 때만 추가.
port/adapter 인터페이스 분리는 복잡성이 생길 때만 도입.

## Kafka 설정 전략
- Phase 0~3: application.yml + 챕터별 프로파일 분리
  (application-chapter06.yml, application-chapter07.yml 등)
- Phase 4: 모듈별 자체 application.yml로 자연스럽게 분리
- 챕터 시작 시 해당 프로파일을 활성화해서 진행
- 설정 변경 이력이 파일로 남아 챕터 간 비교 가능

## 진행 방식
- 단계별로 개념 설명 → 코드 가이드 제공 → 내가 직접 작성 → 빌드/실행
- 코드/설정 파일(build.gradle.kts, application.yml, docker-compose.yml, Java 소스 등)은
  대신 작성하거나 직접 생성하지 말 것 — 개념/기능 설명과 코드 가이드만 주고 내가 직접 작업
- 각 단계마다 "이걸 왜 이렇게 하는가"를 먼저 설명하고 코드를 제시할 것
- 빌드/실행 결과를 내가 공유하면 그걸 보고 다음 단계로 안내할 것
- 막히면 힌트를 먼저 주고, 요청할 때만 답을 제시할 것
- 개념은 충분히 설명하되, ApplicationEventPublisher와의 차이처럼
  기존에 알고 있는 것과 연결해서 설명할 것
- 각 챕터마다 아래 관점을 함께 다룰 것:
  - 트레이드오프 — 이 선택의 장단점, 언제 다른 선택을 해야 하는지
  - 실무 함정 — 겉으로는 동작하지만 운영에서 문제가 되는 케이스
  - 더 생각해볼 것 — 이 개념에서 자연스럽게 이어지는 다음 질문
  - 안티패턴 — 흔히 잘못 쓰는 방식과 그 이유

## 협업 규칙 (파일/커밋)
- **docs/LOG\*.md, README.md 같은 문서 파일은 예외적으로 내가(Claude) 직접 작성/수정한다.**
  대화 맥락(시행착오, Q&A, 결정 내용)을 이미 갖고 있어서, 다시 손으로 옮겨적게 하는 게 낭비이기 때문.
  코드/설정 파일은 위 "진행 방식" 원칙대로 가이드만 제공.
- **git commit은 사용자가 직접 실행한다.** 커밋 메시지만 draft로 제공하고, `git commit` 명령은 실행하지 않는다.
- **커밋 메시지 컨벤션**: Conventional Commits, 단 type은 제목 줄에만 붙인다.
  ```
  <type>: <한글 제목>

  <자유 서술 한글 본문 — 무엇을 했는지>
  ```
  type은 `feat`/`fix`/`docs`/`chore`/`refactor`/`test` 등 표준 영어 유지, 제목/본문은 한글.
  본문에는 실제로 만든/배운 내용만 쓰고, "LOG 문서 추가", "README 체크리스트 반영" 같은
  루틴한 작업 언급은 생략한다.
- **README.md 진행 상태 체크리스트**는 각 챕터 진행 중에는 건드리지 않고,
  해당 챕터의 LOG 문서가 확정된 시점에만 업데이트한다.

## 문서화 규칙
- docs/ 바로 아래 평평하게 쌓는다. 서브디렉터리 없음.
- 파일명: docs/LOG###-{제목}.md
  (예: LOG000-project-setup.md, LOG006-topic-partition-offset.md)
- README.md는 프로젝트 개괄(스택, 아키텍처, 로컬 실행법)과
  챕터별 진행 상태 체크리스트만 담는다

### LOG 문서 구조 (학습/실습 챕터용)
```
# LOG### — 제목

## 배경 / 목표
챕터 목표, 이전 챕터와의 연결고리(예: "챕터 N에서 예고했던 X를 여기서 검증")

## 개념 정리
이 챕터에서 다루는 핵심 개념. 기존에 아는 것과 연결해서 설명.

## 진행 과정
단계별로: 실행한 명령/코드 → 실제 결과(콘솔 출력, EXPLAIN 결과 등) → **해설**
(왜 이런 결과가 나왔는지). 요약이 아니라 재현 가능한 수준으로 상세히 기록.

## 시행착오 / Q&A
헷갈렸던 지점, 잘못 판단했다가 검증으로 바로잡은 것들을 Q&A 형식으로.

## 트레이드오프 / 실무 함정 / 안티패턴
이번 챕터 내용 기준으로 구체적으로.

## 더 생각해볼 것
다음 챕터로 자연스럽게 이어지는 질문.

## 최종 구성
이 챕터가 끝난 시점에 실제로 코드/스키마/설정에서 뭐가 바뀌었는지 (또는 "관찰만 하고
변경 없음") 한 줄로 명시.

## ADR
Decision / Drivers / Alternatives / Consequences / Follow-ups.
아키텍처 결정뿐 아니라 "이번에 배운 걸 앞으로 어떻게 적용할지"도 포함.
해당 사항이 없는 항목은 생략하지 말고 "해당 없음"이라고 명시.
```

프로젝트 세팅류 문서(LOG000 등, 코드 변경이 없는 인프라 세팅)는 "배경/목표", 상세 "진행 과정"
서술을 생략하고 개념 정리 + 실습 내용 + 시행착오/ADR 정도로 간단히 구성해도 된다.

- 챕터 완료 시 README.md 체크리스트 업데이트 + LOG 문서 작성을 세트로 진행

## 학습 커리큘럼

### Phase 0 — 메시징 기초
1. 동기 통신의 한계 — 강결합, 장애 전파, 응답 대기
2. 비동기 통신과 메시지 브로커의 역할
3. Point-to-Point vs Pub/Sub
4. 메시지 큐 vs 이벤트 스트림 (RabbitMQ vs Kafka 차이)
5. ApplicationEventPublisher와 실제 브로커의 차이
   — in-process vs out-of-process, 영속성, 재처리

### Phase 1 — Kafka 핵심 개념
6. 토픽, 파티션, 오프셋 — Kafka가 메시지를 저장하는 방식
7. Producer 동작 원리 — 배치, acks, 재시도
8. Consumer 동작 원리 — 폴링, 커밋, 오프셋 관리
9. Consumer Group — 파티션 할당, 리밸런싱
10. 이벤트 순서 보장 — 파티션 키 설계

### Phase 2 — Spring Kafka 활용
11. KafkaTemplate + @KafkaListener 기본
12. 직렬화/역직렬화 — JSON, Schema Registry 개념
13. 에러 처리 — 재시도, DLQ 설계
14. Testcontainers로 Kafka 통합 테스트
15. 트랜잭션 — Kafka 트랜잭션과 DB 트랜잭션 조합

### Phase 3 — 핵심 패턴 심화
16. at-least-once vs exactly-once — 트레이드오프
17. 멱등성 설계 — Producer 멱등성, Consumer 멱등성
18. Outbox 패턴 — DB 트랜잭션과 Kafka 발행 원자성
19. Kafka Streams 기초 — 스트림 처리, 윈도우, 집계
20. CQRS + Kafka — 이벤트로 읽기 모델 동기화

### Phase 4 — 실습 예제 (멀티모듈)
21. 이커머스 — 주문/재고/결제/배송/알림 SAGA
22. 물류 추적 — 이벤트 순서 보장, 파티션 키 설계
23. 금융 거래 — 멱등성, exactly-once, 사기 탐지

## 진행 규칙
- 각 챕터: 개념 설명 → 실습 → 결과 확인 → LOG 문서 작성
- 챕터 시작 전 목표와 핵심 개념 먼저 설명
- Phase 0은 코드보다 개념 중심, 간단한 예제로 체감
- Phase 1부터 Kafka 직접 실행하며 동작 확인
- 기존 bank-transfer/practice-db-performance에서 다룬 개념
  (SAGA, Outbox, 멱등성 등)은 "이전에 in-process로 했던 것의 확장"
  관점에서 연결해서 설명할 것

## 현재 상태
- 멀티모듈 전환 완료 (common/learning), Gradle Wrapper 9.6.0
- Docker Compose 구성 완료 (Zookeeper + Kafka + PostgreSQL)
- Phase 0 챕터 1 (동기 통신의 한계) 완료
- 진행 중: Phase 0 챕터 2 (비동기 통신과 메시지 브로커의 역할)
