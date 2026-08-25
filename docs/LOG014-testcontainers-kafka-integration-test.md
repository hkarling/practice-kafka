# LOG014 — Testcontainers로 Kafka 통합 테스트

## 배경 / 목표

Chapter 8부터 13까지 계속 로컬 Docker Compose의 실제 Kafka(`localhost:9092`)에 의존해서
테스트해왔다. 그 결과 LOG008·LOG009·LOG013에서 반복적으로 "leftover(이전 실행에서 커밋
안 된 오프셋이나 남은 메시지)" 문제를 겪었다 — 테스트끼리 같은 브로커와 토픽 상태를
공유하기 때문이다. 이번 챕터는 테스트마다 완전히 격리된 Kafka 브로커를 띄워 그 문제를
구조적으로 없애고, "사람이 로그를 안 봐도 CI에서 재현 가능한" 자동화된 assertion을
갖추는 것이 목표다.

## 개념 정리

- **Testcontainers**: 테스트 실행 시점에 실제 Docker 컨테이너를 띄우고 테스트가 끝나면
  정리해주는 라이브러리. `@EmbeddedKafka`(Spring Kafka 자체 제공, JVM 내 인메모리 브로커)와
  달리 실제 Kafka Docker 이미지를 그대로 쓰기 때문에 프로덕션 환경과의 동작 차이가 적다.
- **이미지별 전용 클래스가 분리되어 있다**: `testcontainers-kafka` 모듈(Testcontainers 2.0)에는
  `org.testcontainers.kafka.KafkaContainer`(`apache/kafka`, `apache/kafka-native` 전용)와
  `org.testcontainers.kafka.ConfluentKafkaContainer`(`confluentinc/cp-kafka` 전용) 두 클래스가
  있다. 둘 다 생성자에서 `DockerImageName.assertCompatibleWith(...)`로 이미지 이름을 검증하기
  때문에, 로컬 `docker-compose.yml`이 쓰는 이미지(`confluentinc/cp-kafka:7.6.1`)와 맞는 클래스를
  써야 한다 — 안 맞으면 컨테이너를 아예 못 만들고 즉시 예외가 난다.
- **Testcontainers 2.0의 아티팩트 이름 변경**: `org.testcontainers:kafka`, `org.testcontainers:junit-jupiter`
  같은 예전 좌표가 `org.testcontainers:testcontainers-kafka`, `org.testcontainers:testcontainers-junit-jupiter`로
  바뀌었다(모든 모듈에 `testcontainers-` 접두사 통일). Spring Boot 4.1.0의 `testcontainers-bom:2.0.5`를
  직접 까봐서 확인했다.
- **`@ServiceConnection`의 실제 동작 범위**: `@Container` 필드에 `@ServiceConnection`을 붙이면
  Spring이 컨테이너의 접속 정보를 담은 `KafkaConnectionDetails` 빈을 자동 등록해준다. 그런데
  이 정보를 실제로 반영하는 코드는 **Spring Boot가 자동 설정하는 `kafkaProducerFactory`/
  `kafkaConsumerFactory` 빈 내부에만** 있다(`KafkaAutoConfiguration.applyKafkaConnectionDetailsForProducer/Consumer`).
  이 프로젝트처럼 `KafkaConfig`에 커스텀 `ProducerFactory`/`ConsumerFactory` 빈을 직접 정의해서
  Spring Boot의 자동 설정 빈이 `@ConditionalOnMissingBean`으로 백오프된 경우, `@ServiceConnection`은
  사실상 아무 효과가 없다 — 아래 "시행착오" 참고.
- **`@DynamicPropertySource`**: `@ServiceConnection`과 달리 `Environment`의 프로퍼티 값
  자체(`spring.kafka.bootstrap-servers`)를 직접 등록한다. 그래서 커스텀 빈이든 자동 설정
  빈이든 `KafkaProperties.buildProducerProperties()`/`buildConsumerProperties()`를 호출하는
  모든 코드가 예외 없이 이 값을 읽어간다.

## 진행 과정

### 1. 의존성 추가

```kotlin
// learning/build.gradle.kts
testImplementation("org.springframework.boot:spring-boot-testcontainers")
testImplementation("org.testcontainers:testcontainers-junit-jupiter")
testImplementation("org.testcontainers:testcontainers-kafka")
```

처음엔 `org.testcontainers:junit-jupiter`, `org.testcontainers:kafka`(접두사 없이)로
시도했는데 `Could not find org.testcontainers:junit-jupiter:.`(빈 버전) 에러가 났다.
Spring Boot 4.1.0이 관리하는 `testcontainers-bom:2.0.5`를 직접 까보니 모든 아티팩트
이름이 `testcontainers-` 접두사로 통일되어 있었다 — 예전 버전 기억으로 잘못 안내했던
부분을 소스로 검증 후 수정했다.

### 2. 컨테이너 클래스 선택

로컬 `docker-compose.yml`이 `confluentinc/cp-kafka:7.6.1`을 쓰고 있어서 버전을
맞추기로 했다. 처음엔 `org.testcontainers.kafka.KafkaContainer`(아까 몰랐던 상태에서
가장 먼저 나오는 클래스)를 그대로 썼다가:

```
Failed to verify that image 'confluentinc/cp-kafka:7.6.1' is a compatible substitute for 'apache/kafka'.
```

예외가 났다. 소스(`KafkaContainer.java`)를 까보니 이 클래스는 생성자에서
`assertCompatibleWith(apache/kafka, apache/kafka-native)`를 강제하고 있었다.
`confluentinc/cp-kafka` 전용 클래스인 `org.testcontainers.kafka.ConfluentKafkaContainer`로
바꿔서 해결했다.

```java
@Container
static ConfluentKafkaContainer kafkaContainer = new ConfluentKafkaContainer("confluentinc/cp-kafka:7.6.1");
```

### 3. `group.id` 누락으로 컨텍스트 기동 실패

```
java.lang.IllegalStateException: No group.id found in consumer config, container properties,
or @KafkaListener annotation; a group.id is required when group management is used.
```

`OrderEventConsumer`(Chapter 6에서 만든, `@Profile` 없이 항상 활성화되는 `@Component`)가
`group-id`를 코드에 명시하지 않고 지금까지는 챕터별 `application-chapterNN.yaml`에서
`spring.kafka.consumer.group-id`를 채워줘서 동작했다. 이번 테스트는 `@ActiveProfiles`가
없어서 아무 프로파일도 안 켜졌고, base `application.yaml`엔 `group-id`가 없어서 기동에
실패했다. 지금까지의 챕터별 프로파일 패턴을 그대로 따라 `application-chapter14.yaml`을
새로 만들고 `@ActiveProfiles("chapter14")`를 붙였다.

```yaml
# application-chapter14.yaml
spring:
  application:
    name: learning
  kafka:
    consumer:
      group-id: chapter14-default-group
      auto-offset-reset: earliest
```

### 4. `@ServiceConnection`이 안 통함 — 로컬 Docker Compose로 메시지가 새어나감

`group.id` 문제를 고치고 재실행했더니 이번엔 assertion이 실패했다:

```
java.lang.IllegalStateException: No records found for topic
```

이상한 점은, 검증용 컨슈머(직접 `kafkaContainer.getBootstrapServers()`로 만든 것)의 로그는
`bootstrap.servers = [localhost:51882]`(컨테이너 포트)를 정확히 가리키는데, Spring이
관리하는 `chapter14-default-group` 리스너(`OrderEventConsumer`)의 로그는
`bootstrap.servers = [localhost:9092]`(**로컬 Docker Compose**)를 가리키고 있었다는
것이다. 게다가 그 리스너는 `fail-dlq-test`, `order-1`, `batch-test-*` 등 Chapter 6~13에서
쌓인 과거 메시지를 그대로 리플레이하고 있었다 — 로컬 Docker Compose Kafka에 붙어있다는
확실한 증거였다.

원인은 `KafkaAutoConfiguration` 소스를 까보고 확인했다. `@ServiceConnection`은
`spring.kafka.bootstrap-servers` **프로퍼티 값 자체를 바꾸지 않는다.** 대신
`KafkaConnectionDetails`라는 빈 하나만 등록하고, 그 값을 실제로 반영하는 코드
(`applyKafkaConnectionDetailsForProducer/Consumer`)는 Spring Boot가 자동 설정하는
`kafkaProducerFactory`/`kafkaConsumerFactory` 빈 안에만 있다:

```java
// KafkaAutoConfiguration.java
@Bean
@ConditionalOnMissingBean(ProducerFactory.class)
DefaultKafkaProducerFactory<?, ?> kafkaProducerFactory(KafkaConnectionDetails connectionDetails, ...) {
  Map<String, Object> properties = this.properties.buildProducerProperties();
  applyKafkaConnectionDetailsForProducer(properties, connectionDetails);  // 여기서만 반영됨
  ...
}
```

이 프로젝트의 `KafkaConfig.java`는 `stringProducerFactory`, `stringConsumerFactory` 등
커스텀 `ProducerFactory`/`ConsumerFactory` 빈을 직접 정의하고 있다(Chapter 8, 12, 13에서
누적). 커스텀 빈이 하나라도 있으면 `@ConditionalOnMissingBean` 때문에 Spring Boot의
자동 설정 빈은 통째로 백오프되고, 커스텀 빈들은 `properties.buildProducerProperties()`만
호출할 뿐 `KafkaConnectionDetails`를 전혀 참조하지 않는다. 결국 `KafkaProperties`의
기본값(`localhost:9092`)으로 떨어졌는데, 하필 로컬 Docker Compose 포트와 똑같아서
"그럭저럭 연결되는 것처럼" 보였던 것이다.

**결론: `@ServiceConnection`은 Spring Boot가 자동 설정하는 기본 Kafka 빈에만 통하고,
커스텀 `ProducerFactory`/`ConsumerFactory`를 직접 만든 프로젝트에는 안 통한다.** 이
프로젝트는 챕터마다 필요한 커스텀 설정(acks=0, batch listener, JSON 역직렬화, DLQ 등)을
`KafkaConfig`에 누적해왔기 때문에 정확히 이 케이스에 해당했다.

`@ServiceConnection`을 걷어내고 `@DynamicPropertySource`로 `spring.kafka.bootstrap-servers`
프로퍼티 값 자체를 등록하는 방식으로 바꿨다. 이 방식은 `Environment` 값을 직접 바꾸므로
`KafkaConnectionDetails` 빈 존재 여부와 무관하게 `KafkaProperties.build*Properties()`를
호출하는 모든 코드(자동 설정 빈이든 커스텀 빈이든)가 예외 없이 반영받는다.

```java
@DynamicPropertySource
static void kafkaProperties(DynamicPropertyRegistry registry) {
  registry.add("spring.kafka.bootstrap-servers", kafkaContainer::getBootstrapServers);
}
```

`KafkaConfig`의 커스텀 빈들을 `KafkaConnectionDetails`를 인지하도록 고치는 대안도
있었지만(5개 빈 전부 수정 필요), 이번 챕터의 스코프를 테스트 코드로 한정하기 위해
테스트 쪽 우회를 선택했다 — 아래 ADR 참고.

### 5. `KafkaTestUtils.consumerProps`의 key deserializer 기본값

검증용 컨슈머를 만들 때 처음엔 `KafkaTestUtils.consumerProps(...)`가 반환한 설정을
그대로 썼는데, 로그에 `key.deserializer = class ...IntegerDeserializer`가 찍혀 있었다.
`KafkaTestUtils.consumerProps`는 예전부터 기본 key deserializer를 `IntegerDeserializer`로
잡아왔다(문자열 키를 쓰려면 항상 명시적으로 덮어써야 한다는 뜻). String 키를 검증해야
하므로 `KEY_DESERIALIZER_CLASS_CONFIG`를 `StringDeserializer`로 명시적으로 덮어썼다.

참고로 `consumerProps(String, String, EmbeddedKafkaBroker)`류의 예전 시그니처와 달리,
현재 버전은 `consumerProps(String brokerAddress, String group, boolean autoCommit)`처럼
autoCommit 파라미터가 `String`이 아니라 `boolean`으로 바뀌어 있었다 — 실습 중 컴파일
에러로 발견하고 맞춰 고쳤다.

### 6. Assertion 추가

원래 이 프로젝트의 모든 테스트(Chapter 8~13)는 `Thread.sleep` + 로그를 눈으로 보는
방식이었다. 그런데 Testcontainers를 쓰는 이유 자체가 "사람이 로그를 안 봐도 CI에서
재현 가능한 자동화 테스트"이기 때문에, 이번 챕터는 `KafkaTestUtils.getSingleRecord`로
직접 컨슈머를 만들어 폴링하고 `assertThat`으로 검증하는 방식으로 처음 도입했다.
`getSingleRecord`는 레코드가 도착하면 즉시 리턴하므로 `Thread.sleep`이 필요 없다.

## 시행착오 / Q&A

**Q. `KafkaContainer`와 `ConfluentKafkaContainer`는 뭐가 다른가?**
A. 둘 다 `testcontainers-kafka` 모듈 안에 있고 내부적으로 KRaft 모드 단일 컨테이너로
Kafka를 띄우는 방식은 같다. 차이는 생성자에서 어떤 벤더 이미지를 허용하도록
하드코딩되어 있느냐뿐이다 — `KafkaContainer`는 `apache/kafka`(-native), `ConfluentKafkaContainer`는
`confluentinc/cp-kafka`. 이미지와 클래스가 안 맞으면 즉시 `IllegalStateException`이 난다.

**Q. `@ServiceConnection`을 붙였는데 왜 여전히 로컬 브로커로 연결됐나?**
A. 이 프로젝트가 `KafkaConfig`에 커스텀 `ProducerFactory`/`ConsumerFactory` 빈을 직접
정의하고 있어서다. `@ServiceConnection`이 반영하는 로직은 Spring Boot의 자동 설정 빈
안에만 있는데, 커스텀 빈이 있으면 그 자동 설정 빈은 아예 안 만들어진다(`@ConditionalOnMissingBean`).
`@DynamicPropertySource`로 프로퍼티 값 자체를 바꾸는 방식만이 커스텀 빈에도 확실히 통한다.

**Q. 왜 `localhost:9092`로 갔는데도 예외 없이 "잘 되는 것처럼" 보였나?**
A. `KafkaProperties`의 기본 `bootstrap-servers` 값이 마침 `localhost:9092`였고, 로컬
Docker Compose Kafka가 정확히 그 포트에 떠 있었기 때문이다. 만약 로컬에 Kafka가 안
떠 있었다면 연결 자체가 실패해서 훨씬 빨리 원인을 알았을 것이다 — "우연히 동작하는
것처럼 보이는" 상황이 오히려 디버깅을 더 헷갈리게 만든 사례.

## 트레이드오프 / 실무 함정 / 안티패턴

**트레이드오프**: Testcontainers는 Docker 데몬이 반드시 필요하다 — Docker 없는 CI
러너에선 실패한다. `@EmbeddedKafka`는 Docker 없이 동작하지만 실제 브로커와 미묘한
차이가 있을 수 있다.

**실무 함정**: 커스텀 `ProducerFactory`/`ConsumerFactory` 빈이 있는 프로젝트에서
`@ServiceConnection`만 믿고 통합 테스트를 짜면, 이번처럼 "로컬 인프라로 조용히
새는" 사고가 날 수 있다. 특히 로컬 개발 환경의 기본 포트(`9092`)와 라이브러리 기본값이
우연히 같으면 예외 없이 동작하는 것처럼 보여서 훨씬 늦게 발견된다. 커스텀 Kafka 빈을
쓰는 프로젝트에서 Testcontainers를 도입할 땐 반드시 `@DynamicPropertySource`로
직접 검증하거나, 커스텀 빈들이 `KafkaConnectionDetails`를 인지하도록 고쳐야 한다.

**안티패턴**: assertion 없이 `Thread.sleep` + 로그 확인만으로 "테스트 통과"라고
판단하는 것. 컨슈머가 메시지를 전혀 못 받아도 예외만 안 나면 초록불로 끝난다 — 이번
챕터에서 실제로 겪은 `@ServiceConnection` 버그도, 만약 assertion 없이 `Thread.sleep`만
있었다면 "로그가 안 보이네" 정도로 넘어가고 로컬 브로커로 메시지가 새는 근본 원인은
못 잡았을 것이다.

## 더 생각해볼 것

`KafkaConfig`의 커스텀 빈 5개(`stringProducerFactory`, `stringConsumerFactory`,
`acksZeroProducerFactory`, `jsonProducerFactory`, `jsonConsumerFactory`)를 전부
`KafkaConnectionDetails`를 인지하도록 고치면, 앞으로 어느 챕터에서 Testcontainers를
다시 쓰더라도 `@ServiceConnection` 한 줄로 끝날 수 있다. 지금은 이번 챕터 스코프를
테스트 코드로 한정했지만, `KafkaConfig`가 계속 커지고 있는 만큼(LOG013 ADR에서도
언급됨) 언젠가 정리가 필요한 시점이다. 또한 이번엔 단발성 produce/consume만
검증했는데, DLQ(Chapter 13)나 배치 리스너(Chapter 11) 같은 이전 챕터의 시나리오를
Testcontainers 기반으로 재현하면 어떤 추가 이슈가 나올지도 궁금하다.

CI/CD 환경에서 Testcontainers를 실제로 태우려면 CI 러너가 Docker 데몬에 접근 가능해야
한다 — GitHub Actions 표준 러너는 기본 제공되어 추가 설정이 거의 필요 없지만, GitLab CI
공유 러너는 `docker:dind` 서비스 구성이, Kubernetes 기반 CI(Tekton 등)는 사이드카 Docker
데몬이나 Testcontainers Cloud 같은 별도 인프라가 필요할 수 있다. Docker 자체를 못 쓰는
제약된 환경에서는 `@EmbeddedKafka`(JVM 내 인메모리 브로커, Docker 불필요)가 대안이 되는데,
다만 실제 브로커와의 정합성은 Testcontainers보다 낮다 — "인프라 의존성 없이 어디서나
돌리기"와 "운영 환경과 최대한 같은 조건에서 검증하기" 사이의 트레이드오프이므로, 실제
CI 플랫폼이 정해지면 그에 맞춰 둘 중 하나(또는 둘 다 상황별로) 선택하면 된다. 나중에
CI/CD 파이프라인을 실제로 구성해볼 기회가 생기면 이 트레이드오프를 직접 체감해보는 것도
좋은 주제가 될 것 같다.

## 최종 구성

`learning/build.gradle.kts`에 `spring-boot-testcontainers`, `testcontainers-junit-jupiter`,
`testcontainers-kafka` 테스트 의존성 추가. `application-chapter14.yaml` 신규
(`group-id: chapter14-default-group`, `auto-offset-reset: earliest`). `TestContainerKafkaTest`
신규 — `ConfluentKafkaContainer`(`confluentinc/cp-kafka:7.6.1`)를 `@Container`로 띄우고,
`@DynamicPropertySource`로 `spring.kafka.bootstrap-servers`를 컨테이너 주소로 등록,
`KafkaTestUtils.getSingleRecord` + `AssertJ`로 produce/consume 왕복을 검증. `KafkaConfig`
등 프로덕션 코드는 변경 없음.

## ADR

### Decision
`@ServiceConnection` 대신 `@DynamicPropertySource`로 `spring.kafka.bootstrap-servers`
프로퍼티를 직접 주입한다. `KafkaConfig`의 커스텀 빈들은 수정하지 않는다.

### Drivers
`@ServiceConnection`이 반영되는 경로(`KafkaAutoConfiguration`의 자동 설정 빈)가 이
프로젝트의 커스텀 `ProducerFactory`/`ConsumerFactory` 빈 때문에 아예 동작하지 않는다는
걸 직접 겪었다. `@DynamicPropertySource`는 `Environment` 프로퍼티 자체를 바꾸는 방식이라
빈 구성과 무관하게 항상 통한다.

### Alternatives
`KafkaConfig`의 커스텀 빈 5개에 `KafkaConnectionDetails`를 주입받아 `bootstrap.servers`를
명시적으로 덮어쓰는 방법도 검토했다. 앞으로 다른 챕터에서도 Testcontainers를 계속 쓸
계획이라면 더 근본적인 해결책이지만, 이번 챕터의 목적(Testcontainers 도입 자체를
검증)에 비해 손대는 범위가 프로덕션 설정 전반으로 넓어져서 이번엔 보류했다.

### Consequences
`KafkaConfig`가 여전히 `KafkaConnectionDetails`를 인지하지 못하는 상태로 남아있다 —
다음에 Testcontainers 기반 테스트를 또 만들 때도 동일하게 `@DynamicPropertySource`를
써야 한다는 뜻이다. `KafkaConfig`가 챕터를 거치며 계속 누적되고 있다는 관찰(LOG013에서도
이미 언급)이 이번에도 재확인됐다.

### Follow-ups
Chapter 15 — 트랜잭션 (Kafka 트랜잭션과 DB 트랜잭션 조합). `KafkaConfig`의 커스텀 빈들을
`KafkaConnectionDetails` 인지하도록 정리할지는 별도로 판단이 필요한 시점.
