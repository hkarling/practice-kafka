# LOG019 — Kafka Streams 기초

## 배경 / 목표

Phase 1(Ch6~10)에서 raw Kafka 클라이언트로 토픽/파티션/오프셋/컨슈머 그룹을 직접 다뤘고,
Phase 2~3(Ch11~18)에서는 Spring Kafka로 그 위에 리스너/트랜잭션/멱등성/Outbox 같은
애플리케이션 패턴을 쌓았다. 이번 챕터는 또 다른 축이다 — Kafka Streams로 "스트림을
연속적으로 처리하고 집계하는" 문제를 다룬다. 목표는 KStream/KTable, 윈도우, 집계라는
기초 개념을 실제로 동작하는 토폴로지로 만들어보고, `TopologyTestDriver`와 실제 Docker
브로커 양쪽으로 검증하는 것이다.

## 개념 정리

### 1. 배경 — 이런 라이브러리가 왜 필요한가

Ch6~10에서 raw Kafka 컨슈머로 폴링하면서 애플리케이션 메모리(`HashMap` 등)에 직접
카운트나 합계를 들고 있는 방식도 만들 수 있었다. 실제로 그렇게 짜면 당장은 동작한다.
하지만 이 방식은 몇 가지 문제를 애플리케이션 코드가 직접 떠안아야 한다.

- **재시작하면 상태가 날아간다**: 메모리에 든 카운트는 프로세스가 죽으면 사라진다.
  처음부터 다시 계산하려면 토픽을 처음부터 재생(replay)해야 하는데, 어디까지 계산했었는지
  추적하는 것도 직접 구현해야 한다.
- **인스턴스를 여러 개로 늘리면 상태가 쪼개진다**: 컨슈머 그룹으로 파티션을 나눠
  처리하면(Ch9), 각 인스턴스는 자기가 담당하는 파티션의 메시지만 본다. "전체 카운트"를
  알려면 인스턴스 간 상태를 어떻게든 합쳐야 하는데, 이걸 직접 구현하려면 별도의 상태 공유
  메커니즘이 필요하다.
- **윈도우, 조인 같은 로직을 직접 구현해야 한다**: "최근 10초간 카운트"처럼 시간 경계를
  다루려면 타임스탬프 관리, 오래된 데이터 정리, 지연 도착 데이터 처리를 전부 손으로 짜야
  한다.
- **정확성 보장(중복 없음, 유실 없음)도 직접 챙겨야 한다**: Ch16~18에서 다룬
  at-least-once/exactly-once, 멱등성, Outbox 같은 고민이 집계 로직에도 똑같이 필요해진다.

Kafka Streams는 이 반복되는 문제들을 라이브러리 레벨에서 표준화해서 풀어준다. 로컬 상태
저장소 + changelog 토픽 조합으로 재시작 후 복구를 자동화하고(개념 5번), 컨슈머 그룹
메커니즘을 그대로 활용해 인스턴스를 늘리면 파티션별로 상태도 함께 재분배되도록 만들고,
윈도우/조인 같은 연산을 DSL로 제공한다. "직접 짜면 매번 반복해서 만들게 되는 스트림 처리
인프라"를 한 번 만들어서 라이브러리로 배포해둔 것이라고 보면 된다.

**어디에 쓰이는가 — 실무 사례**

- **실시간 대시보드/모니터링**: 주문 건수, 결제 금액, 에러율 같은 지표를 몇 초~몇 분
  단위로 집계해서 운영 대시보드에 흘려보내는 용도. 이번 챕터의 "이벤트 타입별 10초 윈도우
  카운트"가 축소판이다.
- **이상 탐지/알림**: "최근 5분간 같은 카드로 결제 시도가 N번 이상"처럼 시간 윈도우 기반
  패턴을 탐지해서 알림을 트리거하는 용도. 금융/보안 도메인에서 흔하다.
- **이벤트 재가공/변환 파이프라인**: 여러 마이크로서비스가 발행한 원시 이벤트를
  필터링·변환·조인해서 다른 토픽으로 다시 발행하는 중간 처리 계층. Kafka Connect가
  "외부 시스템과의 연동"에 집중한다면, Streams는 "토픽과 토픽 사이의 로직"에 집중한다.
- **CQRS 읽기 모델 구축**: 이벤트 스트림을 구독해서 조회에 최적화된 뷰(집계, 요약, 검색
  인덱스)를 만드는 용도 — 다음 챕터(Ch20)에서 다룰 주제이고, 이번 챕터의 KTable이 그
  축소판 사례다.
- **세션 기반 사용자 행동 분석**: Session window로 "한 세션 동안 사용자가 한 행동"을
  묶어서 분석하는 용도(예: 이커머스 장바구니 이탈 분석).

**Kafka 생태계 안에서의 위치**

같은 "스트림 처리" 문제를 푸는 도구가 여럿 있는데, 무게감이 다르다.

| | Kafka Streams | ksqlDB | Kafka Connect | Flink/Spark Streaming |
|---|---|---|---|---|
| 형태 | 애플리케이션에 내장되는 라이브러리 | SQL로 스트림 처리, 별도 서버 필요 | 외부 시스템 연동 전용(변환은 최소) | 별도 분산 클러스터, 더 강력하지만 무거움 |
| 별도 인프라 | 불필요 | ksqlDB 서버 필요 | Kafka Connect 클러스터 필요 | 별도 클러스터 필요 |
| 적합한 상황 | 애플리케이션 안에서 가볍게 스트림 처리/집계가 필요할 때 | SQL만으로 빠르게 스트림 쿼리를 만들고 싶을 때 | DB/외부 API와 Kafka를 연결만 하고 싶을 때(Ch18의 CDC 논의와 연결) | 대규모, 복잡한 분산 스트림 처리가 필요할 때 |

이번 챕터에서 Kafka Streams를 고른 이유는 별도 인프라 없이, 지금 있는 `learning`
애플리케이션 안에 의존성 하나만 추가해서 바로 스트림 처리 개념을 실습할 수 있기
때문이다.

### 2. Kafka Streams란 무엇인가

Kafka Connect나 ksqlDB와 달리 별도 클러스터가 필요 없는 **라이브러리**다. 애플리케이션
프로세스 안에서 Consumer로 읽고, 로컬 상태 저장소(RocksDB 또는 in-memory)에 중간 상태를
유지하며, 필요하면 Producer로 다시 쓴다. 이번 챕터에서 `learning` 애플리케이션 안에
`kafka-streams` 의존성만 추가해서 그대로 넣을 수 있었던 이유다.

### 3. KStream vs KTable — 이벤트 로그 vs 최신 상태 뷰

- **KStream**: 매 레코드가 독립적인 사실이다. 같은 키로 레코드가 여러 번 들어와도 다
  별개의 이벤트로 취급된다.
- **KTable**: 키별 최신 값만 유지하는 뷰다. 같은 키로 새 레코드가 오면 이전 값을 덮어쓰며,
  내부적으로 압축된(compacted) changelog 토픽에 대응된다.

Ch6에서 다룬 "토픽은 로그다"의 연장선이다 — KStream이 로그 원본이면, KTable은 그 로그를
계속 리플레이해서 만든 "지금 이 순간의 스냅샷"이다.

### 4. Windowing — 무한한 스트림을 어떻게 잘라서 집계하는가

- **Tumbling window**: 겹치지 않는 고정 크기 구간(0~10초, 10~20초, ...). 이번 챕터에서
  `TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(10))`로 쓴 방식이다.
- **Hopping window**: 겹치는 슬라이딩 구간. 같은 이벤트가 여러 윈도우에 중복 집계될 수 있다.
- **Session window**: 고정 크기가 아니라 비활동 갭으로 경계를 잡는 방식.

### 5. Aggregation과 내구성

`groupBy(...)` 후 `count()`/`aggregate()`/`reduce()`를 호출하면 결과가 로컬 상태 저장소에
쌓인다. 이 상태 변화는 내부 changelog 토픽에도 함께 기록되어, 인스턴스가 죽어도 복구할 수
있다 — Ch7~9의 "브로커가 진실의 원천"이라는 원칙이 Streams 내부에서도 유지되는 셈이다.
단, 이번 챕터에서 로컬 상태 저장소가 정상적으로 복구되지 않는 상황을 직접 겪었는데,
그 내용은 "진행 과정"과 "실무 함정"에 상세히 남겼다.

### 6. 왜 `TopologyTestDriver`로 테스트하는가

Kafka Streams는 토폴로지(처리 로직)와 실행 인프라가 분리되어 있어서, `TopologyTestDriver`로
브로커 없이 토폴로지 로직만 단위 테스트할 수 있다. 가상의 시각(timestamp)을 직접 주입해서
"이 시각에 이 레코드가 들어오면 윈도우 집계가 어떻게 되는가"를 결정론적으로 검증할 수
있다는 게 핵심이다.

### 7. KTable은 "확정된 결과"가 아니라 매 업데이트마다 emit한다

윈도우 집계 결과를 스트림으로 내보내면(`toStream()`), 카운트가 1이 될 때, 2가 될 때마다
각각 별도 레코드로 emit된다. "최종 확정된 값만 보고 싶다"면 `suppress(Suppressed
.untilWindowCloses(...))`가 필요한데, 이번 챕터에서는 다루지 않고 "더 생각해볼 것"에
남겨뒀다.

### 8. 로컬 캐시와 `commit.interval.ms` — 집계 결과는 즉시 나가지 않는다

이번 챕터에서 직접 겪으며 확인한 개념이다. Kafka Streams는 KTable 집계 결과를 매번 즉시
다운스트림(출력 토픽)으로 흘려보내지 않고, 로컬 캐시(`statestore.cache.max.bytes`, 기본
10MB)에 버퍼링했다가 `commit.interval.ms`(기본 30초, `at_least_once` 기준)마다 또는
캐시가 다 차면 flush한다. 실제 트래픽에서 매 업데이트마다 출력 토픽에 쓰는 비용을 줄이기
위한 최적화다. "진행 과정" 9단계에서 이 지연을 직접 겪었고, `STATESTORE_CACHE_MAX_BYTES_CONFIG`를
0으로 낮춰 캐시를 끄면 매 업데이트가 즉시 반영되는 것도 확인했다.

## 진행 과정

### 1. 의존성 추가

```kotlin
// learning/build.gradle.kts
implementation("org.apache.kafka:kafka-streams")
```

### 2. `KafkaTopics`에 토픽 추가

```java
public static final String ORDER_EVENTS_STREAMS = "order-events-streams";
public static final String ORDER_EVENT_COUNTS = "order-event-counts";
```

### 3. `OrderEventCountTopology` — 토폴로지 정의

```java
public static Topology build() {
  StreamsBuilder builder = new StreamsBuilder();
  KStream<Object, Object> events = builder.stream(KafkaTopics.ORDER_EVENTS_STREAMS);

  KTable<Windowed<Object>, Long> counts = events
      .groupBy((orderId, eventType) -> eventType)
      .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(10)))
      .count();

  counts.toStream()
      .map((windowedKey, count) -> KeyValue.pair(
          windowedKey.key() + "@" + windowedKey.window().start(),
          String.valueOf(count)))
      .to(KafkaTopics.ORDER_EVENT_COUNTS, Produced.with(Serdes.String(), Serdes.String()));

  return builder.build();
}
```

`Windowed<String>` 키를 그대로 출력 토픽에 쓰려면 `WindowedSerdes` 등록이 별도로 필요해서,
`"이벤트타입@윈도우시작시각"` 문자열로 단순화했다 — 의도적인 절충이다.

### 4. `TopologyTestDriver` 테스트 — 첫 실패와 원인

`setUp()`에 `APPLICATION_ID_CONFIG`/`BOOTSTRAP_SERVERS_CONFIG`만 넣고 처음 돌렸더니
`ConfigException: Please specify a key serde or set one through StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG`가
났다. `groupBy`/`count()`가 만드는 내부 상태 저장소(`KSTREAM-AGGREGATE-STATE-STORE-...`)는
토폴로지 안에서 명시적으로 Serde를 지정하지 않았기 때문에 `StreamsConfig`의 기본 Serde
설정에 의존한다 — 그게 없어 `TopologyTestDriver` 생성 시점에 상태 저장소 초기화가
실패했다. 아래 두 줄을 추가해서 해결했다.

```java
props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
```

이후 두 테스트(같은 윈도우 누적 집계, 윈도우 경계 분리 집계) 모두 `BUILD SUCCESSFUL`로
통과했다.

### 5. `OrderEventCountStreamsConfig` — 실행용 빈, 그리고 첫 기동 실패

```java
@Configuration
@Profile("chapter19")
public class OrderEventCountStreamsConfig {

  @Bean(destroyMethod = "close")
  public KafkaStreams orderEventCountStreams(KafkaProperties properties) {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "chapter19-order-event-count");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getBootstrapServers());
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

    KafkaStreams streams = new KafkaStreams(OrderEventCountTopology.build(), props);
    streams.start();
    return streams;
  }
}
```

처음 `application-chapter19.yaml`에 `spring.application.name`만 두고 기동했더니
`UnsatisfiedDependencyException: ... No qualifying bean of type 'JdbcTemplate' available`로
실패했다. `learning` 모듈에는 `spring-boot-starter-jdbc` + PostgreSQL 드라이버가 항상
클래스패스에 있는데, chapter19 프로파일엔 datasource 설정이 전혀 없어서 `DataSourceAutoConfiguration`이
데이터소스를 못 만든 것이다.

여기서 `spring.autoconfigure.exclude`로 `DataSourceAutoConfiguration`을 꺼보려는
시도가 있었으나 잘못된 접근이었다 — 이유는 "시행착오 / Q&A"에 정리했다. 최종적으로는
다른 챕터와 동일하게 datasource 설정을 그대로 넣는 쪽으로 해결했다.

```yaml
spring:
  application:
    name: learning
  datasource:
    url: jdbc:postgresql://localhost:5432/practice_kafka
    username: kafka
    password: kafka
  kafka:
    consumer:
      group-id: chapter19-default-group
      auto-offset-reset: earliest
```

`sql.init` 설정은 넣지 않았다 — Chapter 19는 새 테이블이 필요 없고, `schema-locations`를
지정하지 않으면 기존 테이블에 아무 영향도 주지 않는다.

### 6. 두 번째 기동 실패 — `group.id` 누락

datasource를 고친 뒤 다시 기동하니 이번엔 `IllegalStateException: No group.id found in
consumer config, container properties, or @KafkaListener annotation`로 실패했다.
`io.hkarling.learning.kafka.OrderEventConsumer`(Ch6부터 있던, `@Profile`이 안 붙은
항상-켜져있는 리스너)가 `spring.kafka.consumer.group-id`에 의존하는데 위 yaml 초안에는
이 값이 없었다. `kafka.consumer.group-id: chapter19-default-group`을 추가해서 해결했다
(위 최종 yaml에 반영됨).

### 7. 세 번째 기동 실패 — 소스 토픽 없음

`group.id`를 고친 뒤 애플리케이션은 떴지만, `order-events-streams` 토픽에 아직 아무도
쓰지 않아 토픽 자체가 없었다. Kafka Streams는 소스 토픽을 자동 생성하지 않는다 —
`MissingSourceTopicException: Missing source topics. [order-events-streams]`가 나면서
스트림 클라이언트가 `SHUTDOWN_CLIENT`로 종료됐다. Ch6에서 `order-events` 토픽을 미리
만들어야 했던 것과 같은 이유다. 다음 명령으로 입력/출력 토픽을 미리 만들어서 해결했다.

```
docker compose exec kafka kafka-topics --create --topic order-events-streams --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092
docker compose exec kafka kafka-topics --create --topic order-event-counts --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092
```

### 8. 실제 Docker Kafka로 검증 — 출력이 비어있던 문제

토픽까지 만든 뒤 `kafka-console-producer`로 `order-1:ORDER_CREATED`를 8번 입력하고
바로 `kafka-console-consumer --from-beginning`으로 `order-event-counts`를 확인했더니
아무것도 안 나왔다. 개념 정리 8번에서 설명한 캐시/`commit.interval.ms`(기본 30초) 때문이었다
— flush가 아직 안 된 것뿐이었다. 확인을 위해 `OrderEventCountStreamsConfig`에
`STATESTORE_CACHE_MAX_BYTES_CONFIG=0`을 임시로 추가해 캐시를 끄니, 업데이트가 즉시
출력 토픽에 반영되는 걸 확인했다. 이후 다시 기본값(캐시 켠 상태)으로 되돌리고 30초를
기다려서, 정확히 30초 뒤에 결과가 찍히는 것도 재확인했다 — 캐싱 개념을 양쪽 다 실증한
셈이다. 최종 코드는 운영에 가까운 기본값(캐시 오버라이드 없음)으로 남겼다.

### 9. 재기동을 반복하며 카운트가 뒤섞인 현상

앞선 세 번의 기동 실패를 디버깅하며 애플리케이션을 여러 번 재시작했다. 그 과정에서
`order-event-counts`를 확인해보니 카운트가 1부터 여러 번 다시 시작하는 것처럼 보이는
구간들이 섞여 나왔다. 원인은 로그에 반복해서 찍혔던 다음 경고와 관련이 있다.

```
WARN ... Using an OS temp directory in the state.dir property can cause failures with writing the checkpoint file...
ERROR ... Failed to change permissions for the directory C:\Users\USER\AppData\Local\Temp\kafka-streams\...
```

`state.dir`이 OS 임시 디렉터리를 쓰고 있어서 재기동 때마다 로컬 RocksDB 상태 저장소가
온전히 복구되지 않았을 가능성, 그리고 IDE의 강제 종료로 `KafkaStreams`가 정상 종료 절차를
못 밟아 `processing.guarantee = at_least_once` 하에서 입력 오프셋 커밋 전에 죽어 재기동
시 같은 메시지를 재처리했을 가능성이 겹친 것으로 판단했다. 토폴로지 로직 자체의 결함은
아니었다 — 한 세션 안에서는 카운트가 꾸준히 증가하는 걸 확인했다. 토픽을 지우고 다시
만든 뒤 앱을 한 번만 기동해서 처음부터 끝까지 깔끔한 카운트 증가를 재확인했다.

### 10. `application.yaml`에 잘못 들어간 프로파일 하드코딩 정리

디버깅 도중 편의를 위해 모든 챕터가 공유하는 기본 `application.yaml`에
`spring.profiles.active: chapter19`를 넣어뒀던 것을 뒤늦게 `git status`로 발견했다. 이대로
두면 이후 다른 챕터를 실행할 때도 기본값으로 `chapter19`가 계속 활성화되는 문제가 있어
제거했다. 챕터 프로파일은 항상 IDE의 Active profiles 필드나 커맨드라인에서 그때그때
지정하는 방식을 유지한다.

## 시행착오 / Q&A

**Q. `DataSourceAutoConfiguration`을 `spring.autoconfigure.exclude`로 꺼서 Postgres 없이
Chapter 19를 기동하려던 시도는 왜 잘못됐나?**
A. `IdempotentOrderLogRepository`(Ch17), `OrderRepository`(Ch15), `OutboxEventRepository`
(Ch18) 같은 리포지토리들은 `@Profile`이 붙어있지 않고 그냥 `@Repository`라서, 어떤
프로파일이 활성화되든 컴포넌트 스캔으로 항상 빈이 등록된다. `DataSourceAutoConfiguration`을
꺼버리면 `JdbcTemplate` 빈 자체가 없어져서 이 리포지토리들이 기동 자체를 막는다. 이
프로젝트 구조에서는 "이 챕터는 DB를 안 쓴다"는 이유만으로 자동 설정을 끄는 게 항상
가능하진 않다는 걸 확인했다 — 다른 챕터와 마찬가지로 datasource 설정을 그냥 넣어주는
쪽이 이 코드베이스와 맞았다.

**Q. `MissingSourceTopicException`이 나면 애플리케이션이 왜 완전히 죽는가?**
A. `LogAndFailExceptionHandler`/기본 예외 처리 정책상 소스 토픽이 없으면 `SHUTDOWN_CLIENT`로
판단해 스트림 클라이언트를 종료한다. 이후 Spring 컨텍스트가 나머지 라이프사이클 빈을 마저
띄우려다 `OrderEventConsumer`의 `group.id` 누락 문제까지 겹쳐서 전체 컨텍스트 기동이
실패하는 것처럼 보였다 — 실제로는 서로 다른 두 개의 원인이 순차적으로 드러난 것이었다.

**Q. 캐시를 껐을 때(`STATESTORE_CACHE_MAX_BYTES_CONFIG=0`)와 기본값일 때 실제로 차이가
보이나?**
A. 그렇다. 캐시를 끄니 `order-1:ORDER_CREATED`를 입력할 때마다 카운트(1, 2, 3, ...)가
즉시 출력 토픽에 찍혔다. 기본값(캐시 켬)으로 되돌리고 나서는 입력 후 약 30초 뒤에야
결과가 나타났다 — `commit.interval.ms` 기본값과 정확히 일치했다.

## 트레이드오프 / 실무 함정 / 안티패턴

**트레이드오프**: 캐싱(`statestore.cache.max.bytes`)은 다운스트림 쓰기 횟수를 줄여
처리량을 높이지만, 그만큼 결과 반영이 `commit.interval.ms`만큼 지연된다. 실시간성이
중요한 도메인이면 캐시 크기를 줄이거나 커밋 간격을 좁혀야 하는데, 그러면 처리량 이점이
줄어든다 — 이번 챕터에서 캐시를 0으로 낮춰 즉시 반영을 확인한 것 자체가 이 트레이드오프의
극단적인 예시다.

**실무 함정**: `state.dir`을 OS 임시 디렉터리(기본값)로 두면 안 된다 — 이번 챕터에서
실제로 겪었듯 OS가 임시 파일을 정리하면서 체크포인트 파일 쓰기에 실패할 수 있고, 재기동
시 로컬 상태 복구가 불안정해진다. 운영에서는 `STATE_DIR_CONFIG`로 안정적인 영구 경로를
명시적으로 지정해야 한다. 또한 `processing.guarantee = at_least_once`(기본값) 상태에서
비정상 종료(강제 kill)가 반복되면 입력 오프셋 커밋 전에 죽어 재기동 시 같은 메시지를
재처리할 수 있다 — graceful shutdown(`KafkaStreams.close()`가 정상 호출되는 경로)이
중요한 이유다. `@Bean(destroyMethod = "close")`로 애플리케이션 정상 종료 시엔 이 경로를
타지만, IDE에서 강제로 프로세스를 죽이면 이 보장이 깨진다.

**실무 함정**: 이 프로젝트의 리포지토리들(`OrderRepository`, `IdempotentOrderLogRepository`,
`OutboxEventRepository`)이 `@Profile`로 스코프되지 않고 항상 컴포넌트 스캔되는 구조라는
걸 이번 챕터에서 확인했다. 이 자체가 잘못은 아니지만, "이 챕터는 DB를 안 쓴다"고 해서
datasource 설정을 아예 생략할 수 없다는 결합(coupling)을 만든다 — 새 챕터를 추가할 때마다
암묵적으로 datasource가 필요하다는 걸 기억해야 한다.

**안티패턴**: 여러 챕터가 공유하는 기본 `application.yaml`에 특정 챕터의 프로파일을
`spring.profiles.active`로 하드코딩하는 것. 디버깅 편의로 잠깐 넣어두기 쉽지만, 지우는
걸 잊으면 이후 다른 챕터의 실행/테스트가 의도치 않게 그 프로파일로 묶여 돈다. 이번
챕터에서 실제로 이 실수를 했다가 `git status`로 뒤늦게 발견하고 정리했다.

## 더 생각해볼 것

- `suppress(Suppressed.untilWindowCloses(...))`로 윈도우가 완전히 닫힌 최종 결과만 emit하는
  방법 — 이번 챕터는 매 업데이트가 다 나가는 기본 동작만 다뤘다.
- `Windowed<String>` 키를 문자열로 단순화한 부분을 `WindowedSerdes.timeWindowedSerdeFrom(...)`로
  제대로 직렬화하는 방법.
- 여러 인스턴스로 스케일아웃할 때 상태 저장소가 파티션별로 어떻게 재분배(rebalance)되고,
  그 시간 동안 처리가 어떻게 지연되는지.
- Ch18 Outbox의 CDC 논의와 마찬가지로, Kafka Streams도 상태를 changelog 토픽에 의존한다는
  점에서 "로컬 상태 + 원격 내구성"이라는 같은 패턴이 반복된다는 것.
- Chapter 20 CQRS + Kafka에서, 이번 챕터의 집계 결과(KTable)가 읽기 모델 동기화에 어떻게
  쓰일 수 있는지.

## 최종 구성

`learning/build.gradle.kts`에 `org.apache.kafka:kafka-streams` 의존성 추가. `KafkaTopics`에
`ORDER_EVENTS_STREAMS`, `ORDER_EVENT_COUNTS` 추가. `io.hkarling.learning.streams` 패키지
신규 — `OrderEventCountTopology`(정적 토폴로지 빌더), `OrderEventCountStreamsConfig`
(`@Profile("chapter19")`, `KafkaStreams` 빈). 테스트 `OrderEventCountTopologyTest`
(`TopologyTestDriver` 기반, 같은 윈도우 누적 집계 / 윈도우 경계 분리 집계 2개 `@Test`).
`application-chapter19.yaml` 신규(datasource, chapter19 group-id). 기본 `application.yaml`은
디버깅 중 잘못 추가됐던 `spring.profiles.active` 하드코딩을 제거하고 원상 복구.
`KafkaConfig`는 변경 없음.

## ADR

### Decision
Kafka Streams 기초 토폴로지(이벤트 타입별 10초 윈도우 카운트)를 순수 Kafka Streams DSL
(Spring 통합 없이 `StreamsBuilder`/`KafkaStreams` 직접 사용)로 구현한다. 토폴로지 로직은
`TopologyTestDriver`로, 실제 발행/집계/지연 동작은 로컬 Docker 브로커로 각각 검증한다.

### Drivers
Ch6~10에서 다진 raw Kafka 클라이언트 이해를 이어받아, Spring Kafka의 `@KafkaListener`
추상화 없이 Streams DSL을 있는 그대로 다뤄보는 게 "기초" 챕터의 목표에 맞다고 판단했다.

### Alternatives
Spring for Apache Kafka의 `@EnableKafkaStreams` 통합을 검토했으나, 이번 챕터는 Streams
자체의 개념(KStream/KTable, 윈도우, 캐싱)을 순수하게 다루는 게 우선이라 Spring 통합 레이어를
얹지 않기로 했다. `Windowed<String>` 키를 위한 `WindowedSerdes` 도입도 검토했으나, 출력
토픽 키를 문자열로 단순화하는 쪽이 기초 단계에는 더 명확하다고 판단해 보류했다.

### Consequences
`state.dir`을 OS 임시 디렉터리로 방치한 채로 여러 번 재기동하면서 로컬 상태 복구가
불안정해지는 걸 직접 겪었다 — 프로덕션에서는 반드시 영구 경로를 명시해야 한다는 교훈을
얻었다. 리포지토리들이 프로파일 스코프되지 않은 기존 구조 때문에, DB를 쓰지 않는 챕터도
datasource 설정을 생략할 수 없다는 결합이 있다는 것도 확인했다 — 이번 챕터 범위에서는
그냥 datasource를 넣는 것으로 해결했고, 구조 자체를 바꾸지는 않았다.

### Follow-ups
Chapter 20 — CQRS + Kafka. 이번 챕터의 KTable 집계가 읽기 모델 동기화 논의에 다시
등장할 가능성이 있다.
