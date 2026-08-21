# LOG000 — 프로젝트 셋업

## 개념 정리

### 멀티모듈 Gradle 구조
서비스별로 독립 실행 가능한 모듈을 만들되, 공유 코드(이벤트 스키마, 공통 설정)는 `common` 모듈에 둔다. 루트 `build.gradle.kts`는 서브모듈에 공통 적용할 최소 설정만 갖거나, 아예 비워두고 각 모듈이 자기 `build.gradle.kts`에서 필요한 플러그인/의존성을 직접 선언한다. 서비스 간 결합을 낮추기 위한 선택이며, Phase 4에서 `ecommerce-order`, `ecommerce-inventory` 같은 모듈들이 서로 직접 참조 없이 Kafka로만 통신하게 만들기 위한 사전 준비다.

### Docker Compose 프로젝트와 컨테이너의 관계
`docker-compose.yml` 파일 하나가 여러 개의 **독립된 컨테이너**를 정의한다. `docker compose up`을 실행하면 정의된 서비스 수만큼 컨테이너가 각각 뜨고, Compose가 자동으로 만든 공용 네트워크 안에서 서비스 이름(`zookeeper`, `kafka`)을 호스트명처럼 사용해 서로 통신한다.

Docker Compose는 컨테이너를 만들 때 `com.docker.compose.project`(프로젝트명, 기본값은 compose 파일이 있는 디렉터리 이름), `com.docker.compose.service`(서비스명) 라벨을 붙인다. Docker Desktop 대시보드는 이 라벨을 기준으로 컨테이너를 그루핑해서 폴더처럼 접었다 펼 수 있게 보여줄 뿐, 실제로 컨테이너가 하나로 합쳐지는 건 아니다. `docker ps` (CLI)로 보면 그루핑 없이 flat하게 3개가 그대로 보인다.

### Kafka 로컬 구성 (Zookeeper 기반)
KRaft 모드(Zookeeper 제거)가 최신 Kafka의 기본 방향이지만, Zookeeper의 역할을 체감하기 위해 이번 학습에서는 명시적으로 Zookeeper를 붙인 구성을 사용한다. `KAFKA_ADVERTISED_LISTENERS`로 브로커가 자신을 어떤 주소로 광고할지 리스너별로 분리하는 게 핵심이다 — 컨테이너 밖(호스트)에서 접속하는 Spring Boot 앱은 `localhost:9092`를, 컨테이너 내부(Zookeeper와의 통신 등)는 `kafka:29092`를 쓴다.

## 실습 내용

1. `settings.gradle.kts`에 `include("common")`, `include("learning")` 추가해 멀티모듈 전환
2. `learning` 모듈 생성 — Spring Boot 4.1.0 + `spring-boot-starter-kafka` + `spring-boot-starter-webmvc`, Java 21 toolchain
3. `common` 모듈은 디렉터리만 생성, 실제 공유 코드가 생기면 그때 채우기로 결정 (현재 스켈레톤)
4. 루트 `build.gradle.kts`는 삭제 — 지금은 서브모듈 공통 설정이 필요 없어서 비워둠, 필요해지면 다시 추가
5. `docker-compose.yml` 작성 — Zookeeper + Kafka + PostgreSQL 3개 컨테이너
6. `README.md` 작성 — 스택/아키텍처/로컬 실행법 + Phase 0~4 챕터 진행 체크리스트

## 시행착오

### Gradle Wrapper 파일 유실
멀티모듈 리스트럭처링 과정에서 `gradle/wrapper/gradle-wrapper.jar`와 `gradlew.bat`이 삭제된 채로 남아있었다. IntelliJ에서는 빌드/실행이 정상으로 보였는데, 이는 IntelliJ가 wrapper 프로토콜을 자체 구현해서 `gradle-wrapper.properties`만 읽고 명시된 Gradle 버전(9.6.0)을 알아서 받아 쓰기 때문이었다 — 즉 IDE는 `gradlew` 스크립트나 wrapper jar 없이도 동작한다. 실제로 터미널에서 `./gradlew --version`을 실행해보니 `ClassNotFoundException: org.gradle.wrapper.GradleWrapperMain`으로 명확히 깨져 있었다.

**해결**: 로컬에 캐시되어 있던 Gradle 9.6.0 배포판(`~/.gradle/wrapper/dists/gradle-9.6.0-bin/.../gradle-9.6.0`)을 직접 실행해서 `gradle wrapper --gradle-version 9.6.0`으로 wrapper jar와 `gradlew.bat`을 재생성했다.

**교훈**: IDE에서 빌드가 된다고 CLI에서도 되는 게 보장되지 않는다. 특히 CI나 다른 개발자 환경, 또는 이 세션처럼 터미널 기반으로 협업하는 경우 wrapper 파일 자체를 반드시 커밋해야 하고, 주기적으로 `./gradlew`가 실제로 동작하는지 CLI에서 확인할 필요가 있다.

### 존재하지 않는다고 착각했던 의존성 아티팩트
`spring-boot-starter-kafka`, `spring-boot-starter-webmvc`를 처음 봤을 때 "이런 아티팩트는 없다, `spring-kafka`/`spring-boot-starter-web`이 맞는 이름"이라고 판단했다. 이는 Spring Boot 3.x 시절의 네이밍 관례에 기반한 잘못된 판단이었고, 실제로 `gradle :learning:dependencies --configuration compileClasspath`와 `:learning:build`를 로컬 Gradle 9.6.0으로 직접 실행해서 두 아티팩트 모두 Spring Boot 4.1.0에서 정식으로 resolve되고, 빌드/테스트까지 성공하는 것을 확인했다.

**교훈**: 프레임워크 버전이 빠르게 바뀌는 상황에서는 기억이나 관례에 의존하지 말고, 실제로 의존성 트리를 resolve해보거나 빌드를 돌려서 검증하는 게 안전하다.

### 시스템 Gradle 버전 불일치
시스템에 설치된 Gradle(8.6)로 직접 빌드를 시도했더니 `Spring Boot plugin requires Gradle 8.x (8.14 or later) or 9.x. The current version is Gradle 8.6` 에러가 발생했다. `gradle-wrapper.properties`가 명시한 버전(9.6.0)과 시스템 Gradle 버전이 다를 수 있다는 걸 보여주는 사례 — wrapper를 쓰는 이유가 바로 이런 버전 불일치를 막기 위함이다.

## Q&A

**Q. `docker compose` 대신 `spring-boot-docker-compose`(Spring Boot의 Docker Compose 지원 모듈)를 쓰는 게 낫지 않나?**

A. 이 프로젝트에는 맞지 않는다. `spring-boot-docker-compose`는 애플리케이션 하나가 자신의 compose 파일을 갖고, 그 앱의 생명주기에 맞춰 컨테이너를 올리고 내리는 걸 전제로 한다. 반면 이 프로젝트는 Phase 4부터 여러 독립 모듈(order/inventory/payment 등)이 **같은** Kafka/PostgreSQL을 공유해야 하는데, 한 모듈이 종료될 때 공유 인프라까지 함께 내려가버리는 문제가 생긴다. 또한 `spring-boot-docker-compose`는 connection 정보(`spring.kafka.bootstrap-servers` 등)를 자동 주입하는데, 이 프로젝트는 챕터별 `application-chapterNN.yml`에 설정값을 명시적으로 남겨 비교하는 게 목표라 자동 설정이 오히려 방해가 된다. → 루트에서 `docker compose`로 한 번 띄워두고 각 모듈이 거기 붙는 방식을 채택했다.

**Q. Kafka, Zookeeper, PostgreSQL을 컨테이너 하나로 합쳐서(단일 이미지 빌드) 올리는 게 낫지 않나?**

A. 개별 컨테이너로 유지하는 게 낫다. 이후 챕터(예: Consumer Group 리밸런싱)에서 "Kafka 브로커만 재시작해서 리밸런싱 관찰하기" 같은 실습이 필요한데, 하나의 이미지에 여러 서비스를 몰아넣으면 서비스 단위로 독립적인 재시작이 불가능해진다(전체를 같이 내리거나 `supervisord` 같은 프로세스 매니저가 필요). 또한 공식 이미지(`confluentinc/*`, `postgres`)를 그대로 쓰는 게 유지보수 측면에서도 유리하고, 실제 운영 환경의 서비스별 독립 프로세스 토폴로지와도 맞는다.

## ADR

### ADR-1: 멀티모듈 Gradle 구조 채택
- **Decision**: `common`/`learning` 멀티모듈로 시작, Phase 4부터 도메인별 모듈 추가
- **Drivers**: 각 서비스가 독립 실행 가능해야 함, Kafka 통신만으로 결합해야 함
- **Alternatives**: 단일 모듈에 패키지로만 분리 — 실행 단위 분리가 안 돼서 "독립 배포 가능한 서비스"라는 학습 목표와 맞지 않아 기각
- **Consequences**: 모듈이 늘어날수록 보일러플레이트(build.gradle.kts 반복)가 늘어남 — 필요해지면 루트에 `subprojects {}` 공통 설정 재도입 검토
- **Follow-ups**: `common` 모듈 build.gradle.kts는 실제 공유 코드가 생길 때 작성

### ADR-2: Docker Compose(Zookeeper 기반) 직접 관리, spring-boot-docker-compose 미사용
- **Decision**: 루트 `docker-compose.yml`을 수동으로 `docker compose up -d`로 띄워서 여러 모듈이 공유
- **Drivers**: Phase 4의 다중 모듈이 인프라를 공유해야 함, 챕터별 설정값을 명시적으로 비교하고 싶음
- **Alternatives**: `spring-boot-docker-compose` — 앱 생명주기와 인프라 생명주기가 묶여버려서 기각
- **Consequences**: 인프라를 앱과 별개로 수동 관리해야 함(장점이자 단점) — 실습 시작 전 `docker compose up -d`를 직접 실행해야 함
- **Follow-ups**: Phase 4 진입 시 여러 모듈이 실제로 같은 인프라를 문제없이 공유하는지 재검증

### ADR-3: 인프라 컨테이너 개별 실행 (단일 통합 이미지 미사용)
- **Decision**: Zookeeper, Kafka, PostgreSQL을 각각 별도 컨테이너로 구성
- **Drivers**: 서비스 단위 독립 재시작/로그 확인 필요, 공식 이미지 활용
- **Alternatives**: 하나의 커스텀 이미지에 통합 — Consumer Group 리밸런싱 등 브로커 단독 재시작이 필요한 실습과 충돌해서 기각
- **Consequences**: 컨테이너 개수가 늘어나지만 리소스 오버헤드는 로컬 학습 환경에서 체감되지 않는 수준
- **Follow-ups**: 없음

## 트레이드오프 / 실무 함정 / 안티패턴

**트레이드오프**
- 명시적 `docker compose` 관리(현재 선택)는 인프라 제어권을 얻는 대신, 실습 시작 전 수동으로 띄워야 하는 번거로움이 있다. `spring-boot-docker-compose`는 그 반대 — 편의성은 얻지만 다중 모듈 공유 시나리오에서 생명주기 충돌이 생긴다.

**실무 함정**
- Gradle Wrapper 파일(`gradle-wrapper.jar`, `gradlew`, `gradlew.bat`)이 `.gitignore` 예외 처리로 커밋 대상이어도, 로컬에서 실수로 삭제되면 IDE(IntelliJ 등)가 자체 wrapper 구현으로 이를 가려버려서 못 알아챌 수 있다. CI 환경이나 신규 클론 환경에서야 뒤늦게 발견되는 경우가 많다 — 정기적으로 CLI에서 `./gradlew`를 직접 실행해 검증하는 습관이 필요하다.
- IntelliJ `.idea/gradle.xml`의 `myGradleHome`처럼 로컬 머신에 고정된 Gradle 경로 설정은 팀 전체의 재현성에 도움이 안 된다 — wrapper(`gradle-wrapper.properties`)가 진짜 소스 오브 트루스여야 한다.

**안티패턴**
- 여러 인프라 서비스(Zookeeper + Kafka + PostgreSQL)를 하나의 커스텀 이미지에 몰아넣는 것 — 서비스별 독립적인 lifecycle 제어가 불가능해지고, 공식 이미지의 검증된 안정성과 유지보수 이점을 잃는다.
