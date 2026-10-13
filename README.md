# Realtime Chat + Event-sourced Restore

1:1 실시간 채팅 백엔드입니다. 모든 상태 변화를 **append-only 이벤트 원장**에 기록하고, 원장을 리플레이해 **임의 시점의 대화 상태를 결정적으로 복원**합니다.

- 중복 이벤트: `clientEventId` 멱등성 키 + DB 유니크 제약 → 재전송해도 1건만 저장, 같은 seq로 ack
- 순서 뒤바뀜: 서버가 세션별로 발급하는 gap-free `seq`가 유일한 기준. 클라이언트는 연결마다 seq가 1씩 증가하는 스트림만 받음
- 재연결: `lastSeq`로 resume → 끊긴 동안의 이벤트를 누락/중복 없이 받음 (인스턴스가 바뀌어도 동일)
- 복원: 스냅샷(100 이벤트마다) + 리플레이 → 세션 길이와 무관하게 최대 99건 리플레이

**Java 21 · Spring Boot 3.5 · WebSocket · PostgreSQL 16 · Redis 7 · Micrometer/Prometheus/Grafana · Testcontainers · k6**

---

## 빠른 시작

요구사항: Docker, (테스트/스크립트용) JDK 21, Node 22+, k6

```bash
# 전체 스택: 앱 2대 + Nginx(LB) + PostgreSQL + Redis + Prometheus + Grafana
docker compose up -d --build

# 동작 확인
./scripts/smoke.sh                                                        # REST: 생성/참여/중복/순서역전/복원
A=http://localhost:8081 B=http://localhost:8082 node scripts/ws-scenario.mjs   # WS: 서로 다른 인스턴스 간 19개 검증
```

| 주소 | 내용 |
|---|---|
| http://localhost:8080 | Nginx → app1, app2 (라운드로빈, sticky 없음) / 브라우저 테스트 클라이언트 |
| http://localhost:8080/swagger-ui.html | API 문서 (Swagger UI) |
| http://localhost:8081, :8082 | app1, app2 직접 접속 |
| http://localhost:3000 | Grafana 대시보드 "Realtime Chat" (로그인 없음) |
| http://localhost:9090 | Prometheus |

로컬에서 앱만 실행하는 경우:

```bash
docker compose up -d postgres redis
./gradlew bootRun          # :8080
```

### 테스트

```bash
./gradlew test             # 단위 73 + 통합 17 = 90개 (통합은 Testcontainers 로 실제 PostgreSQL/Redis 사용)
```

**단위 테스트 (DB 없음, Mockito)**

| 테스트 | 개수 | 검증 내용 |
|---|---|---|
| `SessionReducerTest` | 7 | 무작위 순서 + 무작위 중복 200회 → 항상 같은 상태 / 모든 지점에서 스냅샷+리플레이 == 전체 리플레이 / 이벤트 누락 시 실패 / 규칙 위반 이벤트 무시 |
| `StateMachineTest` | 21 | 참여자 상태 머신 전 조합(상태 4 × 이벤트), 세션 상태 파생, 클라이언트 전송 가능 타입 |
| `EventAppendServiceTest` | 23 | 멱등성(같은 키 → DUPLICATE + 롤백, 다른 내용 → 409), NOOP 시 seq 미소비, 정원/비참여자/작성자/읽음 포인터/텍스트 검증, 순서 역전(422 retryable), 거절 시 미발행 |
| `EventDeliveryTest` | 10 | 순서대로 전송, 중복 버림, **재정렬 버퍼**(원장 조회 없이 역전 해소), 대기 만료 시 원장 gap-fill, gap-fill 실패 재시도, resume 기준점, 전송 실패 시 연결 종료 |
| `OutboxWorkerTest` | 5 | 지수 백오프 + jitter + 상한, 세션별 1회 catch-up 과 coalescing 삭제, 실패 격리, 최대 재시도 → DLQ |
| `SessionServiceTest` | 5 | keyset 커서 왕복(마이크로초 정밀도), 마지막 페이지, 페이지 크기 경계, 잘못된 커서 |
| `ClientConnectionTest` | 2 | 클라이언트 순번 gap 감지 |

**통합 테스트 (Testcontainers)**

| 테스트 | 개수 | 검증 내용 |
|---|---|---|
| `EventStoreIntegrationTest` | 8 | 32 스레드 동시 재전송 → 1건 저장 / 200건 동시 append → seq 1..N gap-free + 수신시각 단조 / 시점 복원 / 원본보다 먼저 온 수정 / 스냅샷 경계와 전 지점 일치 / poison → 재시도 → DLQ → requeue / 종료 세션 |
| `TimelineApiIntegrationTest` | 6 | HTTP `timeline` API로 시점 t의 메시지·참여자·안 읽은 수 복원 / `at`과 같은 시각에 수신된 이벤트 포함 / 모든 seq에서 `verify` 통과 / 경계 입력·오류 응답(`NO_STATE_AT` 등) |
| `WebSocketIntegrationTest` | 3 | 끊김 → 재연결 resume 시 누락·중복 없는 연속 seq, 중복 ack / 비정상 끊김 → DISCONNECT·`INTERRUPTED` → 재연결 → RECONNECT·`ACTIVE` (원장 순서, 시점 복원 결과까지) / 비참여자 거절 |

### 장애 주입 / 부하 테스트

```bash
node scripts/chaos/instance-down.mjs   # app2 SIGKILL → 약 25s 내 DISCONNECT 기록(실측 21.8~23.5s) → 다른 인스턴스로 resume
node scripts/chaos/redis-down.mjs      # Redis 중단 → 저장은 정상, 복구 후 자동 보충
node scripts/chaos/db-pause.mjs        # DB 무응답 → 5s 후 retryable 오류 → 재시도 시 1건만 저장

k6 run -e SESSIONS=250 -e RATE_MS=500 loadtest/ws-chat.js   # WS 500명, 초당 ~1,000건
k6 run loadtest/read-paths.js                              # timeline / messages / list
```

---

## 구현 범위

### 필수 구현 — 전부 구현

| 요구사항 | 구현 |
|---|---|
| 실시간 메시지 송수신 | WebSocket `/ws` + 자체 JSON 프로토콜 ([명세](docs/websocket-protocol.md)) |
| join / leave | `POST /sessions/{id}/join`, `/leave`, WS `leave` 프레임. 1:1이므로 정원 2명 |
| presence | Redis TTL 기반 ONLINE/OFFLINE + 원장의 DISCONNECT/RECONNECT 이벤트 → 세션 상태 `INTERRUPTED` |
| 이벤트 수집 API | `POST /sessions/{id}/events` (배치, 이벤트별 결과) + WS `send` |
| 중복 방지 | `clientEventId` + 세션 락 하 확인 + `UNIQUE` 제약, 결정적 시스템 이벤트 ID |
| 순서 기준 | 서버 발급 세션별 `seq` (gap-free). 수신 시각도 seq와 같은 방향으로 증가하도록 보정 |
| 특정 시점 복원 | `GET /sessions/{id}/timeline?at=` (또는 `?seq=`, `&verify=true`) |

### 가산점 구현

| 항목 | 내용 |
|---|---|
| Snapshot + Replay 복원 최적화 | seq가 100의 배수일 때 정확히 그 지점에서 스냅샷 생성 → 리플레이 상한 99건 (실측 평균 50, 최대 99) |
| Snapshot 생성 자동화 | outbox 워커가 주기 경계마다 + 세션 종료 시 자동 생성. 수동 `POST /snapshots`도 제공 |
| Projection 비동기 파이프라인 | Transactional Outbox → 다중 인스턴스 워커 (`SKIP LOCKED` + 리스), 지수 백오프 + jitter, DLQ + requeue, 체크포인트 기반 멱등 적용, projection 재구축 API |
| 재연결 resume / replay 고도화 | `lastSeq` resume, 서버 측 재정렬 버퍼 + 원장 gap-fill, 사용자당 1연결(인스턴스 간 KICK), presence 소유권(CAS) + 오탐 자가 치유 |
| 수평 확장 | 앱 2대 + Nginx(sticky 없음) + Redis Pub/Sub 팬아웃, 장애 인스턴스 자동 감지/정리(sweeper) |
| 부하 테스트 / 성능 측정 | k6 2종, 병목 진단 → 5단계 개선 기록 ([결과](docs/load-test-results.md)) |
| 운영 대시보드 | Grafana 자동 프로비저닝 대시보드 15패널, 도메인 메트릭 20종 |
| 관측 가능성 | JSON 구조화 로그, traceId (HTTP 자동 + WS 프레임별 span + Redis 메시지 전파) |
| 테스트 고도화 | 단위 73 + Testcontainers 통합 17, 결정성 property 테스트, 장애 주입 스크립트 3종 + chaos API |
| 추가 통신 방식 검토 | WebSocket/SSE/Long Polling/WebRTC 비교 + WebRTC 공존 설계 (문서) |
| 기타 | 브라우저 테스트 클라이언트 (`/`, 입력 중 표시는 상태가 바뀔 때만 전송: 입력 시작·삭제 시작·전송), 세션 생성 `Idempotency-Key`, 정합성 점검 SQL |

### 문서로만 대체한 항목

- 메시지 수가 매우 많은 세션의 스냅샷 크기 최적화 (메시지 분리 / 증분 스냅샷) — [db-design.md §5](docs/db-design.md)
- 파티셔닝 / 아카이빙 / 해시 샤딩 — [db-design.md §6](docs/db-design.md)
- 트레이스 exporter (OTLP → Tempo/Jaeger) 연동 — [design.md §6](docs/design.md)
- 인증/인가 (비목표) — `X-User-Id` 헤더, WS 쿼리 `userId`로 대체
- 삭제된 메시지 원문의 완전 삭제(crypto-shredding) — 현재 원문은 원장에 남음. [event-sourcing.md §3](docs/event-sourcing.md)

---

## 문서

| 문서 | 내용 |
|---|---|
| [docs/design.md](docs/design.md) | 아키텍처, 기술 선택 근거, **중복 처리 · 순서 기준 · 재연결 정합성 · 수평 확장 · 비동기 처리 · 관측 가능성** |
| [docs/event-sourcing.md](docs/event-sourcing.md) | 복원 전략(스냅샷+리플레이), 스냅샷 주기/포맷, reducer 결정성 규칙, 중복/역순 처리 |
| [docs/db-design.md](docs/db-design.md) | ERD, 핵심 DDL, 인덱스 근거, 정규화/비정규화/JSONB 트레이드오프, 데이터 증가 대응 |
| [docs/query-optimization.md](docs/query-optimization.md) | 핫패스 쿼리 3개 (SQL, 인덱스, EXPLAIN 실측, 병목/개선) + **부하 테스트 트러블슈팅 기록** |
| [docs/incident-response.md](docs/incident-response.md) | 서버 다운 / DB 장애 / 정합성 이슈: 감지 → 완화 → 복구, 정합성 점검 SQL |
| [docs/load-test-results.md](docs/load-test-results.md) | k6 측정 결과와 개선 전후 비교 |
| [docs/websocket-protocol.md](docs/websocket-protocol.md) | WS 프레임, 오류 코드, 클라이언트 정합성 계약 |
| [docs/openapi.yaml](docs/openapi.yaml) | OpenAPI 3.1 명세 |
| [docs/webrtc-comparison.md](docs/webrtc-comparison.md) | 통신 방식 비교 |

## API 요약

| Method | Path | 설명 |
|---|---|---|
| POST | `/sessions` | 세션 생성 (`Idempotency-Key` 지원) |
| GET | `/sessions?status=&participant=&from=&to=&cursor=&limit=` | 세션 목록 (keyset 페이지네이션) |
| GET | `/sessions/{id}` | 세션 상세 + 참여자 presence |
| POST | `/sessions/{id}/join` · `/leave` · `/end` | 참여 / 퇴장 / 종료 (멱등) |
| POST | `/sessions/{id}/events` | 이벤트 수집 (배치) |
| GET | `/sessions/{id}/events?fromSeq=&toSeq=` (또는 `from=&to=`) | 원장 조회 (디버깅) |
| GET | `/sessions/{id}/messages?beforeSeq=&limit=` | 대화창 (read model) |
| GET | `/sessions/{id}/timeline?at=\|seq=&limit=&verify=` | **특정 시점 상태 복원** |
| POST / GET | `/sessions/{id}/snapshots` | 스냅샷 생성 / 목록 |
| GET / POST | `/admin/dlq`, `/admin/dlq/requeue`, `/admin/projections/{id}/rebuild`, `/admin/chaos` | 운영 |
| WS | `/ws?sessionId=&userId=&lastSeq=` | 실시간 |

## 주요 의사결정 요약

1. **진실의 원천은 `event` 테이블 하나.** participant/status(동기), message_projection/snapshot(비동기)은 모두 원장에서 재생성 가능한 파생 데이터.
2. **seq는 세션 행 락 아래의 카운터로 발급.** 시퀀스 객체(롤백 시 gap)나 `max+1`(경합) 대신 gap-free를 택함. 저장하지 않는 경우는 롤백해 seq를 되돌림. 대가는 세션 단위 직렬화 — 1:1 대화에선 무시할 수준.
3. **순서 역전은 서버에서 거절하고 클라이언트가 재시도** (원본보다 먼저 온 수정 → 422 retryable). 원장에는 인과 순서가 지켜진 이벤트만 존재.
4. **실시간 팬아웃은 at-most-once(Redis), 정합성은 원장이 책임.** 유실은 재정렬 버퍼 → gap-fill → resume으로 복구. 그래서 Redis는 Kafka 없이도 충분.
5. **Transactional Outbox**로 이벤트 저장과 비동기 작업 등록을 원자화. outbox 행은 "세션에 새 이벤트 있음" 신호이며 실제 적용은 체크포인트 기반 → 중복/순서 무관 멱등.
6. **스냅샷은 정확한 경계(seq % 100 == 0)에서**, 상태는 결정적 → 동시 생성도 `ON CONFLICT DO NOTHING`으로 안전. `verify=true`로 언제든 전체 리플레이와 대조.
7. **성능보다 정합성.** `synchronous_commit=off` 같은 지연 개선책은 ack 후 유실 가능성 때문에 채택하지 않음.

## 가정

- 1:1 세션: 동시에 참여할 수 있는 사용자 최대 2명 (LEAVE 후 재참여 가능)
- 사용자 식별은 신뢰된 헤더/쿼리 (인증은 비목표)
- 사용자당 세션 연결 1개 (새 연결이 이전 연결을 대체)
- 메시지 ID = MESSAGE 이벤트의 `clientEventId`
- typing indicator는 휘발성 신호로 저장/복원하지 않음
- 시점 복원의 기준 시각은 **서버 수신 시각**

## 프로젝트 구조

```
src/main/java/com/example/chat
├── api/          REST 컨트롤러, 세션 서비스, DTO
├── ingest/       EventAppendService — 모든 이벤트의 단일 쓰기 경로 (락·중복·검증·seq·outbox)
├── restore/      SessionReducer(순수 함수), SessionState, RestoreService, SnapshotService
├── projection/   OutboxWorker(재시도/DLQ), MessageProjector(체크포인트, 스냅샷 자동화)
├── realtime/     WebSocket 핸들러, EventDelivery(재정렬/gap-fill), Redis 버스, presence, sweeper
├── store/        JdbcTemplate 리포지토리 (핫패스 SQL)
├── domain/       EventType, ParticipantState(상태 머신), SessionStatus
├── admin/        DLQ / projection 재구축 / chaos API
└── config/       설정, 메트릭, WebSocket/Redis/OpenAPI
scripts/          smoke.sh, ws-scenario.mjs, seed.mjs, chaos/*.mjs, lib/chat-client.mjs
loadtest/         k6 스크립트, results/
infra/            nginx, prometheus, grafana(대시보드 프로비저닝)
docs/             설계 문서
```
