# 장애 대응 시나리오 (감지 → 완화 → 복구)

각 시나리오에는 재현 스크립트가 있다. `docker compose up -d --build`로 전체 스택을 띄운 뒤 실행한다.

| 시나리오 | 재현 | 결과 |
|---|---|---|
| 서버 인스턴스 강제 종료 | `node scripts/chaos/instance-down.mjs` | 감지 21.8~23.5s (3회), resume 누락/중복 0 |
| Redis 중단 | `node scripts/chaos/redis-down.mjs` | 저장 정상, 복구 후 gap-fill로 자동 보충 |
| DB 응답 없음 | `node scripts/chaos/db-pause.mjs` | 5.0s 후 retryable 오류, 재시도 후 1건만 저장 |
| projection 실패 (poison) | `PUT /admin/chaos {"poisonText":"__poison__"}` | 백오프 재시도 4회 → DLQ → requeue 후 복구 |

## 1. 서버 다운 (인스턴스 장애)

**상황:** app2 프로세스가 SIGKILL로 죽는다(OOM, 노드 장애). 정상 종료 처리(DISCONNECT 기록, presence 삭제)가 실행되지 않는다.

**감지**

- 인스턴스 하트비트 키 `instance:app2`(TTL 15s)가 만료된다. 살아있는 인스턴스의 sweeper가 5초마다 확인하고, **2회 연속 누락**이면 장애로 판정한다. 그래서 감지 시간은 이론상 약 17~25초(마지막 하트비트 후 TTL 만료 12~15초 + sweep 2회)다. 실측은 21.8~23.5초다(2026-10-13, 3회).
- 메트릭: `chat_presence_instance_swept_total` 증가, Prometheus `up{instance="app2"} == 0`, 인스턴스별 `chat_ws_connections` 급감.
- LB: Nginx `max_fails=1 fail_timeout=5s`로 새 연결이 살아있는 인스턴스로 간다. 운영(Kubernetes)에서는 readiness probe(`/actuator/health/readiness`)가 이 역할을 한다.

**완화**

- **데이터 유실 없음:** ack를 받은 메시지는 이미 커밋된 상태다. ack 전에 죽었다면 클라이언트가 같은 `clientEventId`로 재전송하고, 서버가 멱등하게 처리한다.
- 살아있는 인스턴스 사용자는 계속 대화할 수 있다.
- sweeper가 죽은 인스턴스의 연결마다 `DISCONNECT{reason:"instance_down"}`을 기록하고 presence OFFLINE을 알린다. 그래서 상대방은 "연결 끊김"을 볼 수 있고, 세션 상태는 `INTERRUPTED`가 된다. 여러 sweeper가 동시에 돌아도 결정적 ID(`sys:disconnect:{connId}`)와 sweep 락 때문에 한 번만 기록된다.
- 이미 다른 인스턴스로 재연결한 사용자는 presence 소유자가 바뀌었으므로 건너뛴다.
- 클라이언트는 close code 1006을 받고 지수 백오프로 재연결한다. LB가 살아있는 인스턴스로 연결해 준다.

**복구**

- 재연결할 때 `lastSeq`로 resume해서 끊긴 동안의 이벤트를 원장에서 받는다. 실측: 메시지 3건, gap/중복 0. RECONNECT가 기록되면 세션이 `ACTIVE`로 돌아간다.
- 죽은 인스턴스가 처리 중이던 outbox 작업은 리스(30s) 만료 후 다른 워커가 회수한다.
- 인스턴스 재기동 시 자동으로 다시 등록되며, 앱에 상태가 없으니 별도 작업이 없다.
- **사후 점검:** 해당 시간대 `chat_events_rejected_total`, `chat_events_duplicate_total`(재시도 폭증 여부), DLQ를 확인한다.

**오탐 방지 (부하 테스트에서 발견한 버그의 재발 방지)**

- 스케줄러 스레드 공유로 하트비트가 밀려, 살아있는 인스턴스를 장애로 판정한 적이 있다. 다음 조치로 막았다.
  - 하트비트 전용 3s 주기(TTL 대비 5배 여유)
  - 2회 연속 확인
  - 스케줄러 풀 분리
- 그래도 오판하면 살아있는 연결이 자기 DISCONNECT를 받는 즉시 RECONNECT를 기록해 **자가 치유**한다.

## 2. DB 장애 또는 성능 저하

### 2-1. DB 응답 없음 / 네트워크 단절

**감지**

- JDBC `socketTimeout=5s`, `connectTimeout=3s`, Hikari `connection-timeout=3s`로 **무한 대기 대신 빠르게 실패**한다.
  - 처음 장애 주입을 했을 때는 이 설정이 없어 쿼리가 무한 대기했다. 이 버그를 발견해 수정했다.
- 메트릭: `chat_events_rejected_total{code="DB_UNAVAILABLE"}`, HTTP 503 비율, `hikaricp_connections_pending`, `/actuator/health`의 `db: DOWN`.

**완화**

- 클라이언트에 **재시도 가능한 오류**를 돌려준다. WS는 `error{code:DB_UNAVAILABLE, retryable:true}`, REST는 `503 + Retry-After: 1`이다.
- 클라이언트는 같은 `clientEventId`로 백오프 재전송한다.
- 이미 연결된 사용자끼리의 typing과 presence는 Redis 경로라 유지된다.
- outbox 워커는 claim 실패를 경고만 남기고 다음 주기에 재시도한다.
- 읽기 부하를 분산하려면 timeline과 messages를 읽기 전용 replica로 보낸다(운영 확장안).

**복구**

- DB가 복구되면 재시도가 성공한다. 멱등성 키 덕분에 **ack 전 실패와 재전송이 겹쳐도 1건만 저장**된다(실측 검증).
- outbox backlog는 워커가 자동으로 따라잡는다. `chat_outbox_oldest_age_seconds`가 0으로 돌아오는지 확인한다.

### 2-2. 커넥션 고갈

**감지:** `hikaricp_connections_pending > 0` 지속, append p99 상승, `chat_outbox_oldest_age_seconds` 증가.

**원인 진단:**
- `pg_stat_statements`로 느린 쿼리인지 본다.
- 트랜잭션 왕복 수와 커넥션 점유 시간을 본다(부하 테스트에서 실제로 진단한 경로, query-optimization.md 2절).
- `pg_stat_activity`에서 `idle in transaction`을 확인하고, Hikari `leak-detection-threshold=10s` 로그로 커넥션 누수를 찾는다.

**완화:**
- `connection-timeout=3s`로 대기 요청이 쌓여 스레드가 고갈되는 것을 막는다. 빠른 실패 → 클라이언트 재시도로 처리한다.
- outbox 워커 스레드 수(`chat.outbox.worker-threads`)를 줄여 쓰기 경로에 커넥션을 양보한다.
- 휘발성 신호(typing)는 원래 DB를 쓰지 않는다.

**근본 조치:**
- 트랜잭션 왕복을 줄인다(적용 완료: 7 → 4).
- PgBouncer(transaction mode)를 둔다.
- 인스턴스 × 풀 크기가 `max_connections`를 넘지 않게 관리한다.

### 2-3. 락 경합

**설계상 경합 지점:**
- `chat_session` 행 락: 같은 세션의 append만 직렬화된다. 1:1 대화에서는 경합이 거의 없다.
- `projection_offset` 행 락: 워커끼리만 쓰고, 사용자 요청 경로와 분리돼 있다.

**감지:** `pg_locks`/`pg_stat_activity`의 `wait_event_type = 'Lock'`, `chat_append_latency` p99, PostgreSQL `deadlocks` 카운터.

**완화와 사례:**
- 부하 테스트에서 outbox 삭제 간 **데드락**을 발견했다. `FOR UPDATE SKIP LOCKED`로 삭제 경합 자체를 없앴다.
- 데드락이 나더라도 outbox 작업은 백오프 재시도라 결과적으로 처리된다.
- 핫 세션(초당 수백 건)이 생기면 세션 단위 rate limit을 걸고, 그래도 부족하면 세션별 단일 writer 큐로 바꾼다.

## 3. 데이터 유실 또는 정합성 이슈

| 위험 | 예방 설계 | 감지 | 복구 |
|---|---|---|---|
| **중복 저장** (재전송, 동시 요청) | `UNIQUE(session_id, client_event_id)` + 세션 락 하 확인 + 결정적 시스템 이벤트 ID | `chat_events_duplicate_total` (정상 동작의 지표), 같은 키 다른 내용 → 409 | 구조적으로 발생하지 않는다 (32 스레드 동시성 테스트) |
| **seq 구멍 / 중복** | 행 락 아래 카운터 + 저장하지 않으면 롤백 | 아래 정합성 점검 쿼리 | 구조적으로 발생하지 않는다 (200건 동시 append 테스트) |
| **부분 실패** (저장은 됐는데 projection 미반영) | event + outbox 동일 트랜잭션 | `chat_outbox_oldest_age_seconds`, `projectedSeq < sessionLastSeq` | 자동 재시도 → DLQ → requeue |
| **projection 오염 / 버그** | read model은 파생 데이터 | 원장 기반 복원과 projection 비교 | `POST /admin/projections/{id}/rebuild` |
| **스냅샷 버그** | `schema_version`, 결정적 reducer | `timeline?verify=true` (스냅샷 경로 == 전체 리플레이) | 해당 버전 스냅샷 삭제 또는 버전 업 → 자동 재생성 |
| **실시간 전송 유실** (Redis 유실, 인스턴스 장애) | 원장이 진실 | `chat_delivery_gap_filled_total` | 서버 gap-fill, 클라이언트 resume |
| **ack 전 서버 장애** | 클라이언트 재전송 + 멱등 | - | 같은 ID 재전송 → 1건만 저장 |
| **원장 자체 유실** (디스크/운영 실수) | PostgreSQL WAL 아카이빙 + PITR, replica | 백업 검증 작업 | PITR 복원 → projection, 스냅샷 재구축 |

### 정합성 점검 쿼리 (주기 작업 / 사고 시 수동 실행)

```sql
-- 1) seq 구멍: 세션별 이벤트 수와 last_seq 가 같아야 한다
SELECT s.id, s.last_seq, count(e.*) AS events
  FROM chat_session s LEFT JOIN event e ON e.session_id = s.id
 GROUP BY s.id HAVING s.last_seq <> count(e.*);

-- 2) write model 과 원장 불일치: participant 의 마지막 이벤트가 원장에 존재하는지
SELECT p.* FROM participant p
 WHERE NOT EXISTS (SELECT 1 FROM event e WHERE e.session_id = p.session_id AND e.seq = p.last_event_seq);

-- 3) projection 지연/누락: 원장 MESSAGE 수 vs projection 행 수 (projection 이 따라잡은 세션만)
SELECT o.session_id, o.last_applied_seq,
       (SELECT count(*) FROM event e WHERE e.session_id = o.session_id AND e.type = 'MESSAGE'
          AND e.seq <= o.last_applied_seq) AS ledger_messages,
       (SELECT count(*) FROM message_projection m WHERE m.session_id = o.session_id
          AND m.seq <= o.last_applied_seq) AS projected_messages
  FROM projection_offset o
 WHERE (SELECT count(*) FROM event e WHERE e.session_id = o.session_id AND e.type = 'MESSAGE' AND e.seq <= o.last_applied_seq)
    <> (SELECT count(*) FROM message_projection m WHERE m.session_id = o.session_id AND m.seq <= o.last_applied_seq);

-- 4) 수신 시각 단조성 (시각 기반 복원의 전제)
SELECT session_id FROM (
  SELECT session_id, server_received_at < lag(server_received_at) OVER (PARTITION BY session_id ORDER BY seq) AS broken
    FROM event) t WHERE broken GROUP BY session_id;
```

모든 장애 주입 실행 후 1~4번 쿼리 결과는 모두 0행이었다.
