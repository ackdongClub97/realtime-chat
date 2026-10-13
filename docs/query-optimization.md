# 쿼리 / 인덱스 최적화와 트러블슈팅

측정 환경: MacBook (Docker Desktop VM 8 vCPU / 8GB). PostgreSQL 16, 앱 2대, Redis, k6가 모두 한 호스트에서 돌았다.
데이터: 이벤트 약 20만 건, 세션 601개(최대 세션 5,003 이벤트). 모든 계획은 `EXPLAIN (ANALYZE, BUFFERS)` 실측 결과다.

## 1. 핫패스 쿼리 3개

### Q1. 이벤트 append (메시지 1건마다 실행: 가장 뜨거운 경로)

```sql
-- ① 세션 행 잠금 + seq/수신시각 발급 (1 왕복)
UPDATE chat_session
   SET last_seq = last_seq + 1,
       last_event_at = GREATEST(clock_timestamp(), last_event_at)
 WHERE id = $1
RETURNING id, title, status, created_by, created_at, ended_at, last_seq, last_event_at;

-- ② 멱등성 확인 (세션 락 하에서)
SELECT ... FROM event WHERE session_id = $1 AND client_event_id = $2;

-- ③ 참여자 검증
SELECT * FROM participant WHERE session_id = $1 AND user_id = $2;

-- ④ 이벤트 + outbox 동시 기록 (1 왕복)
WITH inserted AS (
    INSERT INTO event (...) VALUES (...)
    ON CONFLICT (session_id, client_event_id) DO NOTHING
    RETURNING session_id, seq)
INSERT INTO outbox (session_id, event_seq) SELECT session_id, seq FROM inserted;
```

| 사용 인덱스 | 실측 |
|---|---|
| ① `chat_session_pkey` | mean 0.62ms (부하 시 행 락 대기 포함) |
| ② `uq_event_client_event_id` | 0.07ms, shared hit 4 |
| ③ `participant_pkey` | 0.05ms |
| ④ `event_pkey` + `uq_event_client_event_id` 갱신 | mean 0.52ms |

**예상 병목과 개선 방향**

- **세션 단위 직렬화:** 같은 세션의 append는 행 락으로 순서대로 처리된다. 1:1 대화의 세션당 쓰기 빈도(초당 수 건)에서는 문제가 없다. 단, 봇이 초당 수백 건을 보내는 식의 핫 세션은 한계가 된다. 그때는 세션별 단일 writer(파티션된 큐의 consumer)로 쓰기를 모아 배치 INSERT하는 구조로 바꾼다.
- **commit 비용:** 측정 결과 statement 실행 시간 합보다 커넥션 점유 시간이 훨씬 컸다(풀 대기 140+). Docker Desktop에서 WAL fsync가 느린 영향이 크다. 개선 방향은 다음과 같다.
  - 동시 commit을 모으는 group commit(`commit_delay`)을 쓴다.
  - 휘발성 신호는 원장에 넣지 않는다. 이미 TYPING은 원장에서 제외했다.
  - 앱 인스턴스가 늘면 PgBouncer(transaction mode)로 DB 커넥션 수를 고정한다.
  - `synchronous_commit=off`는 ack 이후 유실 가능성이 생겨 **채택하지 않았다**. 정합성을 성능과 바꾸지 않는다.
- **인덱스 쓰기 비용:** `event`에는 PK, UNIQUE, 시각 인덱스 3개만 둔다. 이벤트 1건당 B-tree 3개를 갱신한다. 조회 요구는 projection으로 넘기고 원장 인덱스는 늘리지 않는다.

### Q2. 시점 복원 `GET /sessions/{id}/timeline?at=t`

```sql
-- ① 시각 → seq
SELECT seq FROM event
 WHERE session_id = $1 AND server_received_at <= $2
 ORDER BY server_received_at DESC, seq DESC LIMIT 1;

-- ② 시작 스냅샷
SELECT seq, state FROM session_snapshot
 WHERE session_id = $1 AND seq <= $2 AND schema_version = 1
 ORDER BY seq DESC LIMIT 1;

-- ③ 스냅샷 이후 리플레이 (최대 N-1건)
SELECT ... FROM event WHERE session_id = $1 AND seq > $2 AND seq <= $3 ORDER BY seq LIMIT 1000;
```

```
① Index Only Scan Backward using idx_event_session_received   Buffers: shared hit=5   Execution Time: 0.082 ms
② Index Scan Backward using session_snapshot_pkey              Buffers: shared hit=6   Execution Time: 0.083 ms
③ Index Scan using event_pkey  (rows=49)                       Buffers: shared hit=9   Execution Time: 0.086 ms
```

- 세 쿼리 모두 **세션 길이와 무관하게** 인덱스 몇 페이지만 읽는다. 리플레이 건수는 스냅샷 주기(100) 때문에 최대 99건이다(k6 실측 평균 50, 최대 99).
- `seq`와 `server_received_at`이 세션 안에서 같은 방향으로 증가하도록 발급 시 `GREATEST(clock_timestamp(), last_event_at)`를 쓴다. 덕분에 ①은 1건만 찾으면 된다. 시각과 seq가 어긋날 수 있다면 "t 이전 이벤트 집합"이 연속 구간이 아니게 되어 범위 스캔이 불가능해진다.

**예상 병목과 개선 방향 (실측)**

- DB 시간은 0.3ms 미만이지만, 메시지 5,000건 세션의 단건 응답은 약 13ms다. 그중 복원이 10ms인데, 대부분은 **스냅샷 JSON 역직렬화**(seq 2500 스냅샷 원본 약 600KB, 저장 시 압축)다. 메시지 300건 세션은 3~4ms다.
- 동시 50 VU 혼합 부하에서는 timeline p95가 158ms였다. 병목은 앱 CPU(JSON 처리)다.
- 개선 방향:
  - (1) 스냅샷에서 메시지 본문을 분리하고 최근 N개만 범위 질의한다(db-design.md 5절).
  - (2) 역직렬화된 스냅샷을 프로세스 로컬 LRU에 캐시한다. 스냅샷은 불변이라 무효화가 필요 없다.
  - (3) 바이너리 포맷(Smile/CBOR)을 쓴다.

### Q3. 대화창 페이지 `GET /sessions/{id}/messages?beforeSeq=&limit=50`

```sql
SELECT message_id, seq, sender_id, content, status, created_at, updated_at
  FROM message_projection
 WHERE session_id = $1 AND seq < $2
 ORDER BY seq DESC
 LIMIT 51;            -- limit+1 로 다음 페이지 존재 여부 판단
```

```
Limit (rows=51)
  -> Index Scan using idx_msgproj_session_seq   Index Cond: (session_id = ... AND seq < 3000)
     Buffers: shared hit=8      Execution Time: 0.110 ms
```

- **keyset pagination**을 쓴다. `OFFSET`은 깊은 페이지에서 앞의 행을 모두 읽고 버리므로 O(offset)이 된다. `seq < 커서`는 페이지 깊이와 무관하게 인덱스에서 바로 시작한다.
- 원장(`event`)을 직접 읽지 않는 이유는 다음과 같다. 원장에는 수정/삭제/읽음/입퇴장 이벤트가 섞여 있어서, 메시지 50개를 보여 주려면 더 많은 행을 읽고 수정 이력을 합쳐야 한다. projection은 메시지 1개당 1행이며 최종 상태다.

**예상 병목과 개선 방향**

- projection은 비동기라 최대 수백 ms(부하 시 수 초) 늦을 수 있다. 응답의 `projectedSeq < sessionLastSeq`로 지연 여부를 클라이언트가 안다. 실시간 화면은 WebSocket 이벤트로 갱신되므로 이 API는 입장 시 이력 로딩과 스크롤에만 쓰인다.
- 세션 하나의 메시지가 수백만 건이 되어도 인덱스 깊이만 늘 뿐 페이지 비용은 일정하다. 테이블 전체가 커지면 `session_id` 해시 파티셔닝을 적용한다.

### (참고) 세션 목록 `GET /sessions?status=&participant=`

```
상태 필터:  Index Scan using idx_session_status_created  → 0.046 ms (정렬 없음)
참여자 필터: Nested Loop(idx_participant_user → chat_session_pkey) + Sort → 0.097 ms
```

참여자 필터는 사용자의 세션 수만큼 읽은 뒤 정렬한다. 사용자당 세션이 수천 개 이상이면 `participant`에 `session_created_at`을 비정규화하고 `(user_id, session_created_at DESC, session_id DESC)` 인덱스를 두어 정렬 없는 keyset으로 바꾼다.

## 2. 트러블슈팅 기록: 부하 테스트로 찾은 병목과 버그

조건: 세션 250개 × 2명 = WebSocket 500개, 사용자당 초당 4건(목표 초당 2,000건), 60초. 세부 수치는 [load-test-results.md](load-test-results.md)에 있다.

| 단계 | 변경 | ack p95 | e2e p95 | 관찰 |
|---|---|---|---|---|
| 0 | 최초 구현 | 2.58s | 2.92s | Hikari 대기 177, outbox 지연 75s → 이후 backlog 21만 건 |
| 1 | append 왕복 7→4 (UPDATE…RETURNING, CTE), outbox 워커 병렬화 | (악화) | | **버그 2건 노출** ↓ |
| 2 | 스케줄러 풀 분리, outbox coalescing 삭제, claim 쿼리 인덱스화 | | | outbox 지연 247s → **5s** |
| 3 | 팬아웃 비동기화, 하트비트 강화, 오탐 자가 치유, SKIP LOCKED 삭제 | 169ms | 2.28s | |
| 4 | 세션별 전송 레인(striped executor) | 114ms | 3.06s | gap-fill 10,338건 |
| 5 | **재정렬 버퍼**(200ms 대기 후 gap-fill) | 202ms | **352ms** | gap-fill 290건 |

### 문제 1. DB 커넥션 풀 고갈

- **감지:** `hikaricp_connections_pending` 최대 177, append 평균 1.2초.
- **분석 (`pg_stat_statements`):** statement 평균은 모두 1ms 미만이었다. 문제는 개별 쿼리가 아니라 **트랜잭션당 왕복 수(7회 + commit)** 와 그만큼 길어진 커넥션 점유 시간이었다.
- **조치:**
  - `SELECT … FOR UPDATE`와 seq 발급 `UPDATE`를 `UPDATE … RETURNING` 하나로 합쳤다.
  - 락 없이 하던 사전 중복 확인을 없앴다.
  - event와 outbox INSERT를 CTE 한 문장으로 묶었다.
  - 중복, NOOP, 거절일 때는 **롤백**해서 발급한 seq를 되돌린다(gap-free 유지).

### 문제 2. (버그) 살아있는 인스턴스를 "죽었다"고 판정

- **감지:** 로그에 `swept dead instance=app2 disconnectedConnections=250`. 정상 사용자 250명에게 DISCONNECT가 기록됐다.
- **원인:**
  - Spring `@Scheduled` 기본 스레드풀은 **1개**다. outbox 폴링이 길어지면 같은 스레드를 쓰는 presence 하트비트가 밀린다.
  - 하트비트 주기 10초에 TTL 15초라 여유가 5초뿐이었다.
- **조치:**
  - 스케줄러 풀을 4개로 늘렸다.
  - 인스턴스 하트비트를 3초 주기로 분리했다(TTL 대비 5배 여유).
  - **2회 연속** 누락돼야 sweep한다.
  - 그래도 오판하면 살아있는 연결이 자기 DISCONNECT를 받는 순간 RECONNECT를 기록해 **자가 치유**한다.
- **트레이드오프:** 실제 장애 감지 시간이 12초에서 20초로 늘었다.

### 문제 3. outbox backlog와 워커 데드락

- **원인 A:** 이벤트 1건당 outbox 1행이 생기는데, 워커는 poll당 100행만 처리했다. 수집 속도를 못 따라갔다.
  - **조치:** 세션을 seq k까지 따라잡으면 그 세션의 `event_seq <= k` outbox 행을 **일괄 삭제**한다(coalescing). outbox 행은 "이 세션에 새 이벤트가 있다"는 신호일 뿐이기 때문이다.
- **원인 B:** claim 쿼리가 `ORDER BY id`에 PENDING/만료 조건을 `OR`로 묶어, 부분 인덱스를 쓰지 못하고 대기열 전체를 정렬했다.
  - **조치:** 두 조건을 CTE 2개로 나눠 각자의 부분 인덱스 순서(`next_attempt_at, id` / `locked_until`)로 읽는다.
- **원인 C:** 세션 단위 삭제와 ID 단위 삭제가 서로 다른 순서로 행을 잠가 **데드락**(`PessimisticLockingFailureException`)이 났다.
  - **조치:** 세션 단위 삭제를 `FOR UPDATE SKIP LOCKED`로 바꿨다. 남이 잡은 행은 건너뛰고, 그 행은 점유한 워커가 직접 지운다.

### 문제 4. e2e 지연: 동기 팬아웃과 순서 역전

- **원인 A:** 팬아웃이 송신자 처리 스레드에서 동기로 실행됐고, ack는 팬아웃이 끝난 뒤에 나갔다. 그래서 수신자 소켓이 느리면 송신자도 막혔다.
  - **조치:** 커밋 직후 ack를 보내고, 팬아웃은 별도 스레드에서 처리한다.
- **원인 B:** 범용 스레드풀로 팬아웃하자 같은 세션의 seq 10과 11이 다른 스레드에서 역순으로 실행됐다. 매번 DB gap-fill이 일어나 커넥션을 소모했다.
  - **조치:** `session_id` 해시로 **단일 스레드 레인**을 고정했다.
- **원인 C:** 그래도 다른 인스턴스에서 커밋된 이벤트는 Redis를 거쳐 늦게 도착한다. 예를 들어 bob은 자기 seq 11(로컬)을 alice의 seq 10(Redis)보다 먼저 받는다. 구조상 정상적인 역전이다.
  - **조치:** 연결별 **재정렬 버퍼**를 둔다. 앞선 이벤트는 최대 200ms 보관하고, 빠진 seq가 도착하면 순서대로 내보낸다. 끝내 오지 않을 때만 원장에서 채운다.
  - **결과:** gap-fill 10,338건 → 290건, e2e p95 3.06초 → 352ms.

### 남은 한계 (측정 기준)

- **처리량 상한:** 이 환경에서 초당 약 1,850건이 포화점이다. 이때 ack p95는 약 200ms이고, 병목은 PostgreSQL commit 처리량(커넥션 대기)이다.
- **정상 구간:** 초당 940건(500명)에서는 ack/e2e p95가 모두 4ms다.
- **수평 확장 방향:** 앱 인스턴스는 무상태라 늘리면 되지만, 쓰기는 단일 PostgreSQL에 모인다. 다음 단계는 `session_id` 해시 샤딩(db-design.md 6절)이다.
