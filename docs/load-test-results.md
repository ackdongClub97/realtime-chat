# 부하 테스트 결과

## 환경

- MacBook (Apple Silicon), Docker Desktop VM 8 vCPU / 8GB
- `docker compose`: app ×2 (Hikari 20), Nginx, PostgreSQL 16, Redis 7, Prometheus, Grafana
- **k6도 같은 호스트에서 실행.** 부하 발생기와 서버가 CPU를 나눠 쓰므로 절대 수치는 보수적으로 봐야 한다. 이 측정의 목적은 병목 위치와 개선 효과 확인이다.
- 측정 중 macOS 잠자기로 무효가 된 실행 1회는 제외했다. 이후 측정은 `caffeinate`로 잠자기를 막고 진행했다.
- **측정 시점:** 아래 1·2절 표는 개선 과정 중 기록한 값이다(측정 당시 커밋 기록 없음). 최종 코드로 2026-10-13에 같은 조건으로 다시 측정한 값은 4절에 있다.

## 1. WebSocket 메시징 (`loadtest/ws-chat.js`)

세션 S개 × 2명이 Nginx(라운드로빈)를 거쳐 접속한다. 따라서 상당수 세션은 두 사용자가 서로 다른 인스턴스에 붙는다(Redis 팬아웃 경로). 각 사용자는 주기적으로 메시지를 보낸다.

- `ack_latency`: send부터 ack까지
- `e2e_latency`: 송신 시각부터 상대방 수신까지
- `seq_gap`: 수신 스트림에서 seq 구멍 수

### 최종 코드

| 동시 접속 | 목표 처리량 | 실측 처리량 | ack p95 | e2e p95 | seq_gap |
|---|---|---|---|---|---|
| 200 (세션 100) | 200/s | 193/s | **3ms** | **4ms** | 0 |
| 500 (세션 250) | 1,000/s | 942/s | **4ms** | **4ms** | 0 |
| 500 (세션 250) | 2,000/s (포화) | 1,846/s | 202ms | 352ms | 0 |

- 초당 약 1,000건까지는 지연이 거의 늘지 않는다. 포화 구간에서도 **순서와 정합성은 깨지지 않는다**(seq_gap 0, 메시지 약 12만 건).
- 포화 시 병목은 PostgreSQL commit 처리량이다. Hikari 대기 커넥션이 최대 140~180까지 쌓였고, outbox 지연은 약 6~9초였다.

### 개선 과정 (같은 조건: 동시 접속 500, 초당 2,000건)

| 단계 | ack p95 | e2e p95 | Hikari 대기 최대 | outbox 지연 최대 | gap-fill | 비고 |
|---|---|---|---|---|---|---|
| 0. 최초 구현 | 2.58s | 2.92s | 177 | 75s (backlog 21만 행으로 증가) | - | |
| 2. 왕복 감소 + outbox coalescing + 스케줄러 분리 | - | - | 109~157 | **4.6~6.9s** | - | 오탐 sweep 발견 |
| 3. 비동기 팬아웃 + 하트비트 강화 | 169ms | 2.28s | 135~158 | 6.4~7.5s | - | 오탐 sweep 0 |
| 4. 세션별 전송 레인 | 114ms | 3.06s | 78~184 | 6.1~6.4s | 10,338 | 순서 역전 → DB gap-fill 폭증 |
| **5. 재정렬 버퍼** | 202ms | **352ms** | 78~141 | 8.7s | **290** | |

원인 분석과 각 조치의 근거는 [query-optimization.md 2절](query-optimization.md)에 있다.

## 2. 조회 핫패스 (`loadtest/read-paths.js`)

메시지 5,000건 세션 1개와 세션 약 600개 상태에서 다음 세 시나리오를 동시에 30초간 실행했다(폐쇄 루프, think time 0).

- timeline 20 VU: 무작위 seq 복원
- messages 20 VU: 첫 페이지 또는 무작위 페이지
- list 10 VU

| API | 처리량 (합계 1,223 req/s) | avg | p95 | 비고 |
|---|---|---|---|---|
| `GET /timeline?seq=` | | 70.7ms | 158ms | 리플레이 평균 **49.7건**, 최대 **99건** (스냅샷 주기 100) |
| `GET /messages` | | 25.6ms | 57.2ms | keyset, 인덱스 1회 스캔 |
| `GET /sessions?status=` | | 20.0ms | 45.1ms | |
| 실패율 | | | 0.00% | |

- 모든 쿼리의 DB 실행 시간은 0.1ms 수준이다(EXPLAIN ANALYZE). 지연은 앱 CPU(JSON 처리)와 동시성 대기에서 나온다.
- timeline 단건(부하 없음): 메시지 5,000건 세션 **약 13ms**, 메시지 수백 건 세션 **3~4ms**. 스냅샷 역직렬화 비용이 세션의 메시지 수에 비례한다. 리플레이 비용이 아니다. 개선안은 db-design.md 5절에 있다.

## 3. 재현

```bash
docker compose up -d --build
k6 run -e SESSIONS=250 -e RATE_MS=500  -e DURATION=60s loadtest/ws-chat.js      # 초당 ~1,000건
k6 run -e SESSIONS=250 -e RATE_MS=250  -e DURATION=60s loadtest/ws-chat.js      # 초당 ~2,000건 (포화)
k6 run -e EVENTS=5000 loadtest/read-paths.js
# 결과 JSON: loadtest/results/*.json, 대시보드: http://localhost:3000 (Realtime Chat)
```

## 4. 재측정 (2026-10-13, 커밋 `7a0ad0c`)

1·2절과 같은 환경·명령으로 최종 코드를 다시 측정했다. 서버 코드는 커밋 `7a0ad0c`와 같다(이후 커밋은 문서·스크립트 주석·k6 기준값만 바꿨다).

### WebSocket 메시징: 500명, 목표 1,000/s (3회)

| 회차 | 처리량 | ack p95 | e2e p95 | seq_gap | checks |
|---|---|---|---|---|---|
| 1 | 934/s | 11ms | 13ms | 0 | 500/500 |
| 2 | 950/s | 9ms | 10ms | 0 | 500/500 |
| 3 | 947/s | 7ms | 8ms | 0 | 500/500 |

- 처리량은 1절 기록(942/s)과 같은 수준이고, 정합성(seq_gap 0)도 그대로다.
- p95 지연은 3회 모두 1절 기록(ack·e2e 4ms)보다 높다(7~13ms). 원인은 확인하지 않았다. 1절 측정 이후 DB에 쌓인 데이터와 같은 호스트의 CPU 경합이 후보다.
- 수신 건수가 송신보다 약간 적은 것(예: 59,362 / 59,500)은 테스트 종료 시점에 전송 중이던 메시지다.

### 조회 핫패스 (2회)

| API | 1회차 p95 | 2회차 p95 | 기준 (기본값) |
|---|---|---|---|
| `GET /timeline?seq=` | 183ms | 187ms | < 300ms |
| `GET /messages` | 60.1ms | 73.2ms | < 100ms |
| `GET /sessions?status=` | 46.2ms | 60.5ms | < 100ms |
| 합계 처리량 / 실패율 | 1,169 req/s / 0% | 959 req/s / 0% | 실패율 < 1% |

### k6 합격 기준(threshold)

- **원래 목표**는 timeline p95 < 100ms, messages·list p95 < 50ms다. 부하 발생기를 서버와 분리한 환경을 기준으로 잡은 목표다. 이 로컬 환경에서는 2절 기록(158ms, 57.2ms)도 이 목표를 넘는다.
- **기본값**은 timeline 300ms, messages·list 100ms다. k6와 서버가 같은 호스트에서 CPU를 나눠 쓰는 로컬 환경에서 회귀를 감지하는 용도로, 위 실측값에 여유를 둔 값이다.
- 원래 목표로 실행하려면 다음처럼 환경변수로 지정한다.
  ```bash
  k6 run -e TIMELINE_P95=100 -e MESSAGES_P95=50 -e LIST_P95=50 loadtest/read-paths.js
  ```
- **timeline이 가장 느린 이유**는 리플레이가 아니라 스냅샷 역직렬화다(리플레이는 최대 99건으로 일정). 비용이 세션의 메시지 수에 비례하며, 이 테스트의 세션은 메시지가 5,000건이다. 개선안(메시지 상태를 스냅샷에서 분리)은 [db-design.md 5절](db-design.md)에 있다.
