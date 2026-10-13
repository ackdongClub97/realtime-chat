# WebSocket 프로토콜

`ws://{host}/ws?sessionId={uuid}&userId={id}[&lastSeq={n}]`

| 쿼리 | 설명 |
|---|---|
| `sessionId` | 세션 ID (필수) |
| `userId` | 사용자 ID (필수). 인증은 과제 비목표라 쿼리로 받는다. 운영에서는 핸드셰이크 시 토큰 검증으로 대체한다 |
| `lastSeq` | 클라이언트가 이미 가진 마지막 seq. 지정하면 그 이후 이벤트를 먼저 재전송(resume)한 뒤 live로 전환한다. 생략하면 현재 시점부터 live만 받는다 (이력은 REST `GET /messages`로 조회). `0`이면 처음부터 받는다 |

연결 전에 `POST /sessions/{id}/join`으로 참여해 있어야 한다.

모든 프레임은 JSON 텍스트 프레임이다.

## 클라이언트 → 서버

```jsonc
// 이벤트 전송. eventType: MESSAGE | MESSAGE_EDITED | MESSAGE_DELETED | READ | TYPING
{ "type": "send", "clientEventId": "uuid", "eventType": "MESSAGE",
  "payload": { "text": "hello" }, "clientSeq": 12, "clientTs": "2026-10-13T00:00:00Z" }

// 누락 감지 시 재전송 요청 (lastSeq 이후를 다시 받음)
{ "type": "resume", "lastSeq": 41 }

// 명시적 퇴장 (LEAVE 이벤트 기록 후 연결 종료)
{ "type": "leave" }

// 하트비트 (presence TTL 갱신)
{ "type": "ping" }
```

| eventType | payload |
|---|---|
| `MESSAGE` | `{ "text": string(1..4000) }`. 메시지 ID = 이 이벤트의 `clientEventId` |
| `MESSAGE_EDITED` | `{ "messageId": string, "text": string }`. 작성자 본인만 가능 |
| `MESSAGE_DELETED` | `{ "messageId": string }`. 작성자 본인만 가능, 삭제는 최종 상태 |
| `READ` | `{ "upToSeq": number }`. 읽음 포인터, 앞으로만 이동 |
| `TYPING` | `{ "typing": boolean }`. **저장하지 않는 휘발성 신호** (복원 대상 아님) |

## 서버 → 클라이언트

```jsonc
// 연결 직후 1회. resumeFrom 이후의 이벤트가 이어서 전송된다
{ "type": "welcome", "connId": "...", "sessionId": "...", "userId": "bob",
  "serverLastSeq": 57, "resumeFrom": 41 }

// 송신 결과. status: ACCEPTED(신규 저장) | DUPLICATE(이미 저장됨, 같은 seq) | NOOP(상태 변화 없음)
{ "type": "ack", "clientEventId": "uuid", "seq": 58, "status": "ACCEPTED" }

// 거절. retryable=true 면 같은 clientEventId 로 재시도해야 한다
{ "type": "error", "clientEventId": "uuid", "code": "TARGET_NOT_FOUND", "message": "...", "retryable": true }

// 저장된 이벤트 (본인이 보낸 이벤트 포함). seq 는 연결마다 1씩 증가함이 보장된다
{ "type": "event", "seq": 58, "eventType": "MESSAGE", "clientEventId": "uuid", "userId": "alice",
  "payload": { "text": "hello" }, "serverReceivedAt": "2026-10-12T20:11:29.799802Z" }

// 휘발성 신호
{ "type": "presence", "userId": "bob", "status": "ONLINE" }   // ONLINE | OFFLINE
{ "type": "typing", "userId": "alice", "typing": true }
{ "type": "pong", "serverLastSeq": 58 }
```

### 주요 오류 코드

| code | 의미 | 클라이언트 행동 |
|---|---|---|
| `TARGET_NOT_FOUND` | 수정/삭제 대상 메시지가 아직 없음 (순서 뒤바뀜) | 원본의 ack를 받은 뒤 **같은 clientEventId**로 재시도 |
| `DB_UNAVAILABLE` | DB 장애 / 타임아웃 | 백오프 후 **같은 clientEventId**로 재시도 |
| `IDEMPOTENCY_KEY_REUSED` | 같은 clientEventId로 다른 내용을 보냄 | 클라이언트 버그. 새 ID를 발급 |
| `NOT_PARTICIPANT` / `NOT_MESSAGE_OWNER` | 권한 없음 | 재시도하지 않음 |
| `SESSION_ENDED` | 종료된 세션 | 재시도하지 않음 |

### Close code

| code | 의미 |
|---|---|
| 4000 | 세션 종료 |
| 4001 | 같은 사용자의 새 연결로 교체됨 (사용자당 세션 연결 1개) |
| 4400 / 4403 / 4404 / 4409 | 잘못된 파라미터 / 참여자 아님 / 세션 없음 / 종료된 세션 |
| 1000 | 정상 종료 (`leave` 후) |
| 1011 / 1006 | 서버 오류 / 비정상 단절 → `lastSeq`로 재연결 |

## 클라이언트 구현 규칙 (정합성 계약)

1. **이벤트마다 `clientEventId`(UUID)를 붙이고, ack를 못 받으면 같은 ID로 재전송한다.** 서버가 멱등하게 처리하므로 at-least-once 전송으로 effectively-once 저장이 된다.
2. **수신 이벤트는 `seq`로만 정렬하고 판단한다.**
   - `seq <= lastSeq`면 버린다.
   - `seq > lastSeq + 1`이면 `resume`을 보낸다. 서버도 연결별로 gap-fill을 하므로 정상 상황에서는 발생하지 않는다.
3. **재연결할 때는 마지막으로 받은 `seq`를 `lastSeq`로 넘긴다.**
4. 화면 표시 시각은 `serverReceivedAt`을 쓴다. `clientTs`는 기기 시계라 신뢰하지 않는다.

참고 구현: [`scripts/lib/chat-client.mjs`](../scripts/lib/chat-client.mjs)
