# 이벤트 기반 상태 복원

`GET /sessions/{id}/timeline?at={ISO-8601}` 또는 `?seq={n}`

특정 시점의 다음 상태를 복원한다.

- 세션 상태
- 참여자 목록(입장, 퇴장, 연결 끊김 반영)
- 메시지 목록(전송, 수정, 삭제 반영, 최근 `limit`개)
- 참여자별 읽음 포인터와 **unread count**

`verify=true`를 주면 스냅샷 없이 처음부터 전체 리플레이한 결과와 비교한 값(`verifiedAgainstFullReplay`)도 함께 반환한다.

## 1. 복원 전략: 스냅샷 + 리플레이

```
1. at → cutoffSeq   : t 이전(포함)에 서버가 수신한 마지막 이벤트의 seq    (인덱스 역방향 1건)
2. 스냅샷 선택       : seq <= cutoffSeq 인 가장 최근 스냅샷                  (PK 역방향 1건)
3. 리플레이          : (snapshot.seq, cutoffSeq] 이벤트를 seq 순으로 reducer 에 적용  (PK 범위 스캔, 최대 N-1건)
```

| 방식 | 장점 | 단점 | 판단 |
|---|---|---|---|
| 전체 리플레이 | 단순하고 저장 공간 추가 없음 | 비용이 O(세션 길이). 1만 이벤트 세션은 매번 1만 건을 읽음 | 검증용으로만 사용 (`verify=true`) |
| **스냅샷 + 리플레이** | 비용이 O(스냅샷 주기)로 상한이 정해짐 | 스냅샷 저장 공간, 생성 작업 필요 | **채택** |
| 시점별 상태 테이블 (temporal table) | 조회가 가장 빠름 | 이벤트마다 상태 전체를 복사 → 저장 폭증 | 기각 |

### 스냅샷 정책

- **주기:** seq가 N(기본 100)의 배수가 될 때마다 **정확히 그 seq**에서 생성한다. 추가로 세션 종료 시 최종 상태를 저장한다.
  - 처음에는 "마지막 스냅샷 이후 N개 이상이면 현재 위치에서 생성"했다. 그런데 워커가 배치로 따라잡을 때 스냅샷이 seq 180, 374처럼 띄엄띄엄 생겨 리플레이 상한이 N을 넘었다. 경계 정렬 방식으로 바꿔 **어떤 시점이든 리플레이는 최대 N-1건**이 됐다(k6 실측: 평균 50, 최대 99).
  - 새 스냅샷은 직전 스냅샷에 N개를 증분 리플레이해서 만든다. 생성 비용이 일정하다.
- **생성 위치:** outbox 워커(비동기)가 projection을 갱신할 때 함께 만든다. append 지연에 영향이 없다. 수동 생성은 `POST /sessions/{id}/snapshots`다.
- **N 선택 근거:** 1:1 대화의 이벤트 1건은 수백 바이트이고, 100건 리플레이는 수 ms 이하다. N을 줄이면 스냅샷 저장량이 늘고 N을 늘리면 복원 지연이 는다. 100은 단건 복원 수 ms와 저장 부담 사이의 균형점이다. `SNAPSHOT_INTERVAL` 환경변수로 조정할 수 있다.
- **저장 포맷:** JSONB(PostgreSQL TOAST 압축). `schema_version` 컬럼을 둔다. `SessionState` 구조가 바뀌면 버전을 올리고, 구버전 스냅샷은 조회 조건(`schema_version = ?`)에서 자동으로 빠진다. 스냅샷은 파생 데이터라 마이그레이션할 필요 없이 원장에서 다시 만들어진다.
- **비용 실측:** 메시지 5,000건 세션에서 seq 5000 스냅샷은 원본 JSON 1.2MB, 저장 111KB였다. 이 세션의 단건 복원은 약 13ms이고 대부분 JSON 역직렬화 비용이다. 대용량 세션 개선안은 [db-design.md 5절](db-design.md)에 있다.

## 2. Reducer: 결정성(Determinism) 보장

`SessionReducer.apply(state, event)`는 외부 상태(현재 시각, DB, 난수)를 참조하지 않는 순수 함수다.

| 규칙 | 효과 |
|---|---|
| 적용 순서는 오직 `seq`. 입력은 정렬한 뒤 적용한다 | 입력 순서가 섞여도 결과가 같다 |
| `seq <= state.lastSeq`인 이벤트는 무시한다 | **중복 입력에 멱등**하다 |
| `seq != lastSeq + 1`이면 예외를 던진다 | 이벤트가 빠진 상태로 그럴듯한 오답을 만들지 않는다 |
| 상태 전이는 write model과 같은 상태 머신(`ParticipantState.transition`)을 공유한다 | 실시간 검증과 복원 결과가 일치한다 |
| 규칙 위반 이벤트는 항상 같은 방식으로 무시하고 `ignoredEvents`로 센다 | 결과가 결정적이고, 이상 징후도 관측된다 |
| 읽음 포인터는 `max`로만 전진한다 | 교환법칙이 성립해 순서와 무관하다 |
| 직렬화 순서 고정: participants는 `TreeMap`, messages는 seq 순 `LinkedHashMap` | 스냅샷 JSON이 바이트 단위로도 결정적이다 |

**규칙 위반 이벤트 처리** (원장에는 서버 검증 때문에 보통 들어오지 않지만, reducer는 방어적으로 처리한다)

| 상황 | 처리 |
|---|---|
| 존재하지 않는 메시지 수정/삭제 | 무시 |
| 삭제된 메시지 수정 | 무시 (삭제는 최종 상태) |
| 다른 사람의 메시지 수정 | 무시 |
| 이미 JOINED인데 JOIN, LEFT인데 LEAVE | 무시 (상태 전이 없음) |
| 종료된 세션에 다시 종료 | 무시 |

### 중복과 순서 뒤바뀜을 복원에서 다루는 방법

복원 로직에 오는 입력은 이미 **원장에서 seq로 정렬된, 중복 없는 이벤트**다. 중복은 저장 단계에서 걸러지고, 순서는 서버가 seq로 확정한다. 그래도 reducer 자체가 중복과 역순 입력에 안전하도록 만들었다. 다음 같은 경로로 들어온 이벤트를 다시 적용하게 될 수 있기 때문이다.

- 백업 복원
- 다른 시스템에서 이관한 이벤트
- 스냅샷과 리플레이 범위가 겹치는 경우

**검증 테스트** (`SessionReducerTest`)

- `sameResultRegardlessOfArrivalOrderAndDuplicates`: 이벤트 17개를 무작위로 섞고, 무작위로 중복을 넣어 200회 반복한다. 결과 JSON이 항상 같다.
- `snapshotPlusReplayEqualsFullReplayAtEveryCut`: 모든 지점에서 스냅샷(JSON 왕복) 후 나머지를 리플레이한 결과가 전체 리플레이 결과와 같다.
- `missingEventFailsInsteadOfProducingWrongState`: 이벤트가 하나 빠지면 예외가 난다.
- 통합 테스트 `asyncProjectionAndSnapshotsMatchFullReplay`: 실제 DB에서 모든 seq 지점의 스냅샷 경로와 전체 리플레이가 같고, 리플레이는 N 미만이다.

## 3. 복원 대상 상태 모델

```jsonc
{
  "resolvedSeq": 8, "resolvedAt": "...", "status": "INTERRUPTED",
  "participants": [
    { "userId": "bob", "state": "DISCONNECTED", "joinedAt": "...", "disconnectedAt": "...",
      "lastReadSeq": 5, "unreadCount": 2 }
  ],
  "messages": [
    { "messageId": "m1", "seq": 4, "senderId": "alice", "text": "hi bob!!", "status": "EDITED",
      "createdAt": "...", "editedAt": "...", "editCount": 1 },
    { "messageId": "m2", "seq": 6, "senderId": "bob", "text": null, "status": "DELETED", "deletedAt": "..." }
  ],
  "totalMessages": 2, "ignoredEvents": 0,
  "restore": { "snapshotSeq": 0, "replayedEvents": 8, "durationMicros": 3123, "verifiedAgainstFullReplay": true }
}
```

- **unread count:** 상대방이 보낸 메시지 중 삭제되지 않았고 `seq > lastReadSeq`인 메시지 수다.
- **typing indicator:** 의도적으로 **복원 대상에서 뺐다**. 수 초짜리 휘발성 신호라 원장에 쓰면 이벤트 수가 몇 배로 늘지만, 과거 시점에 "입력 중이었다"는 정보는 쓸모가 없다. WebSocket으로만 전달한다.
- `at`이 세션 생성 이전이면 404 `NO_STATE_AT`을 반환한다.

### 삭제된 메시지와 원문 보존

복원은 **그 시점에 참여자 화면에 보였던 상태**를 재현한다. 그래서 삭제된 메시지의 원문을 따로 꺼내 보여 주지 않는다.

| 복원 시점 | 결과 |
|---|---|
| 삭제 이전 | 원래 내용 그대로 (`SENT` 또는 `EDITED`) |
| 삭제 이후 | `status: DELETED`, `text: null`, `deletedAt` |

수정도 같다. 그 시점의 최신 내용만 보이고, 중간 수정본은 상태에 남기지 않는다(`editCount`만 센다).

**원문은 원장에 남는다.** 원장은 append-only라 `MESSAGE_DELETED`는 "삭제됨"이라는 사실을 추가할 뿐, 원래 `MESSAGE` 이벤트의 `payload.text`를 지우지 않는다. 따라서 원문은 다음 두 경로로 볼 수 있다.

- 삭제 이전 시각으로 `timeline?at=` 복원
- `GET /sessions/{id}/events`로 원장 직접 조회

감사·분쟁 대응에는 이 성질이 필요하다. 다만 현재는 인증이 없어(비목표) 세션 ID만 알면 누구나 두 경로를 쓸 수 있다. 운영에서는 `/events`와 삭제 이전 시점의 복원을 감사 권한으로 제한해야 한다.

**원문을 완전히 지워야 하는 경우** (개인정보 삭제 요청 등)에는 append-only 원칙과 충돌한다. 대안은 다음과 같다.

| 방식 | 내용 | 트레이드오프 |
|---|---|---|
| **암호화 후 키 폐기 (crypto-shredding)** | 메시지 본문을 메시지별(또는 사용자별) 키로 암호화해 저장하고, 삭제 시 키만 폐기한다 | 원장을 고치지 않아 seq·이벤트 수·reducer 결정성이 유지된다. 키 저장소가 필요하고 복호화 비용이 든다 |
| 원장 payload 마스킹 | 삭제 시 원래 `MESSAGE` 행의 `payload.text`를 지운다 | 구현은 단순하지만 append-only가 깨진다. 해당 seq 이후 스냅샷을 다시 만들어야 하고, 마스킹 이전 백업에는 원문이 남는다 |

채택한다면 crypto-shredding이 맞다. 삭제 이전 시점 복원은 "삭제된 메시지(원문 폐기)"로 보이게 되고, 나머지 상태(seq, 참여자, 읽음 포인터)는 그대로 결정적이다.

## 4. 재연결 resume과의 관계

복원 API와 WebSocket resume은 같은 원장과 같은 seq를 쓴다.

- **resume:** "seq k 이후의 이벤트 스트림"
- **timeline:** "seq k 시점의 상태"

클라이언트는 다음 두 방법 중 하나로 화면을 다시 만들 수 있다. 어느 쪽이든 같은 결과다.

- (a) 마지막 상태에 resume 이벤트를 적용한다.
- (b) `timeline?seq=k`로 상태를 받은 뒤 `lastSeq=k`로 연결한다.

(b)는 오래 오프라인이었던 클라이언트처럼, 이벤트 수천 건을 받는 것보다 상태 하나를 받는 편이 싼 경우에 쓴다.
