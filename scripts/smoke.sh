#!/usr/bin/env bash
# REST 스모크 테스트: 세션 생성(멱등) → join(정원/멱등) → 이벤트 수집(중복/순서역전/위조) → 복원
set -euo pipefail
BASE=${BASE:-http://localhost:8080}
J=(-H 'Content-Type: application/json')
pp() { python3 -m json.tool; }

KEY="smoke-$(date +%s)"
echo "== 세션 생성 (같은 Idempotency-Key 로 2회)"
SID=$(curl -s -XPOST "$BASE/sessions" -H 'X-User-Id: alice' -H "Idempotency-Key: $KEY" "${J[@]}" -d '{"title":"smoke"}' \
  | python3 -c 'import sys,json;print(json.load(sys.stdin)["id"])')
curl -s -o /dev/null -w "  재요청 HTTP %{http_code} (200 = 기존 세션 반환)\n" -XPOST "$BASE/sessions" -H 'X-User-Id: alice' \
  -H "Idempotency-Key: $KEY" "${J[@]}" -d '{"title":"smoke"}'
echo "  sessionId=$SID"

echo "== join: alice, bob, bob(중복→NOOP), carol(정원 초과→409)"
for u in alice bob bob carol; do
  printf "  %-6s " "$u"; curl -s -XPOST "$BASE/sessions/$SID/join" -H "X-User-Id: $u"; echo
done

echo "== 이벤트 수집: 중복 m1, 원본보다 먼저 온 수정(m9), 정상 수정"
curl -s -XPOST "$BASE/sessions/$SID/events" -H 'X-User-Id: alice' "${J[@]}" -d '{"events":[
  {"clientEventId":"m1","type":"MESSAGE","payload":{"text":"hi bob"},"clientSeq":1},
  {"clientEventId":"m1","type":"MESSAGE","payload":{"text":"hi bob"},"clientSeq":1},
  {"clientEventId":"e9","type":"MESSAGE_EDITED","payload":{"messageId":"m9","text":"x"},"clientSeq":2},
  {"clientEventId":"e1","type":"MESSAGE_EDITED","payload":{"messageId":"m1","text":"hi bob!!"},"clientSeq":3}]}' | pp

echo "== bob: 메시지, 읽음, 같은 키로 다른 내용(→409 IDEMPOTENCY_KEY_REUSED), 남의 메시지 삭제(→403)"
curl -s -XPOST "$BASE/sessions/$SID/events" -H 'X-User-Id: bob' "${J[@]}" -d '{"events":[
  {"clientEventId":"m2","type":"MESSAGE","payload":{"text":"hey alice"}},
  {"clientEventId":"r1","type":"READ","payload":{"upToSeq":5}},
  {"clientEventId":"m1","type":"MESSAGE","payload":{"text":"spoof"}},
  {"clientEventId":"d1","type":"MESSAGE_DELETED","payload":{"messageId":"m1"}}]}' | pp

sleep 1
echo "== 현재 상태 복원 (verify=true: 스냅샷 경로 == 전체 리플레이)"
curl -s "$BASE/sessions/$SID/timeline?verify=true" | pp
echo "== seq=3 시점 복원 (bob 입장 직후, 메시지 없음)"
curl -s "$BASE/sessions/$SID/timeline?seq=3" | python3 -c 'import sys,json;d=json.load(sys.stdin);print("  resolvedSeq",d["resolvedSeq"],"participants",[p["userId"] for p in d["participants"]],"messages",d["totalMessages"])'
echo "== read model (비동기 projection)"
curl -s "$BASE/sessions/$SID/messages" | pp
echo "$SID"
