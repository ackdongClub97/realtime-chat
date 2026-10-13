// 장애 주입: DB 일시 정지 (응답 없음 — 커넥션 고갈/락 대기와 유사한 상황)
//   node scripts/chaos/db-pause.mjs
//
// 기대 동작
//  - append 실패 시 서버는 재시도 가능한 오류(DB_UNAVAILABLE, retryable=true)를 약 5초 후 반환
//    (이미 풀에 있는 커넥션의 쿼리가 응답을 못 받으므로 JDBC socketTimeout=5s 가 적용된다.
//     Hikari connection-timeout=3s 는 풀이 고갈돼 커넥션을 기다릴 때만 적용된다)
//  - 클라이언트는 같은 clientEventId 로 재전송 → DB 복구 후 정확히 한 번만 저장
import { execSync } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { ChatClient, api, sleep, check, summary } from '../lib/chat-client.mjs';

const APP1 = 'http://localhost:8081';
const sh = (cmd) => execSync(cmd, { stdio: 'pipe' }).toString().trim();

const { body: s } = await api(APP1, 'POST', '/sessions', { user: 'alice', body: { title: 'db pause' } });
await api(APP1, 'POST', `/sessions/${s.id}/join`, { user: 'alice' });
await api(APP1, 'POST', `/sessions/${s.id}/join`, { user: 'bob' });
const alice = new ChatClient({ base: APP1, sessionId: s.id, userId: 'alice' });
await alice.connect({ lastSeq: 0 });

console.log('docker compose pause postgres');
sh('docker compose pause postgres');
const id = randomUUID();
const t0 = Date.now();
const r1 = await alice.send('MESSAGE', { text: 'during db outage' }, { clientEventId: id, retries: 0, timeoutMs: 15000 });
check('DB 장애 시 retryable 오류를 빠르게 반환', r1.type === 'error' && r1.retryable === true,
  `${r1.code} after ${Date.now() - t0}ms`);

console.log('docker compose unpause postgres');
sh('docker compose unpause postgres');
await sleep(1000);
const r2 = await alice.send('MESSAGE', { text: 'during db outage' }, { clientEventId: id });
const r3 = await alice.send('MESSAGE', { text: 'during db outage' }, { clientEventId: id });
check('복구 후 같은 clientEventId 재전송 → 저장', r2.status === 'ACCEPTED' || r2.status === 'DUPLICATE', `seq=${r2.seq}`);
check('한 번 더 재전송해도 같은 seq (중복 저장 없음)', r3.status === 'DUPLICATE' && r3.seq === r2.seq);
const { body: ev } = await api(APP1, 'GET', `/sessions/${s.id}/events`);
check('원장에 해당 메시지 1건', ev.items.filter((e) => e.clientEventId === id).length === 1);

await alice.close();
summary();
