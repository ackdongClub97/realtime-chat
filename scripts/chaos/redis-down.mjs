// 장애 주입: Redis 중단 (Pub/Sub 팬아웃 + presence 불가)
//   node scripts/chaos/redis-down.mjs
//
// 기대 동작
//  - 같은 인스턴스 사용자끼리는 로컬 직접 전달로 계속 통신 (Redis 비의존)
//  - 다른 인스턴스 사용자에게는 실시간 전달 안 됨 → 원장(DB)에는 저장됨
//  - Redis 복구 후 다음 이벤트 수신 시 gap-fill 로 누락분 자동 보충 (또는 클라이언트 resume)
import { execSync } from 'node:child_process';
import { ChatClient, api, sleep, check, summary } from '../lib/chat-client.mjs';

const APP1 = 'http://localhost:8081';
const APP2 = 'http://localhost:8082';
const sh = (cmd) => execSync(cmd, { stdio: 'pipe' }).toString().trim();

const { body: s } = await api(APP1, 'POST', '/sessions', { user: 'alice', body: { title: 'redis down' } });
await api(APP1, 'POST', `/sessions/${s.id}/join`, { user: 'alice' });
await api(APP1, 'POST', `/sessions/${s.id}/join`, { user: 'bob' });
const alice = new ChatClient({ base: APP1, sessionId: s.id, userId: 'alice' });
const bob = new ChatClient({ base: APP2, sessionId: s.id, userId: 'bob' });
await alice.connect({ lastSeq: 0 });
await bob.connect({ lastSeq: 0 });

console.log('docker compose stop redis');
sh('docker compose stop redis');
const during = await alice.send('MESSAGE', { text: 'sent while redis is down' });
check('Redis 다운 중에도 메시지 저장(ack) 성공', during.status === 'ACCEPTED', `seq=${during.seq}`);
check('alice 본인(같은 인스턴스)에게는 로컬 전달', !!(await alice.waitFor((f) => f.seq === during.seq, 2000).catch(() => null)));
await sleep(1500);
check('bob(다른 인스턴스)은 아직 미수신 (Pub/Sub 불가)', !bob.events.some((e) => e.seq === during.seq));

console.log('docker compose start redis');
sh('docker compose start redis');
await sleep(6000); // listener container 재구독 (recovery interval 2s)
const after = await alice.send('MESSAGE', { text: 'after redis recovery' });
await bob.waitForSeq(after.seq, 10000).catch(() => {});
check('Redis 복구 후 bob 이 누락분까지 순서대로 수신 (서버 gap-fill)',
  bob.events.some((e) => e.seq === during.seq) && bob.events.some((e) => e.seq === after.seq));
check('bob 스트림 gap/중복 없음', bob.isContiguous());

await alice.close();
await bob.close();
summary();
