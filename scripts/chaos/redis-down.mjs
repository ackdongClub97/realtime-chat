// 장애 주입: Redis 중단 (Pub/Sub 팬아웃 + presence 불가)
//   node scripts/chaos/redis-down.mjs
//
// 기대 동작
//  - 같은 인스턴스 사용자끼리는 로컬 직접 전달로 계속 통신 (Redis 비의존)
//  - 다른 인스턴스 사용자에게는 실시간 전달 안 됨 → 원장(DB)에는 저장됨
//  - 다음 메시지가 없어도 클라이언트 ping 시 서버가 원장과 비교해 누락분을 보낸다 (Redis 비의존)
//  - Redis 복구 후에는 실시간 전달이 다시 동작하고, 스트림에 gap/중복이 없다
import { execSync } from 'node:child_process';
import { ChatClient, api, sleep, check, summary } from '../lib/chat-client.mjs';

const APP1 = 'http://localhost:8081';
const APP2 = 'http://localhost:8082';
const sh = (cmd) => execSync(cmd, { stdio: 'pipe' }).toString().trim();

const { body: s } = await api(APP1, 'POST', '/sessions', { user: 'alice', body: { title: 'redis down' } });
await api(APP1, 'POST', `/sessions/${s.id}/join`, { user: 'alice' });
await api(APP1, 'POST', `/sessions/${s.id}/join`, { user: 'bob' });
const alice = new ChatClient({ base: APP1, sessionId: s.id, userId: 'alice' });
// ping 을 직접 보내 시점을 통제한다 (자동 ping 끄기)
const bob = new ChatClient({ base: APP2, sessionId: s.id, userId: 'bob', pingMs: 0 });
await alice.connect({ lastSeq: 0 });
await bob.connect({ lastSeq: 0 });

console.log('docker compose stop redis');
sh('docker compose stop redis');
const during = await alice.send('MESSAGE', { text: 'sent while redis is down' });
check('Redis 다운 중에도 메시지 저장(ack) 성공', during.status === 'ACCEPTED', `seq=${during.seq}`);
check('alice 본인(같은 인스턴스)에게는 로컬 전달', !!(await alice.waitFor((f) => f.seq === during.seq, 2000).catch(() => null)));
await sleep(1500);
check('bob(다른 인스턴스)은 실시간으로 미수신 (Pub/Sub 불가)', !bob.events.some((e) => e.seq === during.seq));

// 조용한 대화: 이후 메시지를 보내지 않는다. bob 의 ping 만으로 누락을 발견하고 복구해야 한다.
const t0 = Date.now();
bob.raw({ type: 'ping' });
const pong = await bob.waitFor((f) => f.type === 'pong', 3000).catch(() => null);
check('새 메시지 없이 ping 만으로 누락 메시지 복구 (Redis 다운 중)',
  bob.events.some((e) => e.seq === during.seq) && pong?.serverLastSeq === during.seq,
  `${Date.now() - t0}ms, serverLastSeq=${pong?.serverLastSeq}`);

console.log('docker compose start redis');
sh('docker compose start redis');
await sleep(6000); // listener container 재구독 (recovery interval 2s)
const after = await alice.send('MESSAGE', { text: 'after redis recovery' });
await bob.waitForSeq(after.seq, 10000).catch(() => {});
check('Redis 복구 후 실시간 전달 재개', bob.events.some((e) => e.seq === after.seq));
check('bob 스트림 gap/중복 없음', bob.isContiguous());

await alice.close();
await bob.close();
summary();
