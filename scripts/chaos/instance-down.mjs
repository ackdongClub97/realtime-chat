// 장애 주입: 서버 인스턴스 강제 종료 (SIGKILL — 정상 종료 처리 기회 없음)
//   docker compose up -d --build 후 실행: node scripts/chaos/instance-down.mjs
//
// 기대 동작
//  감지: app2 하트비트 키(TTL 15s) 만료 → app1 sweeper 가 app2 소속 연결을 DISCONNECT(reason=instance_down) 기록
//  완화: alice(app1) 는 계속 메시지 전송 가능, bob 은 LB 를 통해 살아있는 app1 로 재연결
//  복구: bob 이 lastSeq 로 resume → 끊긴 동안의 메시지를 누락/중복 없이 수신
import { execSync } from 'node:child_process';
import { ChatClient, api, sleep, check, summary } from '../lib/chat-client.mjs';

const APP1 = 'http://localhost:8081';
const APP2 = 'http://localhost:8082';
const sh = (cmd) => execSync(cmd, { stdio: 'pipe' }).toString().trim();

const { body: s } = await api(APP1, 'POST', '/sessions', { user: 'alice', body: { title: 'instance down' } });
await api(APP1, 'POST', `/sessions/${s.id}/join`, { user: 'alice' });
await api(APP1, 'POST', `/sessions/${s.id}/join`, { user: 'bob' });

const alice = new ChatClient({ base: APP1, sessionId: s.id, userId: 'alice' });
const bob = new ChatClient({ base: APP2, sessionId: s.id, userId: 'bob' });
await alice.connect({ lastSeq: 0 });
await bob.connect({ lastSeq: 0 });
const m = await alice.send('MESSAGE', { text: 'before crash' });
await bob.waitForSeq(m.seq);
const bobLastSeq = bob.lastSeq;

console.log('docker compose kill app2 (SIGKILL)');
const t0 = Date.now();
sh('docker compose kill app2');

const missed = [];
for (let i = 1; i <= 3; i++) missed.push(await alice.send('MESSAGE', { text: `during outage ${i}` }));
check('app2 다운 중에도 alice(app1) 송신 가능', missed.every((x) => x.status === 'ACCEPTED'));

const disc = await alice.waitFor((f) => f.type === 'event' && f.eventType === 'DISCONNECT' && f.userId === 'bob', 40000)
  .catch(() => null);
const detectSec = ((Date.now() - t0) / 1000).toFixed(1);
check('sweeper 가 bob 연결을 DISCONNECT 로 기록', disc?.payload?.reason === 'instance_down', `감지 ${detectSec}s, reason=${disc?.payload?.reason}`);
const { body: st } = await api(APP1, 'GET', `/sessions/${s.id}`);
check('세션 상태 INTERRUPTED, bob presence OFFLINE',
  st.status === 'INTERRUPTED' && st.participants.find((p) => p.userId === 'bob').presence === 'OFFLINE');

console.log('bob 이 살아있는 app1 로 재연결 (lastSeq resume)');
const bob2 = new ChatClient({ base: APP1, sessionId: s.id, userId: 'bob' });
bob2.lastSeq = bobLastSeq;
await bob2.connect();
const rec = await bob2.waitFor((f) => f.type === 'event' && f.eventType === 'RECONNECT');
await bob2.waitForSeq(rec.seq);
check('끊긴 동안 메시지 3건 resume 수신', missed.every((x) => bob2.events.some((e) => e.seq === x.seq)));
check('resume 스트림 gap/중복 없음', bob2.isContiguous(), `seq ${bob2.events[0]?.seq}..${bob2.lastSeq}`);
const { body: st2 } = await api(APP1, 'GET', `/sessions/${s.id}`);
check('세션 상태 ACTIVE 복귀', st2.status === 'ACTIVE');

console.log('docker compose start app2');
sh('docker compose start app2');
await alice.close();
await bob2.close();
summary();
