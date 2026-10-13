// 실시간 시나리오 재현 스크립트
//   node scripts/ws-scenario.mjs                         # 단일 인스턴스 (localhost:8080)
//   A=http://localhost:8081 B=http://localhost:8082 node scripts/ws-scenario.mjs   # 서로 다른 인스턴스에 연결
//
// 검증 항목: 실시간 송수신, 중복 재전송, presence, typing, 재연결 resume, 순서 역전(수정이 원본보다 먼저), 시점 복원
import { ChatClient, api, sleep, check, summary } from './lib/chat-client.mjs';

const A = process.env.A ?? process.env.BASE ?? 'http://localhost:8080';
const B = process.env.B ?? A;
const LOG = process.env.LOG === '1';

console.log(`alice -> ${A}, bob -> ${B}`);
const { body: session } = await api(A, 'POST', '/sessions', { user: 'alice', body: { title: 'ws scenario' } });
const sid = session.id;
await api(A, 'POST', `/sessions/${sid}/join`, { user: 'alice' });
await api(A, 'POST', `/sessions/${sid}/join`, { user: 'bob' });
console.log(`session ${sid}\n`);

const alice = new ChatClient({ base: A, sessionId: sid, userId: 'alice', log: LOG });
const bob = new ChatClient({ base: B, sessionId: sid, userId: 'bob', log: LOG });
await alice.connect({ lastSeq: 0 });
await bob.connect({ lastSeq: 0 });

console.log('1) 실시간 송수신');
const m1 = await alice.send('MESSAGE', { text: 'hello bob' });
await bob.waitForSeq(m1.seq);
check('alice 메시지 ack 수신', m1.status === 'ACCEPTED', `seq=${m1.seq}`);
check('bob 이 같은 seq 로 수신', bob.events.some((e) => e.seq === m1.seq && e.payload.text === 'hello bob'));

console.log('2) 중복 재전송 (네트워크 재시도 시뮬레이션)');
const dup = await alice.send('MESSAGE', { text: 'hello bob' }, { clientEventId: m1.clientEventId });
await sleep(300);
check('재전송 ack = DUPLICATE, 같은 seq', dup.status === 'DUPLICATE' && dup.seq === m1.seq);
check('bob 은 메시지를 한 번만 받음', bob.events.filter((e) => e.clientEventId === m1.clientEventId).length === 1);

console.log('3) typing / presence');
alice.typing(true);
const typing = await bob.waitFor((f) => f.type === 'typing' && f.userId === 'alice').catch(() => null);
check('bob 이 typing 신호 수신 (저장되지 않는 휘발성 신호)', !!typing);

console.log('4) bob 연결 끊김 → alice 가 그동안 3건 전송 → bob 재연결(resume)');
const bobLastSeq = bob.lastSeq;
await bob.close(3001); // 3001: 클라이언트 정의 "네트워크 끊김"
const disc = await alice.waitFor((f) => f.type === 'event' && f.eventType === 'DISCONNECT' && f.userId === 'bob');
check('alice 가 bob DISCONNECT 이벤트 수신', !!disc, `seq=${disc.seq}`);
const offline = await alice.waitFor((f) => f.type === 'presence' && f.userId === 'bob' && f.status === 'OFFLINE').catch(() => null);
check('alice 가 bob OFFLINE presence 수신', !!offline);
const { body: during } = await api(A, 'GET', `/sessions/${sid}`);
check('세션 상태 INTERRUPTED', during.status === 'INTERRUPTED');
const missed = [];
for (let i = 1; i <= 3; i++) missed.push(await alice.send('MESSAGE', { text: `while you were away ${i}` }));
const beforeReconnect = new Date().toISOString();
await sleep(50);

await bob.connect({ lastSeq: bobLastSeq });
const reconnect = await bob.waitFor((f) => f.type === 'event' && f.eventType === 'RECONNECT');
await bob.waitForSeq(reconnect.seq);
check('bob 이 끊긴 동안의 메시지 3건을 resume 으로 수신', missed.every((m) => bob.events.some((e) => e.seq === m.seq)));
check('bob 수신 스트림 gap/중복 없음', bob.isContiguous(), `lastSeq=${bob.lastSeq}`);
const { body: after } = await api(A, 'GET', `/sessions/${sid}`);
check('재연결 후 세션 상태 ACTIVE', after.status === 'ACTIVE');

console.log('5) 순서 뒤바뀜: 수정 이벤트가 원본보다 먼저 도착');
const originalId = crypto.randomUUID();
const early = await bob.send('MESSAGE_EDITED', { messageId: originalId, text: 'edited!' });
check('원본 없는 수정은 TARGET_NOT_FOUND(retryable) 로 거절', early.code === 'TARGET_NOT_FOUND' && early.retryable);
const orig = await bob.send('MESSAGE', { text: 'original' }, { clientEventId: originalId });
const retried = await bob.send('MESSAGE_EDITED', { messageId: originalId, text: 'edited!' }, { clientEventId: early.clientEventId });
check('원본 ack 후 같은 clientEventId 로 재시도하면 수락', retried.status === 'ACCEPTED' && retried.seq > orig.seq);

console.log('6) 읽음 처리 + 시점 복원');
const read = await bob.send('READ', { upToSeq: missed[1].seq });
check('READ 수락', read.status === 'ACCEPTED');
await alice.waitForSeq(read.seq);
const { body: tlNow } = await api(A, 'GET', `/sessions/${sid}/timeline?verify=true`);
const bobView = tlNow.participants.find((p) => p.userId === 'bob');
check('현재 상태: 메시지 5건, original 은 EDITED', tlNow.totalMessages === 5 && tlNow.messages.find((m) => m.messageId === originalId)?.status === 'EDITED');
check('bob unread = 1 (읽음 포인터 이후 alice 메시지 1건)', bobView.unreadCount === 1, `unread=${bobView.unreadCount}`);
check('스냅샷 경로 == 전체 리플레이', tlNow.restore.verifiedAgainstFullReplay === true);
const { body: tlPast } = await api(A, 'GET', `/sessions/${sid}/timeline?at=${encodeURIComponent(beforeReconnect)}`);
check('재연결 직전 시점 복원: bob DISCONNECTED, 세션 INTERRUPTED',
  tlPast.participants.find((p) => p.userId === 'bob')?.state === 'DISCONNECTED' && tlPast.status === 'INTERRUPTED',
  `resolvedSeq=${tlPast.resolvedSeq}`);

console.log('7) 두 클라이언트가 본 이벤트 스트림 일치');
await alice.waitForSeq(bob.lastSeq);
const common = Math.min(alice.lastSeq, bob.lastSeq);
const sig = (c) => c.events.filter((e) => e.seq <= common).map((e) => `${e.seq}:${e.clientEventId}`).join(',');
check('alice/bob 이 같은 순서의 같은 이벤트를 받음', alice.isContiguous() && sig(alice) === sig(bob),
  `common=${common}`);

await alice.close();
await bob.close();
summary();
