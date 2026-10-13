// 대량 이벤트 시드: 세션 1개에 N 개 메시지 (스냅샷 자동화 / 복원 성능 확인용)
//   N=1000 node scripts/seed.mjs
import { api } from './lib/chat-client.mjs';

const BASE = process.env.BASE ?? 'http://localhost:8080';
const N = Number(process.env.N ?? 1000);
const { body: s } = await api(BASE, 'POST', '/sessions', { user: 'alice', body: { title: `seed ${N}` } });
await api(BASE, 'POST', `/sessions/${s.id}/join`, { user: 'alice' });
await api(BASE, 'POST', `/sessions/${s.id}/join`, { user: 'bob' });
let sent = 0;
while (sent < N) {
  const batch = [];
  for (let i = 0; i < 100 && sent < N; i++, sent++) {
    const user = sent % 2 ? 'bob' : 'alice';
    batch.push({ user, e: { clientEventId: `seed-${s.id}-${sent}`, type: 'MESSAGE', payload: { text: `message ${sent}` } } });
  }
  for (const user of ['alice', 'bob']) {
    const events = batch.filter((b) => b.user === user).map((b) => b.e);
    if (events.length) await api(BASE, 'POST', `/sessions/${s.id}/events`, { user, body: { events } });
  }
}
console.log(s.id);
