// 조회 핫패스 부하 테스트: 시점 복원(timeline) / 대화창(messages) / 세션 목록
//   k6 run -e BASE=http://localhost:8080 -e EVENTS=5000 loadtest/read-paths.js
//
// setup 에서 메시지 EVENTS 건이 쌓인 세션을 만든 뒤 임의 시점 복원을 반복한다.
// 스냅샷(주기 100) 덕분에 세션 길이와 무관하게 복원 비용이 일정한지 확인하는 것이 목적.
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend } from 'k6/metrics';

const BASE = __ENV.BASE || 'http://localhost:8080';
const EVENTS = Number(__ENV.EVENTS || 5000);
const replayed = new Trend('timeline_replayed_events');

// p95 기준(ms). 기본값은 k6 와 서버가 같은 호스트에서 CPU 를 나눠 쓰는 로컬 환경의 회귀 감지용이다
// (2026-10-13 실측 timeline 183 / messages 60 / list 46ms 에 여유를 둠). 원래 목표인 100 / 50 / 50ms 는
// 부하 발생기를 분리한 환경 기준이며 -e TIMELINE_P95=100 -e MESSAGES_P95=50 -e LIST_P95=50 으로 적용한다.
// 근거: docs/load-test-results.md 2·4절
const TIMELINE_P95 = Number(__ENV.TIMELINE_P95 || 300);
const MESSAGES_P95 = Number(__ENV.MESSAGES_P95 || 100);
const LIST_P95 = Number(__ENV.LIST_P95 || 100);

export const options = {
  setupTimeout: '300s',
  scenarios: {
    timeline: { executor: 'constant-vus', vus: 20, duration: '30s', exec: 'timeline' },
    messages: { executor: 'constant-vus', vus: 20, duration: '30s', exec: 'messages' },
    list: { executor: 'constant-vus', vus: 10, duration: '30s', exec: 'list' },
  },
  thresholds: {
    'http_req_duration{name:timeline}': [`p(95)<${TIMELINE_P95}`],
    'http_req_duration{name:messages}': [`p(95)<${MESSAGES_P95}`],
    'http_req_duration{name:list}': [`p(95)<${LIST_P95}`],
    http_req_failed: ['rate<0.01'],
  },
};

export function setup() {
  const h = { 'Content-Type': 'application/json' };
  const id = http.post(`${BASE}/sessions`, JSON.stringify({ title: 'read-paths' }),
    { headers: { ...h, 'X-User-Id': 'alice' } }).json('id');
  http.post(`${BASE}/sessions/${id}/join`, null, { headers: { 'X-User-Id': 'alice' } });
  http.post(`${BASE}/sessions/${id}/join`, null, { headers: { 'X-User-Id': 'bob' } });
  for (let sent = 0; sent < EVENTS; sent += 100) {
    const events = [];
    for (let i = 0; i < 100; i++) {
      events.push({ clientEventId: `rp-${id}-${sent + i}`, type: 'MESSAGE', payload: { text: `message ${sent + i}` } });
    }
    http.post(`${BASE}/sessions/${id}/events`, JSON.stringify({ events }), { headers: { ...h, 'X-User-Id': 'alice' } });
  }
  sleep(5); // projection / snapshot 따라잡기
  const lastSeq = http.get(`${BASE}/sessions/${id}`).json('lastSeq');
  return { id, lastSeq };
}

export function timeline(d) {
  const seq = 1 + Math.floor(Math.random() * d.lastSeq);
  const r = http.get(`${BASE}/sessions/${d.id}/timeline?seq=${seq}&limit=50`, { tags: { name: 'timeline' } });
  check(r, { 'timeline 200': (x) => x.status === 200 });
  if (r.status === 200) replayed.add(r.json('restore.replayedEvents'));
}

export function messages(d) {
  const before = Math.random() < 0.5 ? '' : `&beforeSeq=${1 + Math.floor(Math.random() * d.lastSeq)}`;
  const r = http.get(`${BASE}/sessions/${d.id}/messages?limit=50${before}`, { tags: { name: 'messages' } });
  check(r, { 'messages 200': (x) => x.status === 200 });
}

export function list() {
  const r = http.get(`${BASE}/sessions?status=ACTIVE&limit=20`, { tags: { name: 'list' } });
  check(r, { 'list 200': (x) => x.status === 200 });
}
