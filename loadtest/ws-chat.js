// WebSocket 메시징 부하 테스트
//   k6 run -e BASE=http://localhost:8080 -e SESSIONS=100 -e DURATION=60s -e RATE_MS=1000 loadtest/ws-chat.js
//
// 세션 SESSIONS 개 × 참여자 2명 = VU 2*SESSIONS 개. 각 VU 는 RATE_MS 마다 메시지 1건 전송.
// nginx 라운드로빈으로 두 사용자가 서로 다른 인스턴스에 붙는 경우가 많다 → Redis 팬아웃 경로 포함.
//
// 측정
//  - ack_latency : send → ack (append 트랜잭션 + 왕복)
//  - e2e_latency : 송신자 send → 상대방 수신 (팬아웃 포함 end-to-end)
//  - seq_gap     : 수신 스트림의 seq 구멍 (0 이어야 함)
import ws from 'k6/ws';
import http from 'k6/http';
import { check } from 'k6';
import { Trend, Counter } from 'k6/metrics';

const BASE = __ENV.BASE || 'http://localhost:8080';
const SESSIONS = Number(__ENV.SESSIONS || 100);
const DURATION_MS = parseDuration(__ENV.DURATION || '60s');
const RATE_MS = Number(__ENV.RATE_MS || 1000);

const ackLatency = new Trend('ack_latency', true);
const e2eLatency = new Trend('e2e_latency', true);
const sent = new Counter('messages_sent');
const received = new Counter('messages_received');
const seqGap = new Counter('seq_gap');
const errors = new Counter('ws_errors');

export const options = {
  scenarios: {
    chat: { executor: 'per-vu-iterations', vus: SESSIONS * 2, iterations: 1, maxDuration: `${DURATION_MS / 1000 + 60}s` },
  },
  thresholds: {
    ack_latency: ['p(95)<200'],
    e2e_latency: ['p(95)<300'],
    seq_gap: ['count==0'],
  },
};

function parseDuration(s) {
  const m = /^(\d+)(ms|s|m)$/.exec(s);
  return Number(m[1]) * { ms: 1, s: 1000, m: 60000 }[m[2]];
}

export function setup() {
  const headers = { 'Content-Type': 'application/json' };
  const ids = [];
  for (let i = 0; i < SESSIONS; i++) {
    const r = http.post(`${BASE}/sessions`, JSON.stringify({ title: `load ${i}` }),
      { headers: { ...headers, 'X-User-Id': `alice-${i}` } });
    const id = r.json('id');
    http.post(`${BASE}/sessions/${id}/join`, null, { headers: { 'X-User-Id': `alice-${i}` } });
    http.post(`${BASE}/sessions/${id}/join`, null, { headers: { 'X-User-Id': `bob-${i}` } });
    ids.push(id);
  }
  return { ids };
}

export default function (data) {
  const idx = Math.floor((__VU - 1) / 2);
  const user = (__VU - 1) % 2 === 0 ? `alice-${idx}` : `bob-${idx}`;
  const sid = data.ids[idx];
  const url = `${BASE.replace(/^http/, 'ws')}/ws?sessionId=${sid}&userId=${user}&lastSeq=0`;
  const pending = {};
  let lastSeq = null;
  let n = 0;

  const res = ws.connect(url, {}, (socket) => {
    socket.on('message', (raw) => {
      const f = JSON.parse(raw);
      if (f.type === 'ack') {
        const t = pending[f.clientEventId];
        if (t) { ackLatency.add(Date.now() - t); delete pending[f.clientEventId]; }
      } else if (f.type === 'event') {
        if (lastSeq !== null && f.seq !== lastSeq + 1) seqGap.add(1);
        lastSeq = f.seq;
        if (f.eventType === 'MESSAGE' && f.userId !== user && f.payload.sentAt) {
          received.add(1);
          e2eLatency.add(Date.now() - f.payload.sentAt);
        }
      } else if (f.type === 'error') {
        errors.add(1);
      }
    });
    socket.on('open', () => {
      // 시작 시점 분산
      socket.setTimeout(() => {
        socket.setInterval(() => {
          const id = `${user}-${__VU}-${n++}-${Date.now()}`;
          pending[id] = Date.now();
          socket.send(JSON.stringify({ type: 'send', clientEventId: id, eventType: 'MESSAGE', clientSeq: n,
            payload: { text: `load message ${n}`, sentAt: Date.now() } }));
          sent.add(1);
        }, RATE_MS);
      }, Math.random() * RATE_MS);
      socket.setTimeout(() => socket.close(), DURATION_MS);
    });
    socket.on('error', () => errors.add(1));
  });
  check(res, { 'ws status 101': (r) => r && r.status === 101 });
}
