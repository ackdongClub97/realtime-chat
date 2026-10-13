// 재사용 가능한 WebSocket 채팅 클라이언트 (Node 22+ 내장 WebSocket 사용, 외부 의존성 없음)
//
// 클라이언트 측 정합성 규칙 (docs/websocket-protocol.md 참고)
//  - 이벤트마다 clientEventId(UUID) 를 붙이고, ack 를 못 받으면 "같은 id 로" 재전송한다.
//  - 수신 이벤트는 seq 로 관리한다: seq <= lastSeq 는 버리고, 구멍이 보이면 resume 을 요청한다.
//  - 재연결 시 lastSeq 를 넘겨 끊긴 동안의 이벤트를 받는다.
import { randomUUID } from 'node:crypto';

export class ChatClient {
  constructor({ base, sessionId, userId, log = false }) {
    this.base = base.replace(/^http/, 'ws');
    this.sessionId = sessionId;
    this.userId = userId;
    this.log = log;
    this.lastSeq = null;
    this.events = [];        // 수신한 이벤트 (seq 순, 중복 없음)
    this.frames = [];        // 수신한 모든 프레임 (디버깅용)
    this.pending = new Map(); // clientEventId -> {resolve, reject}
    this.clientSeq = 0;
    this.duplicatesDropped = 0;
    this.gapsDetected = 0;
    this.waiters = [];
  }

  connect({ lastSeq } = {}) {
    const resume = lastSeq ?? this.lastSeq;
    const qs = new URLSearchParams({ sessionId: this.sessionId, userId: this.userId });
    if (resume !== null && resume !== undefined) qs.set('lastSeq', String(resume));
    return new Promise((resolve, reject) => {
      const ws = new WebSocket(`${this.base}/ws?${qs}`);
      this.ws = ws;
      const timer = setTimeout(() => reject(new Error('connect timeout')), 5000);
      ws.onmessage = (m) => {
        const f = JSON.parse(m.data);
        if (f.type === 'welcome') {
          clearTimeout(timer);
          this.connId = f.connId;
          if (this.lastSeq === null) this.lastSeq = f.resumeFrom;
          resolve(f);
        }
        this.onFrame(f);
      };
      ws.onclose = (e) => {
        clearTimeout(timer);
        this.closed = { code: e.code, reason: e.reason };
        reject(new Error(`closed ${e.code} ${e.reason}`));
      };
      ws.onerror = () => {};
    });
  }

  onFrame(f) {
    this.frames.push(f);
    if (this.log) console.log(`  [${this.userId}] <=`, JSON.stringify(f));
    if (f.type === 'event') {
      if (this.lastSeq !== null && f.seq <= this.lastSeq) { this.duplicatesDropped++; return; }
      if (this.lastSeq !== null && f.seq > this.lastSeq + 1) {
        this.gapsDetected++;
        this.raw({ type: 'resume', lastSeq: this.lastSeq });
        return;
      }
      this.lastSeq = f.seq;
      this.events.push(f);
    }
    if (f.type === 'ack' || f.type === 'error') {
      const p = this.pending.get(f.clientEventId);
      if (p) { this.pending.delete(f.clientEventId); p.resolve(f); }
    }
    this.waiters = this.waiters.filter((w) => !w(f));
  }

  raw(frame) { this.ws.send(JSON.stringify(frame)); }

  /** ack 를 받을 때까지 같은 clientEventId 로 재전송 (at-least-once + 서버 멱등 = effectively-once) */
  async send(eventType, payload, { clientEventId = randomUUID(), retries = 3, timeoutMs = 2000 } = {}) {
    const clientSeq = ++this.clientSeq;
    for (let attempt = 0; attempt <= retries; attempt++) {
      const result = await new Promise((resolve) => {
        this.pending.set(clientEventId, { resolve });
        this.raw({ type: 'send', clientEventId, eventType, payload, clientSeq, clientTs: new Date().toISOString() });
        setTimeout(() => resolve(null), timeoutMs);
      });
      if (result) return { clientEventId, ...result };
    }
    throw new Error(`no ack for ${clientEventId}`);
  }

  typing(on = true) { this.raw({ type: 'send', eventType: 'TYPING', payload: { typing: on } }); }

  waitFor(predicate, timeoutMs = 5000) {
    const found = this.frames.find(predicate);
    if (found) return Promise.resolve(found);
    return new Promise((resolve, reject) => {
      const t = setTimeout(() => reject(new Error('waitFor timeout')), timeoutMs);
      this.waiters.push((f) => { if (predicate(f)) { clearTimeout(t); resolve(f); return true; } return false; });
    });
  }

  waitForSeq(seq, timeoutMs = 5000) {
    return this.waitFor((f) => f.type === 'event' && f.seq >= seq && this.lastSeq >= seq, timeoutMs)
      .catch(() => { if (this.lastSeq >= seq) return; throw new Error(`waitForSeq ${seq} timeout (lastSeq=${this.lastSeq})`); });
  }

  close(code = 1000) {
    return new Promise((resolve) => {
      if (!this.ws || this.ws.readyState === WebSocket.CLOSED) return resolve();
      this.ws.onclose = (e) => { this.closed = { code: e.code }; resolve(); };
      this.ws.close(code);
    });
  }

  /** 수신한 seq 스트림이 1씩 증가하는지 (gap-free, no-dup, in-order) */
  isContiguous() {
    for (let i = 1; i < this.events.length; i++) {
      if (this.events[i].seq !== this.events[i - 1].seq + 1) return false;
    }
    return true;
  }
}

export async function api(base, method, path, { user, body, headers = {} } = {}) {
  const res = await fetch(`${base}${path}`, {
    method,
    headers: { 'Content-Type': 'application/json', ...(user ? { 'X-User-Id': user } : {}), ...headers },
    body: body ? JSON.stringify(body) : undefined,
  });
  const text = await res.text();
  return { status: res.status, body: text ? JSON.parse(text) : null };
}

export const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

let failures = 0;
export function check(name, cond, detail = '') {
  console.log(`  ${cond ? 'PASS' : 'FAIL'}  ${name}${detail ? `  (${detail})` : ''}`);
  if (!cond) failures++;
}
export function summary() {
  console.log(failures === 0 ? '\nALL CHECKS PASSED' : `\n${failures} CHECK(S) FAILED`);
  process.exitCode = failures === 0 ? 0 : 1;
}
