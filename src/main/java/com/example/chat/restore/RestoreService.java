package com.example.chat.restore;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import com.example.chat.common.ApiException;
import com.example.chat.common.Json;
import com.example.chat.config.ChatMetrics;
import com.example.chat.domain.StoredEvent;
import com.example.chat.store.EventRepository;
import com.example.chat.store.SessionRepository;
import com.example.chat.store.SessionRow;
import com.example.chat.store.SnapshotRepository;
import com.example.chat.store.SnapshotRepository.Snapshot;
import org.springframework.stereotype.Service;

/**
 * 특정 시점 상태 복원: 가장 가까운 스냅샷 + 이후 이벤트 리플레이.
 *
 * <pre>
 *   at(시각) --(idx_event_session_received)--> cutoffSeq
 *   cutoffSeq --(PK 역방향 1건)--> snapshot(seq &lt;= cutoffSeq)
 *   (snapshot.seq, cutoffSeq] --(PK 범위 스캔)--> events --reducer--> state
 * </pre>
 * 리플레이 비용은 스냅샷 주기(N)로 상한이 정해진다: 최대 N-1 개 + 스냅샷 역직렬화 1회.
 */
@Service
public class RestoreService {

    private static final int PAGE_SIZE = 1000;

    private final SessionRepository sessions;
    private final EventRepository events;
    private final SnapshotRepository snapshots;
    private final Json json;
    private final ChatMetrics metrics;

    public RestoreService(SessionRepository sessions, EventRepository events, SnapshotRepository snapshots, Json json,
                          ChatMetrics metrics) {
        this.sessions = sessions;
        this.events = events;
        this.snapshots = snapshots;
        this.json = json;
        this.metrics = metrics;
    }

    /**
     * @param at  기준 시각 (해당 시각까지 서버가 수신한 이벤트 반영). seq 와 둘 다 없으면 현재 상태.
     * @param seq 기준 seq (디버깅용, at 보다 우선)
     */
    public RestoreResult restore(UUID sessionId, Instant at, Long seq) {
        SessionRow session = sessions.findById(sessionId)
                .orElseThrow(() -> ApiException.notFound("SESSION_NOT_FOUND", "session not found: " + sessionId));
        long cutoff;
        if (seq != null) {
            cutoff = Math.min(seq, session.lastSeq());
        } else if (at != null) {
            cutoff = events.maxSeqAtOrBefore(sessionId, at);
        } else {
            cutoff = session.lastSeq();
        }
        if (cutoff <= 0) {
            throw ApiException.notFound("NO_STATE_AT", "session did not exist at the requested point");
        }
        return metrics.restoreLatency().record(() -> restoreAtSeq(sessionId, cutoff));
    }

    public RestoreResult restoreAtSeq(UUID sessionId, long cutoffSeq) {
        long start = System.nanoTime();
        Snapshot snapshot = snapshots.findLatestAtOrBefore(sessionId, cutoffSeq, SessionState.SCHEMA_VERSION)
                .orElse(null);
        SessionState state = snapshot == null
                ? SessionState.empty(sessionId)
                : json.read(snapshot.stateJson(), SessionState.class);
        int replayed = replayInto(state, cutoffSeq);
        metrics.restoreReplayed(replayed);
        return new RestoreResult(state, snapshot == null ? 0 : snapshot.seq(), replayed,
                (System.nanoTime() - start) / 1000);
    }

    /** 스냅샷을 쓰지 않고 seq 1 부터 전체 리플레이. 스냅샷 경로와 결과가 같은지 검증하는 용도. */
    public SessionState fullReplay(UUID sessionId, long cutoffSeq) {
        SessionState state = SessionState.empty(sessionId);
        replayInto(state, cutoffSeq);
        return state;
    }

    public boolean sameState(SessionState a, SessionState b) {
        return json.toTree(a).equals(json.toTree(b));
    }

    private int replayInto(SessionState state, long cutoffSeq) {
        int replayed = 0;
        while (state.lastSeq < cutoffSeq) {
            List<StoredEvent> page = events.findRange(state.sessionId, state.lastSeq, cutoffSeq, PAGE_SIZE);
            if (page.isEmpty()) {
                throw new IllegalStateException("events missing for session %s in (%d, %d]"
                        .formatted(state.sessionId, state.lastSeq, cutoffSeq));
            }
            for (StoredEvent e : page) {
                SessionReducer.apply(state, e);
                replayed++;
            }
        }
        return replayed;
    }
}
