package com.example.chat.restore;

import java.util.UUID;

import com.example.chat.common.ApiException;
import com.example.chat.common.Json;
import com.example.chat.config.ChatMetrics;
import com.example.chat.store.SessionRepository;
import com.example.chat.store.SnapshotRepository;
import org.springframework.stereotype.Service;

@Service
public class SnapshotService {

    private final RestoreService restore;
    private final SnapshotRepository snapshots;
    private final SessionRepository sessions;
    private final Json json;
    private final ChatMetrics metrics;

    public SnapshotService(RestoreService restore, SnapshotRepository snapshots, SessionRepository sessions, Json json,
                           ChatMetrics metrics) {
        this.restore = restore;
        this.snapshots = snapshots;
        this.sessions = sessions;
        this.json = json;
        this.metrics = metrics;
    }

    /** 현재(last_seq) 시점 스냅샷 생성 — 수동 API 용. */
    public SnapshotCreated createLatest(UUID sessionId) {
        long lastSeq = sessions.findById(sessionId)
                .orElseThrow(() -> ApiException.notFound("SESSION_NOT_FOUND", "session not found: " + sessionId))
                .lastSeq();
        return createAt(sessionId, lastSeq, "manual");
    }

    /**
     * seq 시점 스냅샷 생성. 이전 스냅샷 + 증분 리플레이로 만들기 때문에 비용은 스냅샷 주기에 비례한다.
     * 상태가 seq 에 대해 결정적이라 동시에 여러 번 호출되어도 같은 행이 한 번만 저장된다(멱등).
     */
    public SnapshotCreated createAt(UUID sessionId, long seq, String trigger) {
        RestoreResult r = restore.restoreAtSeq(sessionId, seq);
        if (r.snapshotSeq() == seq) {
            return new SnapshotCreated(seq, false, r.replayedEvents());
        }
        boolean created = snapshots.insert(sessionId, seq, SessionState.SCHEMA_VERSION, json.write(r.state()));
        if (created) {
            metrics.snapshotCreated(trigger);
        }
        return new SnapshotCreated(seq, created, r.replayedEvents());
    }

    public record SnapshotCreated(long seq, boolean created, int replayedEvents) {
    }
}
