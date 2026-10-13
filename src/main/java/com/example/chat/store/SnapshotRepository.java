package com.example.chat.store;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class SnapshotRepository {

    private final JdbcTemplate jdbc;

    public SnapshotRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** 상태는 seq 에 대해 결정적이므로 같은 seq 스냅샷을 동시에 만들어도 결과가 같다 → DO NOTHING 으로 충분. */
    public boolean insert(UUID sessionId, long seq, int schemaVersion, String stateJson) {
        return jdbc.update("""
                INSERT INTO session_snapshot (session_id, seq, schema_version, state)
                VALUES (?, ?, ?, ?::jsonb)
                ON CONFLICT (session_id, seq) DO NOTHING
                """, sessionId, seq, schemaVersion, stateJson) > 0;
    }

    /** 복원 시작점: cutoff 이하에서 가장 최신 스냅샷. PK (session_id, seq) 역방향 스캔 1건. */
    public Optional<Snapshot> findLatestAtOrBefore(UUID sessionId, long cutoffSeq, int schemaVersion) {
        return jdbc.query("""
                SELECT seq, schema_version, state::text AS state, created_at
                  FROM session_snapshot
                 WHERE session_id = ? AND seq <= ? AND schema_version = ?
                 ORDER BY seq DESC
                 LIMIT 1
                """, (rs, i) -> new Snapshot(sessionId, rs.getLong("seq"), rs.getInt("schema_version"),
                        rs.getString("state"), rs.getTimestamp("created_at").toInstant()),
                sessionId, cutoffSeq, schemaVersion).stream().findFirst();
    }

    public List<SnapshotInfo> list(UUID sessionId) {
        return jdbc.query("""
                SELECT seq, schema_version, pg_column_size(state) AS size_bytes, created_at
                  FROM session_snapshot
                 WHERE session_id = ?
                 ORDER BY seq DESC
                """, (rs, i) -> new SnapshotInfo(rs.getLong("seq"), rs.getInt("schema_version"),
                rs.getInt("size_bytes"), rs.getTimestamp("created_at").toInstant()), sessionId);
    }

    public record Snapshot(UUID sessionId, long seq, int schemaVersion, String stateJson, Instant createdAt) {
    }

    public record SnapshotInfo(long seq, int schemaVersion, int sizeBytes, Instant createdAt) {
    }
}
