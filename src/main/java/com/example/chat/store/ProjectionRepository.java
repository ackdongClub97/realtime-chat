package com.example.chat.store;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class ProjectionRepository {

    public static final String MESSAGES = "messages";

    private final JdbcTemplate jdbc;

    public ProjectionRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * 세션의 projection 체크포인트를 잠근다. 같은 세션의 projection 갱신을 직렬화해
     * 여러 워커가 동시에 처리해도 이벤트가 seq 순서대로 정확히 한 번 반영된다.
     */
    public Offset lockOffset(UUID sessionId, String projection) {
        jdbc.update("""
                INSERT INTO projection_offset (session_id, projection) VALUES (?, ?)
                ON CONFLICT DO NOTHING
                """, sessionId, projection);
        return jdbc.queryForObject("""
                SELECT last_applied_seq, last_snapshot_seq FROM projection_offset
                 WHERE session_id = ? AND projection = ? FOR UPDATE
                """, (rs, i) -> new Offset(rs.getLong(1), rs.getLong(2)), sessionId, projection);
    }

    public void saveOffset(UUID sessionId, String projection, long lastApplied, long lastSnapshot) {
        jdbc.update("""
                UPDATE projection_offset
                   SET last_applied_seq = ?, last_snapshot_seq = ?, updated_at = now()
                 WHERE session_id = ? AND projection = ?
                """, lastApplied, lastSnapshot, sessionId, projection);
    }

    public long appliedSeq(UUID sessionId, String projection) {
        List<Long> r = jdbc.queryForList("""
                SELECT last_applied_seq FROM projection_offset WHERE session_id = ? AND projection = ?
                """, Long.class, sessionId, projection);
        return r.isEmpty() ? 0 : r.getFirst();
    }

    public void insertMessage(UUID sessionId, String messageId, long seq, String senderId, String content,
                              Instant at) {
        jdbc.update("""
                INSERT INTO message_projection (session_id, message_id, seq, sender_id, content, status,
                                                created_at, updated_at, updated_seq)
                VALUES (?, ?, ?, ?, ?, 'SENT', ?, ?, ?)
                ON CONFLICT (session_id, message_id) DO NOTHING
                """, sessionId, messageId, seq, senderId, content, Timestamp.from(at), Timestamp.from(at), seq);
    }

    /** updated_seq &lt; ? 조건으로 같은 이벤트의 재적용을 막는다 (오프셋과 별개의 이중 안전장치). */
    public void editMessage(UUID sessionId, String messageId, String senderId, String content, Instant at,
                            long seq) {
        jdbc.update("""
                UPDATE message_projection
                   SET content = ?, status = 'EDITED', updated_at = ?, updated_seq = ?
                 WHERE session_id = ? AND message_id = ? AND sender_id = ?
                   AND status <> 'DELETED' AND updated_seq < ?
                """, content, Timestamp.from(at), seq, sessionId, messageId, senderId, seq);
    }

    public void deleteMessage(UUID sessionId, String messageId, String senderId, Instant at, long seq) {
        jdbc.update("""
                UPDATE message_projection
                   SET content = NULL, status = 'DELETED', updated_at = ?, updated_seq = ?
                 WHERE session_id = ? AND message_id = ? AND sender_id = ?
                   AND status <> 'DELETED' AND updated_seq < ?
                """, Timestamp.from(at), seq, sessionId, messageId, senderId, seq);
    }

    /**
     * 대화창 페이지 조회 (핫패스). idx_msgproj_session_seq (session_id, seq DESC) 범위 스캔 + LIMIT.
     */
    public List<MessageRow> page(UUID sessionId, long beforeSeq, int limit) {
        return jdbc.query("""
                SELECT message_id, seq, sender_id, content, status, created_at, updated_at
                  FROM message_projection
                 WHERE session_id = ? AND seq < ?
                 ORDER BY seq DESC
                 LIMIT ?
                """, (rs, i) -> new MessageRow(rs.getString("message_id"), rs.getLong("seq"),
                rs.getString("sender_id"), rs.getString("content"), rs.getString("status"),
                rs.getTimestamp("created_at").toInstant(), rs.getTimestamp("updated_at").toInstant()),
                sessionId, beforeSeq, limit);
    }

    /** projection 재구축: read model 은 원장에서 언제든 다시 만들 수 있다. */
    public void reset(UUID sessionId, String projection) {
        jdbc.update("DELETE FROM message_projection WHERE session_id = ?", sessionId);
        jdbc.update("""
                UPDATE projection_offset SET last_applied_seq = 0, updated_at = now()
                 WHERE session_id = ? AND projection = ?
                """, sessionId, projection);
    }

    public record Offset(long lastAppliedSeq, long lastSnapshotSeq) {
    }

    public record MessageRow(String messageId, long seq, String senderId, String content, String status,
                             Instant createdAt, Instant updatedAt) {
    }
}
