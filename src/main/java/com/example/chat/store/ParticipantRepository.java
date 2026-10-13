package com.example.chat.store;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import com.example.chat.domain.ParticipantState;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class ParticipantRepository {

    private final JdbcTemplate jdbc;

    public ParticipantRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public Optional<ParticipantRow> find(UUID sessionId, String userId) {
        return jdbc.query("SELECT * FROM participant WHERE session_id = ? AND user_id = ?",
                ParticipantRepository::map, sessionId, userId).stream().findFirst();
    }

    public List<ParticipantRow> findBySession(UUID sessionId) {
        return jdbc.query("SELECT * FROM participant WHERE session_id = ? ORDER BY user_id",
                ParticipantRepository::map, sessionId);
    }

    public void upsert(UUID sessionId, String userId, ParticipantState state, Instant joinedAt, Instant leftAt,
                       long seq) {
        jdbc.update("""
                INSERT INTO participant (session_id, user_id, state, joined_at, left_at, last_event_seq)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (session_id, user_id) DO UPDATE
                   SET state = EXCLUDED.state,
                       joined_at = EXCLUDED.joined_at,
                       left_at = EXCLUDED.left_at,
                       last_event_seq = EXCLUDED.last_event_seq
                """, sessionId, userId, state.name(), Timestamp.from(joinedAt),
                leftAt == null ? null : Timestamp.from(leftAt), seq);
    }

    private static ParticipantRow map(ResultSet rs, int i) throws SQLException {
        Timestamp left = rs.getTimestamp("left_at");
        return new ParticipantRow(
                rs.getObject("session_id", UUID.class),
                rs.getString("user_id"),
                ParticipantState.valueOf(rs.getString("state")),
                rs.getTimestamp("joined_at").toInstant(),
                left == null ? null : left.toInstant(),
                rs.getLong("last_event_seq"));
    }
}
