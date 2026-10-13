package com.example.chat.store;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import com.example.chat.domain.SessionStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

@Repository
public class SessionRepository {

    private static final String COLUMNS =
            "s.id, s.title, s.status, s.created_by, s.created_at, s.ended_at, s.last_seq, s.last_event_at";

    private static final RowMapper<SessionRow> MAPPER = SessionRepository::map;

    private final JdbcTemplate jdbc;

    public SessionRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * 세션 생성. Idempotency-Key 가 같으면 기존 세션이 유지되고 빈 결과를 반환한다.
     * 시각은 DB 시계(clock_timestamp)를 사용해 인스턴스 간 시계 오차의 영향을 받지 않는다.
     */
    public Optional<SessionRow> insert(UUID id, String title, String createdBy, String requestKey) {
        List<SessionRow> rows = jdbc.query("""
                INSERT INTO chat_session (id, title, status, created_by, created_at, last_seq, last_event_at, create_request_key)
                VALUES (?, ?, 'ACTIVE', ?, clock_timestamp(), 1, clock_timestamp(), ?)
                ON CONFLICT (create_request_key) DO NOTHING
                RETURNING id, title, status, created_by, created_at, ended_at, last_seq, last_event_at
                """, MAPPER, id, title, createdBy, requestKey);
        return rows.stream().findFirst();
    }

    public Optional<SessionRow> findById(UUID id) {
        return jdbc.query("SELECT " + COLUMNS + " FROM chat_session s WHERE s.id = ?", MAPPER, id)
                .stream().findFirst();
    }

    public Optional<SessionRow> findByRequestKey(String key) {
        return jdbc.query("SELECT " + COLUMNS + " FROM chat_session s WHERE s.create_request_key = ?", MAPPER, key)
                .stream().findFirst();
    }

    /**
     * 세션 행 잠금 + 다음 seq/수신 시각 발급을 한 번의 왕복으로 처리한다 (append 핫패스).
     * 반환된 행의 lastSeq 가 이번에 발급된 seq 이다. 이벤트를 저장하지 않게 되면(중복/NOOP/거절)
     * 트랜잭션을 롤백하므로 seq 는 소비되지 않는다 → gap-free 유지.
     */
    public Optional<SessionRow> lockAndAllocate(UUID id) {
        return jdbc.query("""
                UPDATE chat_session
                   SET last_seq = last_seq + 1,
                       last_event_at = GREATEST(clock_timestamp(), last_event_at)
                 WHERE id = ?
                RETURNING id, title, status, created_by, created_at, ended_at, last_seq, last_event_at
                """, MAPPER, id).stream().findFirst();
    }

    public void updateStatus(UUID id, SessionStatus status, Instant endedAt) {
        jdbc.update("UPDATE chat_session SET status = ?, ended_at = COALESCE(?, ended_at) WHERE id = ?",
                status.name(), endedAt == null ? null : Timestamp.from(endedAt), id);
    }

    /**
     * 세션 목록 (keyset pagination). 정렬 키 (created_at DESC, id DESC) 와 인덱스 순서를 일치시켜
     * OFFSET 없이 다음 페이지를 인덱스 범위 탐색으로 가져온다.
     */
    public List<SessionRow> search(SessionSearch q) {
        StringBuilder sql = new StringBuilder("SELECT ").append(COLUMNS).append(" FROM chat_session s");
        List<Object> args = new ArrayList<>();
        if (q.participant() != null) {
            sql.append(" JOIN participant p ON p.session_id = s.id AND p.user_id = ?");
            args.add(q.participant());
        }
        sql.append(" WHERE 1 = 1");
        if (q.status() != null) {
            sql.append(" AND s.status = ?");
            args.add(q.status().name());
        }
        if (q.from() != null) {
            sql.append(" AND s.created_at >= ?");
            args.add(Timestamp.from(q.from()));
        }
        if (q.to() != null) {
            sql.append(" AND s.created_at < ?");
            args.add(Timestamp.from(q.to()));
        }
        if (q.cursorCreatedAt() != null) {
            sql.append(" AND (s.created_at, s.id) < (?, ?)");
            args.add(Timestamp.from(q.cursorCreatedAt()));
            args.add(q.cursorId());
        }
        sql.append(" ORDER BY s.created_at DESC, s.id DESC LIMIT ?");
        args.add(q.limit());
        return jdbc.query(sql.toString(), MAPPER, args.toArray());
    }

    private static SessionRow map(ResultSet rs, int i) throws SQLException {
        Timestamp ended = rs.getTimestamp("ended_at");
        return new SessionRow(
                rs.getObject("id", UUID.class),
                rs.getString("title"),
                SessionStatus.valueOf(rs.getString("status")),
                rs.getString("created_by"),
                rs.getTimestamp("created_at").toInstant(),
                ended == null ? null : ended.toInstant(),
                rs.getLong("last_seq"),
                rs.getTimestamp("last_event_at").toInstant());
    }

    public record SessionSearch(SessionStatus status, String participant, Instant from, Instant to,
                                Instant cursorCreatedAt, UUID cursorId, int limit) {
    }
}
