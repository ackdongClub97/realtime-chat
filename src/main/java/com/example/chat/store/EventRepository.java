package com.example.chat.store;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import com.example.chat.common.Json;
import com.example.chat.domain.EventType;
import com.example.chat.domain.StoredEvent;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

@Repository
public class EventRepository {

    private static final String COLUMNS = "session_id, seq, client_event_id, type, user_id, payload::text AS payload, "
            + "client_seq, client_ts, server_received_at";

    private final JdbcTemplate jdbc;
    private final Json json;
    private final RowMapper<StoredEvent> mapper;

    public EventRepository(JdbcTemplate jdbc, Json json) {
        this.jdbc = jdbc;
        this.json = json;
        this.mapper = this::map;
    }

    /**
     * @return 삽입된 행 수. (session_id, client_event_id) 충돌 시 0 — 최종 방어선 역할의 멱등 삽입.
     */
    public int insert(StoredEvent e) {
        return jdbc.update("""
                INSERT INTO event (session_id, seq, client_event_id, type, user_id, payload,
                                   client_seq, client_ts, server_received_at)
                VALUES (?, ?, ?, ?, ?, ?::jsonb, ?, ?, ?)
                ON CONFLICT (session_id, client_event_id) DO NOTHING
                """,
                e.sessionId(), e.seq(), e.clientEventId(), e.type().name(), e.userId(), json.write(e.payload()),
                e.clientSeq(), e.clientTs() == null ? null : Timestamp.from(e.clientTs()),
                Timestamp.from(e.serverReceivedAt()));
    }

    /**
     * 이벤트 저장 + outbox 등록을 한 문장(CTE)으로 처리한다. 같은 트랜잭션이므로 원자적이다.
     *
     * @return 1: 저장됨 / 0: (session_id, client_event_id) 충돌
     */
    public int insertWithOutbox(StoredEvent e) {
        return jdbc.update("""
                WITH inserted AS (
                    INSERT INTO event (session_id, seq, client_event_id, type, user_id, payload,
                                       client_seq, client_ts, server_received_at)
                    VALUES (?, ?, ?, ?, ?, ?::jsonb, ?, ?, ?)
                    ON CONFLICT (session_id, client_event_id) DO NOTHING
                    RETURNING session_id, seq)
                INSERT INTO outbox (session_id, event_seq) SELECT session_id, seq FROM inserted
                """,
                e.sessionId(), e.seq(), e.clientEventId(), e.type().name(), e.userId(), json.write(e.payload()),
                e.clientSeq(), e.clientTs() == null ? null : Timestamp.from(e.clientTs()),
                Timestamp.from(e.serverReceivedAt()));
    }

    public Optional<StoredEvent> findByClientEventId(UUID sessionId, String clientEventId) {
        return jdbc.query("SELECT " + COLUMNS + " FROM event WHERE session_id = ? AND client_event_id = ?",
                mapper, sessionId, clientEventId).stream().findFirst();
    }

    /** (afterSeq, toSeq] 범위를 seq 오름차순으로. PK (session_id, seq) 인덱스 범위 스캔. */
    public List<StoredEvent> findRange(UUID sessionId, long afterSeq, long toSeqInclusive, int limit) {
        return jdbc.query("SELECT " + COLUMNS + """
                 FROM event
                WHERE session_id = ? AND seq > ? AND seq <= ?
                ORDER BY seq
                LIMIT ?
                """, mapper, sessionId, afterSeq, toSeqInclusive, limit);
    }

    public List<StoredEvent> findByTimeRange(UUID sessionId, Instant from, Instant to, int limit) {
        return jdbc.query("SELECT " + COLUMNS + """
                 FROM event
                WHERE session_id = ? AND server_received_at >= ? AND server_received_at < ?
                ORDER BY seq
                LIMIT ?
                """, mapper, sessionId, Timestamp.from(from), Timestamp.from(to), limit);
    }

    /**
     * 시각 t 를 seq 로 변환: t 이전(포함)에 수신된 마지막 이벤트의 seq.
     * idx_event_session_received (session_id, server_received_at, seq) 의 역방향 스캔 1건.
     */
    public long maxSeqAtOrBefore(UUID sessionId, Instant at) {
        List<Long> seq = jdbc.queryForList("""
                SELECT seq FROM event
                 WHERE session_id = ? AND server_received_at <= ?
                 ORDER BY server_received_at DESC, seq DESC
                 LIMIT 1
                """, Long.class, sessionId, Timestamp.from(at));
        return seq.isEmpty() ? 0L : seq.getFirst();
    }

    private StoredEvent map(ResultSet rs, int i) throws SQLException {
        long clientSeq = rs.getLong("client_seq");
        boolean clientSeqNull = rs.wasNull();
        Timestamp clientTs = rs.getTimestamp("client_ts");
        return new StoredEvent(
                rs.getObject("session_id", UUID.class),
                rs.getLong("seq"),
                rs.getString("client_event_id"),
                EventType.valueOf(rs.getString("type")),
                rs.getString("user_id"),
                json.readTree(rs.getString("payload")),
                clientSeqNull ? null : clientSeq,
                clientTs == null ? null : clientTs.toInstant(),
                rs.getTimestamp("server_received_at").toInstant());
    }
}
