package com.example.chat.store;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class OutboxRepository {

    private final JdbcTemplate jdbc;

    public OutboxRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public void enqueue(UUID sessionId, long eventSeq) {
        jdbc.update("INSERT INTO outbox (session_id, event_seq) VALUES (?, ?)", sessionId, eventSeq);
    }

    /**
     * 처리할 작업을 리스(lease) 방식으로 점유한다.
     * <ul>
     *   <li>FOR UPDATE SKIP LOCKED: 여러 인스턴스의 워커가 같은 행을 동시에 가져가지 않는다.</li>
     *   <li>locked_until: 워커가 처리 중 죽으면 리스 만료 후 다른 워커가 회수한다 (at-least-once).</li>
     *   <li>PENDING / 리스 만료를 별도 CTE 로 나눠 각각 부분 인덱스 순서대로 읽는다 (대기열이 커져도 정렬 없음).</li>
     * </ul>
     */
    public List<OutboxItem> claim(String workerId, int limit, int leaseSeconds) {
        return jdbc.query("""
                WITH due AS (
                    SELECT id FROM outbox
                     WHERE status = 'PENDING' AND next_attempt_at <= now()
                     ORDER BY next_attempt_at, id
                     LIMIT ?
                     FOR UPDATE SKIP LOCKED
                ), expired AS (
                    SELECT id FROM outbox
                     WHERE status = 'PROCESSING' AND locked_until < now()
                     ORDER BY locked_until
                     LIMIT ?
                     FOR UPDATE SKIP LOCKED
                )
                UPDATE outbox o
                   SET status = 'PROCESSING',
                       locked_by = ?,
                       locked_until = now() + make_interval(secs => ?)
                 WHERE o.id IN (SELECT id FROM due UNION ALL SELECT id FROM expired)
                RETURNING o.id, o.session_id, o.event_seq, o.attempts
                """, (rs, i) -> new OutboxItem(rs.getLong("id"), rs.getObject("session_id", UUID.class),
                rs.getLong("event_seq"), rs.getInt("attempts")), limit, limit, workerId, leaseSeconds);
    }

    /**
     * 세션을 seq 까지 따라잡았으면 그 이하의 outbox 행은 (점유 여부와 무관하게) 모두 처리 완료다.
     * 이벤트당 1행씩 쌓이는 outbox 를 세션 단위로 한 번에 정리한다 (coalescing).
     */
    public int deleteCovered(UUID sessionId, long appliedSeq) {
        // SKIP LOCKED: 다른 워커가 점유/삭제 중인 행은 건너뛴다 (그 워커가 직접 지운다) → 워커 간 데드락 없음
        return jdbc.update("""
                DELETE FROM outbox WHERE id IN (
                    SELECT id FROM outbox WHERE session_id = ? AND event_seq <= ? FOR UPDATE SKIP LOCKED)
                """, sessionId, appliedSeq);
    }

    public void delete(List<Long> ids) {
        if (ids.isEmpty()) {
            return;
        }
        jdbc.update("DELETE FROM outbox WHERE id = ANY(?)", (Object) ids.toArray(Long[]::new));
    }

    public void scheduleRetry(long id, int attempts, Instant nextAttemptAt, String error) {
        jdbc.update("""
                UPDATE outbox
                   SET status = 'PENDING', attempts = ?, next_attempt_at = ?, last_error = ?,
                       locked_by = NULL, locked_until = NULL
                 WHERE id = ?
                """, attempts, Timestamp.from(nextAttemptAt), error, id);
    }

    @Transactional
    public void moveToDeadLetter(OutboxItem item, int attempts, String error) {
        jdbc.update("""
                INSERT INTO dead_letter (outbox_id, session_id, event_seq, attempts, last_error)
                VALUES (?, ?, ?, ?, ?)
                """, item.id(), item.sessionId(), item.eventSeq(), attempts, error);
        jdbc.update("DELETE FROM outbox WHERE id = ?", item.id());
    }

    public OutboxStats stats() {
        return jdbc.queryForObject("""
                SELECT count(*) AS pending,
                       COALESCE(EXTRACT(EPOCH FROM now() - min(created_at)), 0) AS oldest_age_seconds,
                       (SELECT count(*) FROM dead_letter WHERE requeued_at IS NULL) AS dead_letters
                  FROM outbox
                """, (rs, i) -> new OutboxStats(rs.getLong("pending"), rs.getDouble("oldest_age_seconds"),
                rs.getLong("dead_letters")));
    }

    public List<DeadLetter> openDeadLetters(int limit) {
        return jdbc.query("""
                SELECT id, outbox_id, session_id, event_seq, attempts, last_error, failed_at
                  FROM dead_letter WHERE requeued_at IS NULL ORDER BY id LIMIT ?
                """, (rs, i) -> new DeadLetter(rs.getLong("id"), rs.getLong("outbox_id"),
                rs.getObject("session_id", UUID.class), rs.getLong("event_seq"), rs.getInt("attempts"),
                rs.getString("last_error"), rs.getTimestamp("failed_at").toInstant()), limit);
    }

    /** DLQ 재처리: 원인 수정 후 운영자가 호출. 재시도 횟수를 초기화해 outbox 로 되돌린다. */
    @Transactional
    public int requeueDeadLetters() {
        int n = jdbc.update("""
                INSERT INTO outbox (session_id, event_seq)
                SELECT session_id, event_seq FROM dead_letter WHERE requeued_at IS NULL
                """);
        jdbc.update("UPDATE dead_letter SET requeued_at = now() WHERE requeued_at IS NULL");
        return n;
    }

    public record OutboxItem(long id, UUID sessionId, long eventSeq, int attempts) {
    }

    public record OutboxStats(long pending, double oldestAgeSeconds, long deadLetters) {
    }

    public record DeadLetter(long id, long outboxId, UUID sessionId, long eventSeq, int attempts, String lastError,
                             Instant failedAt) {
    }
}
