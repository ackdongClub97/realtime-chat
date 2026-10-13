package com.example.chat.it;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.LongStream;

import com.example.chat.admin.ChaosSettings;
import com.example.chat.api.SessionService;
import com.example.chat.common.ApiException;
import com.example.chat.domain.EventType;
import com.example.chat.domain.NewEvent;
import com.example.chat.ingest.AppendResult;
import com.example.chat.ingest.EventAppendService;
import com.example.chat.restore.RestoreResult;
import com.example.chat.restore.RestoreService;
import com.example.chat.restore.SessionState;
import com.example.chat.store.OutboxRepository;
import com.example.chat.store.ProjectionRepository;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EventStoreIntegrationTest extends IntegrationTestBase {

    @Autowired
    SessionService sessions;
    @Autowired
    EventAppendService appender;
    @Autowired
    RestoreService restore;
    @Autowired
    ProjectionRepository projections;
    @Autowired
    OutboxRepository outbox;
    @Autowired
    ChaosSettings chaos;
    @Autowired
    JdbcTemplate jdbc;

    private UUID newSessionWith(String... users) {
        UUID id = sessions.create(users[0], "it", null).session().id();
        for (String u : users) {
            appender.append(id, new NewEvent(UUID.randomUUID().toString(), EventType.JOIN, u, null, null, null), "it");
        }
        return id;
    }

    private static ObjectNode text(String t) {
        return JsonNodeFactory.instance.objectNode().put("text", t);
    }

    @Test
    void concurrentRetriesOfSameEventAreStoredOnce() throws Exception {
        UUID sid = newSessionWith("alice", "bob");
        NewEvent same = new NewEvent("retry-storm", EventType.MESSAGE, "alice", text("hello"), 1L, null);

        List<AppendResult> results = runConcurrently(32, i -> appender.append(sid, same, "it"));

        assertThat(results).filteredOn(r -> r.outcome() == AppendResult.Outcome.ACCEPTED).hasSize(1);
        assertThat(results).extracting(AppendResult::seq).containsOnly(results.getFirst().seq());
        assertThat(jdbc.queryForObject("SELECT count(*) FROM event WHERE session_id = ? AND client_event_id = ?",
                Long.class, sid, "retry-storm")).isEqualTo(1);
    }

    @Test
    void concurrentDistinctEventsGetGapFreeUniqueSeq() throws Exception {
        UUID sid = newSessionWith("alice", "bob");
        long base = sessions.require(sid).lastSeq();

        runConcurrently(200, i -> appender.append(sid, new NewEvent("m-" + i, EventType.MESSAGE,
                i % 2 == 0 ? "alice" : "bob", text("msg " + i), null, null), "it"));

        List<Long> seqs = jdbc.queryForList("SELECT seq FROM event WHERE session_id = ? AND seq > ? ORDER BY seq",
                Long.class, sid, base);
        assertThat(seqs).containsExactlyElementsOf(LongStream.rangeClosed(base + 1, base + 200).boxed().toList());
        // seq 와 수신 시각이 같은 방향으로 증가 → 시각 기준 복원과 seq 기준 복원이 일치
        Boolean monotonic = jdbc.queryForObject("""
                SELECT bool_and(server_received_at >= prev) FROM (
                  SELECT server_received_at, lag(server_received_at) OVER (ORDER BY seq) prev
                    FROM event WHERE session_id = ?) t WHERE prev IS NOT NULL
                """, Boolean.class, sid);
        assertThat(monotonic).isTrue();
    }

    @Test
    void sameKeyWithDifferentContentIsRejected() {
        UUID sid = newSessionWith("alice", "bob");
        appender.append(sid, new NewEvent("k1", EventType.MESSAGE, "alice", text("a"), null, null), "it");
        assertThatThrownBy(() -> appender.append(sid,
                new NewEvent("k1", EventType.MESSAGE, "alice", text("b"), null, null), "it"))
                .isInstanceOf(ApiException.class)
                .extracting(e -> ((ApiException) e).code()).isEqualTo("IDEMPOTENCY_KEY_REUSED");
    }

    @Test
    void editArrivingBeforeOriginalIsRejectedThenAcceptedOnRetry() {
        UUID sid = newSessionWith("alice", "bob");
        NewEvent edit = new NewEvent("e1", EventType.MESSAGE_EDITED, "alice",
                text("fixed").put("messageId", "m-late"), null, null);
        assertThatThrownBy(() -> appender.append(sid, edit, "it"))
                .extracting(e -> ((ApiException) e).code()).isEqualTo("TARGET_NOT_FOUND");

        appender.append(sid, new NewEvent("m-late", EventType.MESSAGE, "alice", text("typo"), null, null), "it");
        assertThat(appender.append(sid, edit, "it").outcome()).isEqualTo(AppendResult.Outcome.ACCEPTED);

        SessionState s = restore.restore(sid, null, null).state();
        assertThat(s.messages.get("m-late").text).isEqualTo("fixed");
    }

    @Test
    void restoresStateAtPointInTime() throws Exception {
        UUID sid = newSessionWith("alice", "bob");
        appender.append(sid, new NewEvent("m1", EventType.MESSAGE, "alice", text("first"), null, null), "it");
        Thread.sleep(20);
        Instant between = Instant.now();
        Thread.sleep(20);
        appender.append(sid, new NewEvent("m2", EventType.MESSAGE, "bob", text("second"), null, null), "it");
        appender.append(sid, new NewEvent("l1", EventType.LEAVE, "bob", null, null, null), "it");

        SessionState past = restore.restore(sid, between, null).state();
        assertThat(past.messages).containsOnlyKeys("m1");
        assertThat(past.participants.get("bob").state.name()).isEqualTo("JOINED");

        SessionState now = restore.restore(sid, null, null).state();
        assertThat(now.messages).containsOnlyKeys("m1", "m2");
        assertThat(now.participants.get("bob").state.name()).isEqualTo("LEFT");
    }

    @Test
    void asyncProjectionAndSnapshotsMatchFullReplay() {
        UUID sid = newSessionWith("alice", "bob");
        for (int i = 0; i < 35; i++) {
            appender.append(sid, new NewEvent("s-" + i, EventType.MESSAGE, "alice", text("m" + i), null, null), "it");
        }
        long last = sessions.require(sid).lastSeq();
        await(() -> projections.appliedSeq(sid, ProjectionRepository.MESSAGES) == last);

        List<Long> snapshotSeqs = jdbc.queryForList(
                "SELECT seq FROM session_snapshot WHERE session_id = ? ORDER BY seq", Long.class, sid);
        assertThat(snapshotSeqs).containsExactly(10L, 20L, 30L); // interval=10 경계마다 정확히

        for (long cut = 1; cut <= last; cut++) {
            RestoreResult fast = restore.restoreAtSeq(sid, cut);
            assertThat(fast.replayedEvents()).isLessThan(10);
            assertThat(restore.sameState(fast.state(), restore.fullReplay(sid, cut))).as("seq %d", cut).isTrue();
        }
        assertThat(projections.page(sid, Long.MAX_VALUE, 100)).hasSize(35);
    }

    @Test
    void poisonEventIsRetriedThenDeadLetteredThenRecoveredByRequeue() {
        UUID sid = newSessionWith("alice", "bob");
        chaos.poisonText("__poison__");
        try {
            appender.append(sid, new NewEvent("p1", EventType.MESSAGE, "alice", text("x __poison__"), null, null),
                    "it");
            await(() -> outbox.openDeadLetters(100).stream().anyMatch(d -> d.sessionId().equals(sid)));
            assertThat(outbox.openDeadLetters(100)).filteredOn(d -> d.sessionId().equals(sid))
                    .first().satisfies(d -> assertThat(d.attempts()).isEqualTo(3));
        } finally {
            chaos.poisonText(null);
        }
        outbox.requeueDeadLetters();
        long last = sessions.require(sid).lastSeq();
        await(() -> projections.appliedSeq(sid, ProjectionRepository.MESSAGES) == last);
    }

    @Test
    void endedSessionRejectsNewEvents() {
        UUID sid = newSessionWith("alice", "bob");
        appender.append(sid, new NewEvent("end", EventType.SESSION_ENDED, "alice", null, null, null), "it");
        assertThatThrownBy(() -> appender.append(sid,
                new NewEvent("late", EventType.MESSAGE, "bob", text("too late"), null, null), "it"))
                .extracting(e -> ((ApiException) e).code()).isEqualTo("SESSION_ENDED");
    }

    private static <T> List<T> runConcurrently(int n, ThrowingFn<T> task) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(16);
        try {
            List<Callable<T>> calls = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                int idx = i;
                calls.add(() -> task.apply(idx));
            }
            List<T> out = new ArrayList<>();
            for (Future<T> f : pool.invokeAll(calls)) {
                out.add(f.get());
            }
            return out;
        } finally {
            pool.shutdown();
        }
    }

    static void await(java.util.function.BooleanSupplier cond) {
        Instant deadline = Instant.now().plus(Duration.ofSeconds(15));
        while (!cond.getAsBoolean()) {
            if (Instant.now().isAfter(deadline)) {
                throw new AssertionError("condition not met in time");
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    @FunctionalInterface
    interface ThrowingFn<T> {
        T apply(int i) throws Exception;
    }
}
