package com.example.chat.ingest;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import com.example.chat.common.ApiException;
import com.example.chat.config.ChatMetrics;
import com.example.chat.domain.EventType;
import com.example.chat.domain.NewEvent;
import com.example.chat.domain.ParticipantState;
import com.example.chat.domain.SessionStatus;
import com.example.chat.domain.StoredEvent;
import com.example.chat.store.EventRepository;
import com.example.chat.store.ParticipantRepository;
import com.example.chat.store.ParticipantRow;
import com.example.chat.store.SessionRepository;
import com.example.chat.store.SessionRow;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * 쓰기 경로의 도메인 검증 / 멱등성 / 롤백 규칙 (DB 없이).
 * lockAndAllocate 는 이번 이벤트에 발급될 seq(=11)를 돌려주고, 직전 seq 는 10 이다.
 */
class EventAppendServiceTest {

    private static final UUID SID = UUID.fromString("00000000-0000-0000-0000-0000000000aa");
    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
    private static final long ALLOCATED = 11;

    private final SessionRepository sessions = mock(SessionRepository.class);
    private final ParticipantRepository participants = mock(ParticipantRepository.class);
    private final EventRepository events = mock(EventRepository.class);
    private final ApplicationEventPublisher publisher = mock(ApplicationEventPublisher.class);
    private final TransactionTemplate tx = mock(TransactionTemplate.class);
    private final TransactionStatus txStatus = mock(TransactionStatus.class);
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private EventAppendService service;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        when(tx.execute(any())).thenAnswer(inv ->
                ((TransactionCallback<Object>) inv.getArgument(0)).doInTransaction(txStatus));
        when(events.insertWithOutbox(any())).thenReturn(1);
        when(events.findByClientEventId(eq(SID), any())).thenReturn(Optional.empty());
        service = new EventAppendService(sessions, participants, events, publisher, tx, new ChatMetrics(registry));
        session(SessionStatus.ACTIVE);
    }

    private void session(SessionStatus status) {
        when(sessions.lockAndAllocate(SID)).thenReturn(Optional.of(
                new SessionRow(SID, "t", status, "alice", NOW, null, ALLOCATED, NOW)));
    }

    private void participant(String user, ParticipantState state) {
        when(participants.find(SID, user)).thenReturn(Optional.of(new ParticipantRow(SID, user, state, NOW, null, 1)));
    }

    private void members(ParticipantState... states) {
        List<ParticipantRow> rows = new java.util.ArrayList<>();
        for (int i = 0; i < states.length; i++) {
            rows.add(new ParticipantRow(SID, "u" + i, states[i], NOW, null, 1));
        }
        when(participants.findBySession(SID)).thenReturn(rows);
    }

    private static ObjectNode text(String t) {
        return JsonNodeFactory.instance.objectNode().put("text", t);
    }

    private static NewEvent event(String id, EventType type, String user, ObjectNode payload) {
        return new NewEvent(id, type, user, payload, null, null);
    }

    private static StoredEvent stored(String id, EventType type, String user, ObjectNode payload) {
        return new StoredEvent(SID, 5, id, type, user, payload, null, null, NOW);
    }

    private static String codeOf(Throwable t) {
        return ((ApiException) t).code();
    }

    private double counter(String name, String... tags) {
        var c = registry.find(name).tags(tags).counter();
        return c == null ? 0 : c.count();
    }

    @Nested
    class Accepted {

        @Test
        void messageIsStoredWithAllocatedSeqAndPublishedAfterCommit() {
            participant("alice", ParticipantState.JOINED);

            AppendResult r = service.append(SID, event("m1", EventType.MESSAGE, "alice", text("hi")), "ws");

            assertThat(r.outcome()).isEqualTo(AppendResult.Outcome.ACCEPTED);
            assertThat(r.seq()).isEqualTo(ALLOCATED);
            verify(events).insertWithOutbox(any());
            verify(publisher).publishEvent(any(EventPublishedEvent.class));
            verify(txStatus, never()).setRollbackOnly();
            assertThat(counter("chat.events.appended", "type", "MESSAGE")).isEqualTo(1);
        }

        @Test
        void disconnectedParticipantCanStillSendAndDisconnectInterruptsSession() {
            participant("bob", ParticipantState.JOINED);
            members(ParticipantState.JOINED, ParticipantState.DISCONNECTED);

            service.append(SID, event("d1", EventType.DISCONNECT, "bob", null), "system");

            verify(participants).upsert(eq(SID), eq("bob"), eq(ParticipantState.DISCONNECTED), any(), any(),
                    eq(ALLOCATED));
            verify(sessions).updateStatus(SID, SessionStatus.INTERRUPTED, null);
        }

        @Test
        void creatorCanEndSessionWithoutBeingParticipant() {
            AppendResult r = service.append(SID, event("end", EventType.SESSION_ENDED, "alice", null), "rest");

            assertThat(r.outcome()).isEqualTo(AppendResult.Outcome.ACCEPTED);
            verify(sessions).updateStatus(SID, SessionStatus.ENDED, NOW);
        }

        @Test
        void rejoinAfterLeaveIsAccepted() {
            participant("bob", ParticipantState.LEFT);
            members(ParticipantState.JOINED, ParticipantState.LEFT);

            AppendResult r = service.append(SID, event("j2", EventType.JOIN, "bob", null), "rest");

            assertThat(r.outcome()).isEqualTo(AppendResult.Outcome.ACCEPTED);
            verify(participants).upsert(eq(SID), eq("bob"), eq(ParticipantState.JOINED), eq(NOW), eq(null),
                    eq(ALLOCATED));
        }
    }

    @Nested
    class Idempotency {

        @Test
        void sameKeySameContentReturnsOriginalSeqAndRollsBackAllocation() {
            when(events.findByClientEventId(SID, "m1"))
                    .thenReturn(Optional.of(stored("m1", EventType.MESSAGE, "alice", text("hi"))));

            AppendResult r = service.append(SID, event("m1", EventType.MESSAGE, "alice", text("hi")), "ws");

            assertThat(r.outcome()).isEqualTo(AppendResult.Outcome.DUPLICATE);
            assertThat(r.seq()).isEqualTo(5);
            verify(txStatus).setRollbackOnly();
            verify(events, never()).insertWithOutbox(any());
            verifyNoInteractions(publisher);
            assertThat(counter("chat.events.duplicate", "channel", "ws")).isEqualTo(1);
        }

        @Test
        void sameKeyDifferentContentIsRejected() {
            when(events.findByClientEventId(SID, "m1"))
                    .thenReturn(Optional.of(stored("m1", EventType.MESSAGE, "alice", text("hi"))));

            assertThatThrownBy(() -> service.append(SID, event("m1", EventType.MESSAGE, "alice", text("changed")), "ws"))
                    .satisfies(e -> assertThat(codeOf(e)).isEqualTo("IDEMPOTENCY_KEY_REUSED"));
            assertThat(counter("chat.events.rejected", "code", "IDEMPOTENCY_KEY_REUSED")).isEqualTo(1);
        }

        @Test
        void sameKeyFromAnotherUserIsRejected() {
            when(events.findByClientEventId(SID, "m1"))
                    .thenReturn(Optional.of(stored("m1", EventType.MESSAGE, "alice", text("hi"))));

            assertThatThrownBy(() -> service.append(SID, event("m1", EventType.MESSAGE, "bob", text("hi")), "ws"))
                    .satisfies(e -> assertThat(codeOf(e)).isEqualTo("IDEMPOTENCY_KEY_REUSED"));
        }

        @Test
        void redundantJoinIsNoopAndDoesNotConsumeSeq() {
            participant("alice", ParticipantState.JOINED);

            AppendResult r = service.append(SID, event("j-again", EventType.JOIN, "alice", null), "rest");

            assertThat(r.outcome()).isEqualTo(AppendResult.Outcome.NOOP);
            assertThat(r.sessionLastSeq()).isEqualTo(ALLOCATED - 1);
            verify(txStatus).setRollbackOnly();
            verify(events, never()).insertWithOutbox(any());
        }

        @Test
        void reconnectWithoutPriorDisconnectIsNoop() {
            participant("alice", ParticipantState.JOINED);

            assertThat(service.append(SID, event("rc", EventType.RECONNECT, "alice", null), "system").outcome())
                    .isEqualTo(AppendResult.Outcome.NOOP);
        }

        @Test
        void conflictUnderLockFailsInsteadOfConsumingSeq() {
            participant("alice", ParticipantState.JOINED);
            when(events.insertWithOutbox(any())).thenReturn(0);

            assertThatThrownBy(() -> service.append(SID, event("m1", EventType.MESSAGE, "alice", text("hi")), "ws"))
                    .isInstanceOf(IllegalStateException.class);
            verifyNoInteractions(publisher);
        }
    }

    @Nested
    class Validation {

        @Test
        void missingOrTooLongClientEventIdIsRejectedBeforeTransaction() {
            assertThatThrownBy(() -> service.append(SID, event(" ", EventType.MESSAGE, "alice", text("x")), "ws"))
                    .satisfies(e -> assertThat(codeOf(e)).isEqualTo("INVALID_CLIENT_EVENT_ID"));
            assertThatThrownBy(() -> service.append(SID, event("x".repeat(65), EventType.MESSAGE, "alice", text("x")),
                    "ws")).satisfies(e -> assertThat(codeOf(e)).isEqualTo("INVALID_CLIENT_EVENT_ID"));
            verifyNoInteractions(tx);
        }

        @Test
        void unknownSession() {
            when(sessions.lockAndAllocate(SID)).thenReturn(Optional.empty());
            assertThatThrownBy(() -> service.append(SID, event("m1", EventType.MESSAGE, "alice", text("x")), "ws"))
                    .satisfies(e -> assertThat(codeOf(e)).isEqualTo("SESSION_NOT_FOUND"));
        }

        @Test
        void endedSessionRejectsEverything() {
            session(SessionStatus.ENDED);
            participant("alice", ParticipantState.JOINED);
            assertThatThrownBy(() -> service.append(SID, event("m1", EventType.MESSAGE, "alice", text("x")), "ws"))
                    .satisfies(e -> assertThat(codeOf(e)).isEqualTo("SESSION_ENDED"));
        }

        @Test
        void serverOnlyTypeCannotBeAppended() {
            assertThatThrownBy(() -> service.append(SID, event("c", EventType.SESSION_CREATED, "alice", null), "rest"))
                    .satisfies(e -> assertThat(codeOf(e)).isEqualTo("INVALID_EVENT_TYPE"));
        }

        @Test
        void thirdParticipantIsRejected() {
            members(ParticipantState.JOINED, ParticipantState.DISCONNECTED);
            assertThatThrownBy(() -> service.append(SID, event("j", EventType.JOIN, "carol", null), "rest"))
                    .satisfies(e -> assertThat(codeOf(e)).isEqualTo("SESSION_FULL"));
        }

        @Test
        void nonParticipantAndLeftParticipantCannotSend() {
            assertThatThrownBy(() -> service.append(SID, event("m", EventType.MESSAGE, "mallory", text("x")), "ws"))
                    .satisfies(e -> assertThat(codeOf(e)).isEqualTo("NOT_PARTICIPANT"));
            participant("bob", ParticipantState.LEFT);
            assertThatThrownBy(() -> service.append(SID, event("m", EventType.MESSAGE, "bob", text("x")), "ws"))
                    .satisfies(e -> assertThat(codeOf(e)).isEqualTo("NOT_PARTICIPANT"));
        }

        @Test
        void messageTextMustBePresentAndBounded() {
            participant("alice", ParticipantState.JOINED);
            assertThatThrownBy(() -> service.append(SID, event("m", EventType.MESSAGE, "alice", text("  ")), "ws"))
                    .satisfies(e -> assertThat(codeOf(e)).isEqualTo("INVALID_TEXT"));
            assertThatThrownBy(() -> service.append(SID, event("m", EventType.MESSAGE, "alice", text("a".repeat(4001))),
                    "ws")).satisfies(e -> assertThat(codeOf(e)).isEqualTo("INVALID_TEXT"));
        }

        @Test
        void editBeforeOriginalArrivesIsRetryableTargetNotFound() {
            participant("alice", ParticipantState.JOINED);
            ObjectNode edit = text("fixed").put("messageId", "m-missing");

            assertThatThrownBy(() -> service.append(SID, event("e1", EventType.MESSAGE_EDITED, "alice", edit), "ws"))
                    .satisfies(e -> {
                        assertThat(codeOf(e)).isEqualTo("TARGET_NOT_FOUND");
                        assertThat(((ApiException) e).status().value()).isEqualTo(422);
                    });
        }

        @Test
        void editTargetMustBeAMessage() {
            participant("alice", ParticipantState.JOINED);
            when(events.findByClientEventId(SID, "r1"))
                    .thenReturn(Optional.of(stored("r1", EventType.READ, "alice", JsonNodeFactory.instance.objectNode())));

            assertThatThrownBy(() -> service.append(SID, event("d1", EventType.MESSAGE_DELETED, "alice",
                    JsonNodeFactory.instance.objectNode().put("messageId", "r1")), "ws"))
                    .satisfies(e -> assertThat(codeOf(e)).isEqualTo("TARGET_NOT_FOUND"));
        }

        @Test
        void onlySenderCanModifyMessage() {
            participant("bob", ParticipantState.JOINED);
            when(events.findByClientEventId(SID, "m1"))
                    .thenReturn(Optional.of(stored("m1", EventType.MESSAGE, "alice", text("hi"))));

            assertThatThrownBy(() -> service.append(SID, event("d1", EventType.MESSAGE_DELETED, "bob",
                    JsonNodeFactory.instance.objectNode().put("messageId", "m1")), "ws"))
                    .satisfies(e -> assertThat(codeOf(e)).isEqualTo("NOT_MESSAGE_OWNER"));
        }

        @Test
        void editRequiresMessageId() {
            participant("alice", ParticipantState.JOINED);
            assertThatThrownBy(() -> service.append(SID, event("e1", EventType.MESSAGE_EDITED, "alice", text("x")),
                    "ws")).satisfies(e -> assertThat(codeOf(e)).isEqualTo("INVALID_EVENT"));
        }

        @Test
        void readPointerMustPointToExistingSeq() {
            participant("bob", ParticipantState.JOINED);
            ObjectNode tooFar = JsonNodeFactory.instance.objectNode().put("upToSeq", ALLOCATED);
            ObjectNode zero = JsonNodeFactory.instance.objectNode().put("upToSeq", 0);
            ObjectNode ok = JsonNodeFactory.instance.objectNode().put("upToSeq", ALLOCATED - 1);

            assertThatThrownBy(() -> service.append(SID, event("r1", EventType.READ, "bob", tooFar), "ws"))
                    .satisfies(e -> assertThat(codeOf(e)).isEqualTo("INVALID_READ_POINTER"));
            assertThatThrownBy(() -> service.append(SID, event("r2", EventType.READ, "bob", zero), "ws"))
                    .satisfies(e -> assertThat(codeOf(e)).isEqualTo("INVALID_READ_POINTER"));
            assertThat(service.append(SID, event("r3", EventType.READ, "bob", ok), "ws").outcome())
                    .isEqualTo(AppendResult.Outcome.ACCEPTED);
        }

        @Test
        void rejectedEventsAreNeverPublished() {
            assertThatThrownBy(() -> service.append(SID, event("m", EventType.MESSAGE, "mallory", text("x")), "ws"));
            verifyNoInteractions(publisher);
            verify(participants, never()).upsert(any(), any(), any(), any(), any(), anyLong());
        }
    }
}
