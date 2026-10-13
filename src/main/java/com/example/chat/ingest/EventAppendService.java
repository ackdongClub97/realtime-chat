package com.example.chat.ingest;

import java.util.List;
import java.util.Objects;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * 이벤트 원장 쓰기 경로 (모든 이벤트가 여기를 통과한다: REST, WebSocket, 서버 생성 이벤트).
 *
 * <pre>
 * BEGIN
 *   1. UPDATE chat_session ... RETURNING   — 세션 행 잠금 + seq/수신시각 발급 (세션 단위 직렬화)   [왕복 1]
 *   2. clientEventId 중복 확인                — 있으면 롤백 후 기존 결과 반환 (멱등 ack)            [왕복 2]
 *   3. 참여자 조회 + 도메인 검증              — 위반이면 거절, 전이 없으면 NOOP (둘 다 롤백)        [왕복 3]
 *   4. event INSERT + outbox INSERT (CTE)   — UNIQUE(session_id, client_event_id) 가 최종 방어선 [왕복 4]
 *   5. participant / session 상태 갱신        — JOIN/LEAVE/DISCONNECT/RECONNECT/END 일 때만
 * COMMIT
 *   6. (커밋 후) 실시간 전송 이벤트 발행
 * </pre>
 * 저장하지 않는 경우(중복/NOOP/거절)는 롤백하므로 1번에서 올린 seq 가 되돌아가 gap 이 생기지 않는다.
 */
@Service
public class EventAppendService {

    private static final Logger log = LoggerFactory.getLogger(EventAppendService.class);
    private static final int MAX_TEXT_LENGTH = 4000;
    private static final int MAX_PARTICIPANTS = 2;

    private final SessionRepository sessions;
    private final ParticipantRepository participants;
    private final EventRepository events;
    private final ApplicationEventPublisher publisher;
    private final TransactionTemplate tx;
    private final ChatMetrics metrics;

    public EventAppendService(SessionRepository sessions, ParticipantRepository participants, EventRepository events,
                              ApplicationEventPublisher publisher, TransactionTemplate tx,
                              ChatMetrics metrics) {
        this.sessions = sessions;
        this.participants = participants;
        this.events = events;
        this.publisher = publisher;
        this.tx = tx;
        this.metrics = metrics;
    }

    public AppendResult append(UUID sessionId, NewEvent cmd, String channel) {
        validateShape(cmd);
        try {
            AppendResult result = metrics.appendLatency().record(() -> tx.execute(status -> doAppend(sessionId, cmd, status)));
            Objects.requireNonNull(result);
            switch (result.outcome()) {
                case ACCEPTED -> {
                    metrics.eventAppended(cmd.type().name());
                    publisher.publishEvent(new EventPublishedEvent(result.event()));
                }
                case DUPLICATE -> {
                    metrics.eventDuplicate(channel);
                    log.info("duplicate event ignored session={} clientEventId={} seq={} channel={}",
                            sessionId, cmd.clientEventId(), result.seq(), channel);
                }
                case NOOP -> log.debug("noop event session={} type={} user={}", sessionId, cmd.type(), cmd.userId());
            }
            return result;
        } catch (ApiException e) {
            metrics.eventRejected(e.code());
            throw e;
        }
    }

    private AppendResult doAppend(UUID sessionId, NewEvent cmd, TransactionStatus status) {
        SessionRow session = sessions.lockAndAllocate(sessionId)
                .orElseThrow(() -> ApiException.notFound("SESSION_NOT_FOUND", "session not found: " + sessionId));
        long seq = session.lastSeq();
        long previousSeq = seq - 1;

        // 세션 락 하에서 확인하므로 동시에 도착한 같은 이벤트도 정확히 하나만 통과한다
        Optional<StoredEvent> dup = events.findByClientEventId(sessionId, cmd.clientEventId());
        if (dup.isPresent()) {
            status.setRollbackOnly();
            return duplicateOf(dup.get(), cmd);
        }

        ParticipantRow participant = participants.find(sessionId, cmd.userId()).orElse(null);
        if (!validate(session, previousSeq, participant, cmd)) {
            status.setRollbackOnly();
            return AppendResult.noop(previousSeq);
        }

        StoredEvent event = new StoredEvent(sessionId, seq, cmd.clientEventId(), cmd.type(), cmd.userId(),
                cmd.payload(), cmd.clientSeq(), cmd.clientTs(), session.lastEventAt());
        if (events.insertWithOutbox(event) == 0) {
            // 세션 락 하에서는 발생할 수 없다. 발생하면 롤백해 seq 를 소비하지 않는다.
            throw new IllegalStateException("unexpected client_event_id conflict under session lock");
        }
        applyWriteModel(session, participant, event);
        return AppendResult.accepted(event);
    }

    /** 같은 키로 다른 내용을 보내면 클라이언트 버그이므로 조용히 삼키지 않고 409 로 알린다. */
    private AppendResult duplicateOf(StoredEvent existing, NewEvent cmd) {
        if (existing.type() != cmd.type() || !existing.userId().equals(cmd.userId())
                || !existing.payload().equals(cmd.payload())) {
            throw ApiException.conflict("IDEMPOTENCY_KEY_REUSED",
                    "clientEventId already used for a different event: " + cmd.clientEventId());
        }
        return AppendResult.duplicate(existing);
    }

    private void validateShape(NewEvent cmd) {
        if (cmd.clientEventId() == null || cmd.clientEventId().isBlank() || cmd.clientEventId().length() > 64) {
            throw ApiException.badRequest("INVALID_CLIENT_EVENT_ID", "clientEventId is required (max 64 chars)");
        }
        if (cmd.type() == null || cmd.userId() == null || cmd.userId().isBlank()) {
            throw ApiException.badRequest("INVALID_EVENT", "type and userId are required");
        }
    }

    /**
     * @return true: 저장해야 하는 이벤트 / false: 상태 전이가 없는 NOOP
     */
    private boolean validate(SessionRow session, long previousSeq, ParticipantRow p, NewEvent cmd) {
        if (session.status() == SessionStatus.ENDED) {
            throw ApiException.conflict("SESSION_ENDED", "session already ended");
        }
        ParticipantState current = p == null ? null : p.state();
        switch (cmd.type()) {
            case SESSION_CREATED -> throw ApiException.badRequest("INVALID_EVENT_TYPE", "server-only event type");
            case JOIN -> {
                if (current != null && current.isMember()) {
                    return false;
                }
                long members = participants.findBySession(session.id()).stream()
                        .filter(row -> row.state().isMember()).count();
                if (members >= MAX_PARTICIPANTS) {
                    throw ApiException.conflict("SESSION_FULL", "1:1 session already has 2 participants");
                }
                return true;
            }
            case LEAVE, DISCONNECT, RECONNECT -> {
                return ParticipantState.transition(current, cmd.type()).isPresent();
            }
            case MESSAGE -> {
                requireMember(current);
                requireText(cmd);
                return true;
            }
            case MESSAGE_EDITED, MESSAGE_DELETED -> {
                requireMember(current);
                if (cmd.type() == EventType.MESSAGE_EDITED) {
                    requireText(cmd);
                }
                validateTarget(session.id(), cmd);
                return true;
            }
            case READ -> {
                requireMember(current);
                long upTo = cmd.payload().path("upToSeq").asLong(0);
                if (upTo < 1 || upTo > previousSeq) {
                    throw ApiException.badRequest("INVALID_READ_POINTER",
                            "upToSeq must be within 1.." + previousSeq);
                }
                return true;
            }
            case SESSION_ENDED -> {
                if (!session.createdBy().equals(cmd.userId())) {
                    requireMember(current);
                }
                return true;
            }
        }
        throw new IllegalArgumentException("unknown type " + cmd.type());
    }

    /**
     * 순서 뒤바뀜 처리 규칙: 원본 MESSAGE 보다 수정/삭제가 먼저 도착하면 422 TARGET_NOT_FOUND 로 거절한다.
     * 클라이언트는 원본의 ack 를 받은 뒤 같은 clientEventId 로 재전송한다. 따라서 원장에는 항상 "원본 → 수정" 순서만 저장된다.
     */
    private void validateTarget(UUID sessionId, NewEvent cmd) {
        String messageId = cmd.payload().path("messageId").asText(null);
        if (messageId == null) {
            throw ApiException.badRequest("INVALID_EVENT", "payload.messageId is required");
        }
        StoredEvent target = events.findByClientEventId(sessionId, messageId)
                .filter(e -> e.type() == EventType.MESSAGE)
                .orElseThrow(() -> ApiException.unprocessable("TARGET_NOT_FOUND",
                        "target message not found (yet): " + messageId));
        if (!target.userId().equals(cmd.userId())) {
            throw ApiException.forbidden("NOT_MESSAGE_OWNER", "only the sender can modify the message");
        }
    }

    private static void requireMember(ParticipantState state) {
        if (state == null || !state.isMember()) {
            throw ApiException.forbidden("NOT_PARTICIPANT", "user is not a participant of this session");
        }
    }

    private static void requireText(NewEvent cmd) {
        String text = cmd.payload().path("text").asText("");
        if (text.isBlank() || text.length() > MAX_TEXT_LENGTH) {
            throw ApiException.badRequest("INVALID_TEXT", "payload.text is required (max " + MAX_TEXT_LENGTH + ")");
        }
    }

    /** 검증에 필요한 상태(participant, session.status)는 같은 트랜잭션에서 동기 갱신한다 (강한 일관성). */
    private void applyWriteModel(SessionRow session, ParticipantRow p, StoredEvent e) {
        if (e.type().affectsParticipant()) {
            ParticipantState next = ParticipantState.transition(p == null ? null : p.state(), e.type()).orElseThrow();
            var joinedAt = (e.type() == EventType.JOIN || p == null) ? e.serverReceivedAt() : p.joinedAt();
            var leftAt = e.type() == EventType.LEAVE ? e.serverReceivedAt() : null;
            participants.upsert(session.id(), e.userId(), next, joinedAt, leftAt, e.seq());
        }
        if (e.type() == EventType.SESSION_ENDED) {
            sessions.updateStatus(session.id(), SessionStatus.ENDED, e.serverReceivedAt());
        } else if (e.type().affectsParticipant()) {
            List<ParticipantState> states = participants.findBySession(session.id()).stream()
                    .map(ParticipantRow::state).toList();
            SessionStatus status = SessionStatus.derive(false, states);
            if (status != session.status()) {
                sessions.updateStatus(session.id(), status, null);
            }
        }
    }
}
