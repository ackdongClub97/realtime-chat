package com.example.chat.api;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import com.example.chat.api.dto.Dtos.ParticipantResponse;
import com.example.chat.api.dto.Dtos.SessionPage;
import com.example.chat.api.dto.Dtos.SessionResponse;
import com.example.chat.common.ApiException;
import com.example.chat.common.Json;
import com.example.chat.domain.EventType;
import com.example.chat.domain.SessionStatus;
import com.example.chat.domain.StoredEvent;
import com.example.chat.store.EventRepository;
import com.example.chat.store.OutboxRepository;
import com.example.chat.store.ParticipantRepository;
import com.example.chat.store.SessionRepository;
import com.example.chat.store.SessionRepository.SessionSearch;
import com.example.chat.store.SessionRow;
import com.example.chat.realtime.PresenceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

@Service
public class SessionService {

    private static final Logger log = LoggerFactory.getLogger(SessionService.class);

    private final SessionRepository sessions;
    private final ParticipantRepository participants;
    private final EventRepository events;
    private final OutboxRepository outbox;
    private final PresenceService presence;
    private final TransactionTemplate tx;
    private final Json json;

    public SessionService(SessionRepository sessions, ParticipantRepository participants, EventRepository events,
                          OutboxRepository outbox, PresenceService presence, TransactionTemplate tx, Json json) {
        this.sessions = sessions;
        this.participants = participants;
        this.events = events;
        this.outbox = outbox;
        this.presence = presence;
        this.tx = tx;
        this.json = json;
    }

    /**
     * 세션 생성 + SESSION_CREATED(seq=1) 이벤트를 한 트랜잭션으로 기록한다.
     * Idempotency-Key 가 같은 재요청은 기존 세션을 그대로 반환한다.
     */
    public Created create(String userId, String title, String idempotencyKey) {
        if (idempotencyKey != null) {
            Optional<SessionRow> existing = sessions.findByRequestKey(idempotencyKey);
            if (existing.isPresent()) {
                return new Created(existing.get(), false);
            }
        }
        return tx.execute(s -> {
            UUID id = UUID.randomUUID();
            Optional<SessionRow> inserted = sessions.insert(id, title, userId, idempotencyKey);
            if (inserted.isEmpty()) {
                return new Created(sessions.findByRequestKey(idempotencyKey).orElseThrow(), false);
            }
            SessionRow row = inserted.get();
            events.insert(new StoredEvent(id, 1, "sys:create:" + id, EventType.SESSION_CREATED, userId,
                    json.object().put("title", title), null, null, row.createdAt()));
            outbox.enqueue(id, 1);
            return new Created(row, true);
        });
    }

    public SessionRow require(UUID id) {
        return sessions.findById(id)
                .orElseThrow(() -> ApiException.notFound("SESSION_NOT_FOUND", "session not found: " + id));
    }

    public SessionResponse detail(UUID id) {
        SessionRow row = require(id);
        return toResponse(row, participantsOf(id));
    }

    public SessionResponse summary(SessionRow row) {
        return toResponse(row, null);
    }

    public SessionPage search(SessionStatus status, String participant, Instant from, Instant to, String cursor,
                              int limit) {
        int size = Math.clamp(limit, 1, 100);
        Instant cursorAt = null;
        UUID cursorId = null;
        if (cursor != null && !cursor.isBlank()) {
            try {
                String[] parts = new String(Base64.getUrlDecoder().decode(cursor), StandardCharsets.UTF_8).split("\\|");
                cursorAt = Instant.parse(parts[0]);
                cursorId = UUID.fromString(parts[1]);
            } catch (RuntimeException e) {
                throw ApiException.badRequest("INVALID_CURSOR", "malformed cursor");
            }
        }
        List<SessionRow> rows = sessions.search(new SessionSearch(status, participant, from, to, cursorAt, cursorId,
                size + 1));
        boolean hasMore = rows.size() > size;
        List<SessionRow> page = hasMore ? rows.subList(0, size) : rows;
        String next = null;
        if (hasMore) {
            SessionRow last = page.getLast();
            next = Base64.getUrlEncoder().withoutPadding().encodeToString(
                    (last.createdAt() + "|" + last.id()).getBytes(StandardCharsets.UTF_8));
        }
        return new SessionPage(page.stream().map(this::summary).toList(), next);
    }

    private List<ParticipantResponse> participantsOf(UUID sessionId) {
        List<ParticipantResponse> list = new ArrayList<>();
        for (var p : participants.findBySession(sessionId)) {
            String status;
            try {
                status = presence.isOnline(sessionId, p.userId()) ? "ONLINE" : "OFFLINE";
            } catch (Exception e) {
                log.debug("presence unavailable: {}", e.getMessage());
                status = "UNKNOWN";
            }
            list.add(new ParticipantResponse(p.userId(), p.state(), p.joinedAt(), p.leftAt(), status));
        }
        return list;
    }

    private SessionResponse toResponse(SessionRow row, List<ParticipantResponse> participants) {
        return new SessionResponse(row.id(), row.title(), row.status(), row.createdBy(), row.createdAt(),
                row.endedAt(), row.lastSeq(), participants);
    }

    public record Created(SessionRow session, boolean created) {
    }
}
