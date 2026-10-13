package com.example.chat.realtime;

import java.io.IOException;
import java.util.UUID;

import com.example.chat.common.ApiException;
import com.example.chat.common.Json;
import com.example.chat.config.ChatMetrics;
import com.example.chat.domain.EventType;
import com.example.chat.domain.NewEvent;
import com.example.chat.store.ParticipantRow;
import com.example.chat.domain.SessionStatus;
import com.example.chat.ingest.AppendResult;
import com.example.chat.ingest.EventAppendService;
import com.example.chat.store.ParticipantRepository;
import com.example.chat.store.SessionRepository;
import com.example.chat.store.SessionRow;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Component;
import org.springframework.transaction.TransactionException;
import org.springframework.util.MultiValueMap;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.ConcurrentWebSocketSessionDecorator;
import org.springframework.web.socket.handler.TextWebSocketHandler;
import org.springframework.web.util.UriComponentsBuilder;

/**
 * {@code ws://host/ws?sessionId=...&userId=...&lastSeq=...}
 * <p>
 * 연결 수립 순서 (resume 시 누락/중복 없이 live 로 전환):
 * <ol>
 *   <li>참여자 검증 → 초기 기준점 lastSentSeq = 클라이언트 lastSeq (없으면 현재 seq = live only)</li>
 *   <li>welcome 전송 → 레지스트리 등록 (이후 live 이벤트 수신 시작) → presence online</li>
 *   <li>DISCONNECTED 상태였다면 RECONNECT 기록</li>
 *   <li>원장에서 lastSentSeq 이후 이벤트 재전송 (live 이벤트와 겹치면 EventDelivery 가 seq 로 중복 제거/gap-fill)</li>
 * </ol>
 */
@Component
public class ChatWebSocketHandler extends TextWebSocketHandler {

    private static final Logger log = LoggerFactory.getLogger(ChatWebSocketHandler.class);
    private static final String CONN_ATTR = "conn";
    private static final int SEND_TIME_LIMIT_MS = 10_000;
    private static final int SEND_BUFFER_LIMIT_BYTES = 512 * 1024;

    private final SessionRepository sessions;
    private final ParticipantRepository participants;
    private final EventAppendService appender;
    private final ConnectionRegistry registry;
    private final EventDelivery delivery;
    private final PresenceService presence;
    private final RealtimeService realtime;
    private final ChatMetrics metrics;
    private final Tracer tracer;
    private final Json json;

    public ChatWebSocketHandler(SessionRepository sessions, ParticipantRepository participants,
                                EventAppendService appender, ConnectionRegistry registry, EventDelivery delivery,
                                PresenceService presence, RealtimeService realtime, ChatMetrics metrics, Tracer tracer,
                                Json json) {
        this.sessions = sessions;
        this.participants = participants;
        this.appender = appender;
        this.registry = registry;
        this.delivery = delivery;
        this.presence = presence;
        this.realtime = realtime;
        this.metrics = metrics;
        this.tracer = tracer;
        this.json = json;
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession raw) throws IOException {
        MultiValueMap<String, String> q = UriComponentsBuilder.fromUri(raw.getUri()).build().getQueryParams();
        UUID sessionId;
        String userId = q.getFirst("userId");
        try {
            sessionId = UUID.fromString(q.getFirst("sessionId"));
        } catch (RuntimeException e) {
            raw.close(new CloseStatus(4400, "sessionId required"));
            return;
        }
        if (userId == null || userId.isBlank()) {
            raw.close(new CloseStatus(4400, "userId required"));
            return;
        }
        SessionRow session = sessions.findById(sessionId).orElse(null);
        if (session == null) {
            raw.close(new CloseStatus(4404, "session not found"));
            return;
        }
        if (session.status() == SessionStatus.ENDED) {
            raw.close(new CloseStatus(4409, "session ended"));
            return;
        }
        ParticipantRow p = participants.find(sessionId, userId).orElse(null);
        if (p == null || !p.state().isMember()) {
            raw.close(new CloseStatus(4403, "join the session first (POST /sessions/{id}/join)"));
            return;
        }

        String lastSeqParam = q.getFirst("lastSeq");
        long initial = lastSeqParam == null
                ? session.lastSeq()
                : Math.max(0, Math.min(Long.parseLong(lastSeqParam), session.lastSeq()));

        WebSocketSession ws = new ConcurrentWebSocketSessionDecorator(raw, SEND_TIME_LIMIT_MS, SEND_BUFFER_LIMIT_BYTES);
        ClientConnection c = new ClientConnection(sessionId, userId, ws, initial);
        raw.getAttributes().put(CONN_ATTR, c);
        withContext(c, "connect", () -> {
            c.sendRaw(json.write(new Frames.Welcome(c.connId(), sessionId, userId, session.lastSeq(), initial)));

            ClientConnection previous = registry.register(c);
            if (previous != null) {
                previous.markReplaced();
                previous.close(RealtimeService.REPLACED);
            }
            metrics.wsConnected();
            try {
                presence.markOnline(c);
            } catch (Exception e) {
                log.warn("presence unavailable: {}", e.getMessage());
            }
            realtime.kickOthers(c);
            realtime.publishPresence(sessionId, userId, "ONLINE");

            appender.append(sessionId, NewEvent.system("sys:reconnect:" + c.connId(), EventType.RECONNECT, userId,
                    json.object().put("connId", c.connId())), "system");

            long latest = sessions.findById(sessionId).map(SessionRow::lastSeq).orElse(initial);
            long replayed = delivery.catchUp(c, latest);
            if (replayed > 0) {
                metrics.resumeReplayed(replayed);
            }
            log.info("ws connected conn={} lastSeq={} replayed={}", c.connId(), initial, replayed);
        });
    }

    @Override
    protected void handleTextMessage(WebSocketSession raw, TextMessage message) {
        ClientConnection c = (ClientConnection) raw.getAttributes().get(CONN_ATTR);
        if (c == null) {
            return;
        }
        Frames.ClientFrame frame;
        try {
            frame = json.read(message.getPayload(), Frames.ClientFrame.class);
        } catch (IllegalArgumentException e) {
            sendQuietly(c, new Frames.Error(null, "INVALID_FRAME", "malformed json", false));
            return;
        }
        String type = frame.type() == null ? "" : frame.type();
        withContext(c, "ws." + type, () -> {
            switch (type) {
                case "send" -> handleSend(c, frame);
                case "resume" -> {
                    long latest = sessions.findById(c.sessionId()).map(SessionRow::lastSeq).orElse(0L);
                    long n = delivery.resumeFrom(c, frame.lastSeq() == null ? 0 : frame.lastSeq(), latest);
                    metrics.resumeReplayed(n);
                }
                case "leave" -> {
                    c.markDeparted();
                    appender.append(c.sessionId(), new NewEvent(frame.clientEventId() != null
                            ? frame.clientEventId() : "sys:leave:" + c.connId(), EventType.LEAVE, c.userId(), null,
                            frame.clientSeq(), frame.clientTs()), "ws");
                    c.close(CloseStatus.NORMAL);
                }
                case "ping" -> {
                    try {
                        presence.refresh(c);
                    } catch (Exception e) {
                        log.debug("presence refresh failed: {}", e.getMessage());
                    }
                    sendQuietly(c, new Frames.Pong(c.lastSentSeqSnapshot()));
                }
                default -> sendQuietly(c, new Frames.Error(frame.clientEventId(), "UNKNOWN_FRAME",
                        "unknown frame type: " + type, false));
            }
        });
    }

    private void handleSend(ClientConnection c, Frames.ClientFrame frame) {
        if ("TYPING".equals(frame.eventType())) {
            realtime.publishTyping(c.sessionId(), c.userId(), frame.payload() == null
                    || frame.payload().path("typing").asBoolean(true));
            return;
        }
        EventType type;
        try {
            type = EventType.valueOf(frame.eventType());
        } catch (RuntimeException e) {
            sendQuietly(c, new Frames.Error(frame.clientEventId(), "INVALID_EVENT_TYPE",
                    "unknown eventType: " + frame.eventType(), false));
            return;
        }
        if (!EventType.CLIENT_SENDABLE.contains(type)) {
            sendQuietly(c, new Frames.Error(frame.clientEventId(), "INVALID_EVENT_TYPE",
                    "eventType not allowed over send: " + type, false));
            return;
        }
        if (c.trackClientSeq(frame.clientSeq())) {
            metrics.clientSeqGap();
            log.info("client seq gap detected conn={} clientSeq={}", c.connId(), frame.clientSeq());
        }
        try {
            AppendResult r = appender.append(c.sessionId(), new NewEvent(frame.clientEventId(), type, c.userId(),
                    frame.payload(), frame.clientSeq(), frame.clientTs()), "ws");
            sendQuietly(c, new Frames.Ack(frame.clientEventId(), r.seq(), r.outcome().name()));
        } catch (ApiException e) {
            boolean retryable = "TARGET_NOT_FOUND".equals(e.code());
            sendQuietly(c, new Frames.Error(frame.clientEventId(), e.code(), e.getMessage(), retryable));
        } catch (DataAccessException | TransactionException e) {
            log.warn("append failed (db): {}", e.getMessage());
            sendQuietly(c, new Frames.Error(frame.clientEventId(), "DB_UNAVAILABLE",
                    "temporarily unavailable, retry with the same clientEventId", true));
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession raw, CloseStatus status) {
        ClientConnection c = (ClientConnection) raw.getAttributes().get(CONN_ATTR);
        if (c == null) {
            return;
        }
        registry.unregister(c);
        metrics.wsDisconnected();
        withContext(c, "disconnect", () -> {
            boolean owner;
            try {
                owner = presence.release(c);
            } catch (Exception e) {
                // Redis 장애: 로컬에 같은 사용자의 새 연결이 없으면 소유자로 간주
                owner = registry.find(c.sessionId(), c.userId()) == null;
            }
            if (c.replaced() || c.departed() || !owner) {
                log.info("ws closed conn={} code={} (no disconnect event: replaced={} departed={} owner={})",
                        c.connId(), status.getCode(), c.replaced(), c.departed(), owner);
                if (c.departed() && owner) {
                    realtime.publishPresence(c.sessionId(), c.userId(), "OFFLINE");
                }
                return;
            }
            try {
                appender.append(c.sessionId(), NewEvent.system("sys:disconnect:" + c.connId(), EventType.DISCONNECT,
                        c.userId(), json.object().put("reason", "ws_closed").put("closeCode", status.getCode())
                                .put("connId", c.connId())), "system");
            } catch (ApiException e) {
                log.debug("disconnect event skipped: {}", e.code());
            }
            realtime.publishPresence(c.sessionId(), c.userId(), "OFFLINE");
            log.info("ws closed conn={} code={}", c.connId(), status.getCode());
        });
    }

    @Override
    public void handleTransportError(WebSocketSession session, Throwable exception) {
        log.debug("transport error: {}", exception.getMessage());
    }

    /** WebSocket 프레임마다 span 을 열어 traceId 를 MDC 에 넣는다 (HTTP 는 자동, WS 는 수동). */
    private void withContext(ClientConnection c, String name, ThrowingRunnable action) {
        Span span = tracer.nextSpan().name(name).start();
        try (Tracer.SpanInScope ignored = tracer.withSpan(span)) {
            MDC.put("sessionId", c.sessionId().toString());
            MDC.put("userId", c.userId());
            MDC.put("connId", c.connId());
            action.run();
        } catch (ApiException e) {
            sendQuietly(c, new Frames.Error(null, e.code(), e.getMessage(), false));
        } catch (Exception e) {
            log.error("ws handling failed: {}", e.getMessage(), e);
            sendQuietly(c, new Frames.Error(null, "INTERNAL_ERROR", "internal error", true));
        } finally {
            MDC.remove("sessionId");
            MDC.remove("userId");
            MDC.remove("connId");
            span.end();
        }
    }

    private void sendQuietly(ClientConnection c, Object frame) {
        try {
            c.sendRaw(json.write(frame));
        } catch (Exception e) {
            log.debug("send failed conn={}: {}", c.connId(), e.getMessage());
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }
}
