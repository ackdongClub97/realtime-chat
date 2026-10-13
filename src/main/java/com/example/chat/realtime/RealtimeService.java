package com.example.chat.realtime;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.example.chat.common.ApiException;
import com.example.chat.common.Json;
import com.example.chat.config.InstanceId;
import com.example.chat.domain.EventType;
import com.example.chat.domain.NewEvent;
import com.example.chat.domain.StoredEvent;
import com.example.chat.ingest.EventAppendService;
import com.example.chat.ingest.EventPublishedEvent;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.CloseStatus;

/**
 * 저장된 이벤트를 로컬 연결 + 다른 인스턴스로 팬아웃하고, 버스 메시지를 로컬 연결로 전달한다.
 * <p>
 * 로컬 연결에는 Redis 를 거치지 않고 바로 전달한다 → Redis 장애 시에도 같은 인스턴스 사용자 간 통신은 유지된다.
 */
@Service
public class RealtimeService {

    public static final CloseStatus REPLACED = new CloseStatus(4001, "replaced by a newer connection");
    public static final CloseStatus SESSION_ENDED = new CloseStatus(4000, "session ended");
    private static final Logger log = LoggerFactory.getLogger(RealtimeService.class);

    private final ConnectionRegistry registry;
    private final SessionStripedExecutor fanout;
    private final PresenceService presence;
    private final EventDelivery delivery;
    private final RedisEventBus bus;
    private final EventAppendService appender;
    private final InstanceId instanceId;
    private final Tracer tracer;
    private final Json json;
    private final ExecutorService healer = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "presence-healer");
        t.setDaemon(true);
        return t;
    });

    public RealtimeService(ConnectionRegistry registry, SessionStripedExecutor fanout, PresenceService presence, EventDelivery delivery, RedisEventBus bus,
                           EventAppendService appender, InstanceId instanceId, Tracer tracer, Json json) {
        this.registry = registry;
        this.fanout = fanout;
        this.presence = presence;
        this.delivery = delivery;
        this.bus = bus;
        this.appender = appender;
        this.instanceId = instanceId;
        this.tracer = tracer;
        this.json = json;
    }

    @PostConstruct
    void subscribe() {
        bus.subscribe(this::onBusMessage);
    }

    @PreDestroy
    void shutdown() {
        healer.shutdownNow();
    }

    /**
     * 커밋된 이벤트 (EventAppendService 가 커밋 후 발행).
     * 팬아웃은 세션별 전송 레인에서 비동기로 수행한다 → 느린 수신자가 송신자의 ack/다음 프레임 처리를 막지 않는다.
     * 같은 세션은 같은 레인에서 처리되므로 대부분 seq 순서대로 전달된다 (드문 역전은 EventDelivery 가 seq 로 보정).
     */
    @EventListener
    public void onEventCommitted(EventPublishedEvent published) {
        StoredEvent e = published.event();
        String traceId = currentTraceId();
        fanout.execute(e.sessionId(), () -> {
            deliverLocal(e);
            bus.publish(new BusMessage(BusMessage.Kind.EVENT, e.sessionId(), e, e.userId(), null, null, null,
                    instanceId.value(), traceId));
        });
    }

    public void publishPresence(UUID sessionId, String userId, String status) {
        sendToSession(sessionId, userId, json.write(new Frames.Presence(userId, status)));
        bus.publish(new BusMessage(BusMessage.Kind.PRESENCE, sessionId, null, userId, null, status, null,
                instanceId.value(), currentTraceId()));
    }

    public void publishTyping(UUID sessionId, String userId, boolean typing) {
        sendToSession(sessionId, userId, json.write(new Frames.Typing(userId, typing)));
        bus.publish(new BusMessage(BusMessage.Kind.TYPING, sessionId, null, userId, null, null, typing,
                instanceId.value(), currentTraceId()));
    }

    /** 다른 인스턴스에 남아 있는 같은 사용자의 이전 연결을 닫게 한다. */
    public void kickOthers(ClientConnection keep) {
        bus.publish(new BusMessage(BusMessage.Kind.KICK, keep.sessionId(), null, keep.userId(), keep.connId(), null,
                null, instanceId.value(), currentTraceId()));
    }

    private void onBusMessage(BusMessage m) {
        if (m.traceId() != null) {
            MDC.put("traceId", m.traceId());
        }
        try {
            switch (m.kind()) {
                case EVENT -> {
                    String traceId = m.traceId();
                    fanout.execute(m.sessionId(), () -> {
                        MDC.put("traceId", traceId);
                        try {
                            deliverLocal(m.event());
                        } finally {
                            MDC.remove("traceId");
                        }
                    });
                }
                case PRESENCE -> sendToSession(m.sessionId(), m.userId(),
                        json.write(new Frames.Presence(m.userId(), m.status())));
                case TYPING -> sendToSession(m.sessionId(), m.userId(),
                        json.write(new Frames.Typing(m.userId(), Boolean.TRUE.equals(m.typing()))));
                case KICK -> {
                    ClientConnection c = registry.find(m.sessionId(), m.userId());
                    if (c != null && !c.connId().equals(m.connId())) {
                        c.markReplaced();
                        c.close(REPLACED);
                    }
                }
            }
        } finally {
            MDC.remove("traceId");
        }
    }

    private void deliverLocal(StoredEvent e) {
        for (ClientConnection c : registry.forSession(e.sessionId())) {
            delivery.deliver(c, e);
            healPresence(c, e);
            if (e.type() == EventType.SESSION_ENDED) {
                c.markDeparted();
                c.close(SESSION_ENDED);
            } else if (e.type() == EventType.LEAVE && e.userId().equals(c.userId())) {
                // REST 로 LEAVE 한 경우에도 해당 사용자의 실시간 연결을 정리한다
                c.markDeparted();
                c.close(CloseStatus.NORMAL);
            }
        }
    }

    /**
     * 자가 치유: 내 연결이 살아있는데 원장에 "내가 끊겼다(DISCONNECT)" 가 기록된 경우 즉시 RECONNECT 로 바로잡는다.
     * <ul>
     *   <li>이전 연결의 종료 처리와 새 연결 수립이 경합한 경우 (다른 connId)</li>
     *   <li>일시적 정체로 sweeper 가 살아있는 인스턴스를 죽었다고 오판한 경우 (같은 connId)</li>
     * </ul>
     * 이 연결 자체가 닫히면서 기록한 DISCONNECT 는 isOpen()=false 이므로 대상이 아니다.
     */
    private void healPresence(ClientConnection c, StoredEvent e) {
        if (e.type() != EventType.DISCONNECT || !e.userId().equals(c.userId()) || !c.isOpen() || c.departed()) {
            return;
        }
        healer.execute(() -> {
            try {
                presence.markOnline(c);
            } catch (Exception ex) {
                log.debug("presence re-own failed: {}", ex.getMessage());
            }
            try {
                appender.append(c.sessionId(), NewEvent.system("sys:heal:" + c.connId() + ":" + e.seq(),
                        EventType.RECONNECT, c.userId(), json.object().put("connId", c.connId())
                                .put("reason", "self_heal")), "system");
            } catch (ApiException ex) {
                log.debug("heal skipped: {}", ex.getMessage());
            }
        });
    }

    private void sendToSession(UUID sessionId, String exceptUser, String frame) {
        for (ClientConnection c : registry.forSession(sessionId)) {
            if (c.userId().equals(exceptUser)) {
                continue;
            }
            try {
                c.sendRaw(frame);
            } catch (Exception e) {
                log.debug("ephemeral send failed conn={}: {}", c.connId(), e.getMessage());
            }
        }
    }

    private String currentTraceId() {
        Span span = tracer.currentSpan();
        return span == null ? MDC.get("traceId") : span.context().traceId();
    }
}
