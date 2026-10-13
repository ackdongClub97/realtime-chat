package com.example.chat.realtime;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.example.chat.common.Json;
import com.example.chat.config.ChatMetrics;
import com.example.chat.domain.StoredEvent;
import com.example.chat.store.EventRepository;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;

/**
 * 연결별 순서 보장 전송.
 * <ul>
 *   <li>seq &lt;= lastSentSeq : 이미 보낸 이벤트 → 버림 (Redis 중복 수신, resume 과 live 의 겹침)</li>
 *   <li>seq == lastSentSeq+1 : 전송 후, 보관 중인 후속 이벤트를 이어서 전송</li>
 *   <li>seq &gt; lastSentSeq+1 : 재정렬 버퍼에 보관하고 최대 {@link #REORDER_HOLD_MS} 동안 빠진 seq 를 기다린다.
 *       서로 다른 인스턴스에서 커밋된 이벤트는 Redis 경유 여부에 따라 도착 순서가 바뀌는 것이 정상이므로
 *       대부분 짧은 대기로 해결된다. 끝내 오지 않으면(Redis 유실 등) DB 원장에서 채운다(gap-fill).</li>
 * </ul>
 * 결과적으로 클라이언트는 항상 seq 가 1씩 증가하는 스트림만 받는다 (gap-free, no-dup, in-order).
 */
@Component
public class EventDelivery {

    static final long REORDER_HOLD_MS = 200;
    private static final Logger log = LoggerFactory.getLogger(EventDelivery.class);
    private static final int PAGE_SIZE = 500;

    private final EventRepository events;
    private final Json json;
    private final ChatMetrics metrics;
    private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "reorder-timer");
        t.setDaemon(true);
        return t;
    });

    public EventDelivery(EventRepository events, Json json, ChatMetrics metrics) {
        this.events = events;
        this.json = json;
        this.metrics = metrics;
    }

    @PreDestroy
    void shutdown() {
        timer.shutdownNow();
    }

    public void deliver(ClientConnection c, StoredEvent e) {
        c.lock().lock();
        try {
            if (!c.isOpen() || e.seq() <= c.lastSentSeq()) {
                return;
            }
            if (e.seq() == c.lastSentSeq() + 1) {
                if (send(c, e)) {
                    flushContiguous(c);
                }
                return;
            }
            c.pending().put(e.seq(), e);
            metrics.deliveryReordered();
            armTimer(c);
        } finally {
            c.lock().unlock();
        }
    }

    /** resume: lastSentSeq 이후 ~ toSeq 까지 원장에서 재전송. */
    public long catchUp(ClientConnection c, long toSeq) {
        c.lock().lock();
        try {
            long n = sendRange(c, toSeq);
            flushContiguous(c);
            return n;
        } finally {
            c.lock().unlock();
        }
    }

    /** resume 요청: 클라이언트가 실제로 가진 마지막 seq 로 기준점을 맞춘 뒤 재전송. 서버 기준이 아니라 클라이언트 기준이다. */
    public long resumeFrom(ClientConnection c, long clientLastSeq, long toSeq) {
        c.lock().lock();
        try {
            c.lastSentSeq(Math.max(0, Math.min(clientLastSeq, toSeq)));
            long n = sendRange(c, toSeq);
            flushContiguous(c);
            return n;
        } finally {
            c.lock().unlock();
        }
    }

    private void armTimer(ClientConnection c) {
        if (c.reorderTimerArmed()) {
            return;
        }
        c.reorderTimerArmed(true);
        timer.schedule(() -> onHoldExpired(c), REORDER_HOLD_MS, TimeUnit.MILLISECONDS);
    }

    /** 대기 시간 안에 빠진 seq 가 오지 않음 → 원장에서 채운다. */
    private void onHoldExpired(ClientConnection c) {
        c.lock().lock();
        try {
            c.reorderTimerArmed(false);
            if (!c.isOpen() || c.pending().isEmpty()) {
                return;
            }
            long firstHeld = c.pending().firstKey();
            if (firstHeld > c.lastSentSeq() + 1) {
                long filled = sendRange(c, firstHeld - 1);
                metrics.deliveryGapFilled(filled);
                log.info("delivery gap filled from ledger conn={} session={} events={}", c.connId(), c.sessionId(),
                        filled);
            }
            flushContiguous(c);
            if (!c.pending().isEmpty()) {
                armTimer(c);
            }
        } catch (RuntimeException ex) {
            log.warn("gap-fill failed conn={}: {}", c.connId(), ex.getMessage());
            armTimer(c);
        } finally {
            c.lock().unlock();
        }
    }

    /** 보관 중인 이벤트 중 이어지는 것을 전송하고, 이미 보낸 것은 버린다. */
    private void flushContiguous(ClientConnection c) {
        Map.Entry<Long, StoredEvent> head;
        while ((head = c.pending().firstEntry()) != null) {
            long seq = head.getKey();
            if (seq <= c.lastSentSeq()) {
                c.pending().pollFirstEntry();
            } else if (seq == c.lastSentSeq() + 1) {
                c.pending().pollFirstEntry();
                if (!send(c, head.getValue())) {
                    return;
                }
            } else {
                return;
            }
        }
    }

    private long sendRange(ClientConnection c, long toSeq) {
        long sent = 0;
        while (c.isOpen() && c.lastSentSeq() < toSeq) {
            List<StoredEvent> page = events.findRange(c.sessionId(), c.lastSentSeq(), toSeq, PAGE_SIZE);
            if (page.isEmpty()) {
                break;
            }
            for (StoredEvent e : page) {
                if (!send(c, e)) {
                    return sent;
                }
                sent++;
            }
        }
        return sent;
    }

    private boolean send(ClientConnection c, StoredEvent e) {
        try {
            c.sendRaw(json.write(Frames.Event.of(e)));
            c.lastSentSeq(e.seq());
            return true;
        } catch (IOException | RuntimeException ex) {
            // 느린 소비자(버퍼 초과) 포함: 연결을 끊고 클라이언트가 lastSeq 로 resume 하도록 한다.
            log.warn("send failed conn={} seq={}: {}", c.connId(), e.seq(), ex.getMessage());
            c.pending().clear();
            c.close(CloseStatus.SESSION_NOT_RELIABLE);
            return false;
        }
    }
}
