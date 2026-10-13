package com.example.chat.realtime;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.BooleanSupplier;

import com.example.chat.common.Json;
import com.example.chat.config.ChatMetrics;
import com.example.chat.domain.EventType;
import com.example.chat.domain.StoredEvent;
import com.example.chat.store.EventRepository;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * 연결별 전송 불변식: 클라이언트는 항상 seq 가 1씩 증가하는 스트림만 받는다.
 */
class EventDeliveryTest {

    private static final UUID SID = UUID.fromString("00000000-0000-0000-0000-0000000000bb");

    private final EventRepository events = mock(EventRepository.class);
    private final WebSocketSession ws = mock(WebSocketSession.class);
    private final List<Long> sent = new ArrayList<>();
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final Json json = new Json(JsonMapper.builder().findAndAddModules()
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS).build());
    private EventDelivery delivery;
    private ClientConnection conn;

    @BeforeEach
    void setUp() throws IOException {
        when(ws.isOpen()).thenReturn(true);
        doAnswer(inv -> {
            TextMessage m = inv.getArgument(0);
            sent.add(json.readTree(m.getPayload()).get("seq").asLong());
            return null;
        }).when(ws).sendMessage(any());
        delivery = new EventDelivery(events, json, new ChatMetrics(registry));
        conn = new ClientConnection(SID, "bob", ws, 0);
    }

    @AfterEach
    void tearDown() {
        delivery.shutdown();
    }

    private static StoredEvent ev(long seq) {
        return new StoredEvent(SID, seq, "e" + seq, EventType.MESSAGE, "alice",
                JsonNodeFactory.instance.objectNode().put("text", "m" + seq), null, null, Instant.EPOCH);
    }

    private static List<StoredEvent> range(long fromExclusive, long toInclusive) {
        List<StoredEvent> list = new ArrayList<>();
        for (long s = fromExclusive + 1; s <= toInclusive; s++) {
            list.add(ev(s));
        }
        return list;
    }

    private double counter(String name) {
        var c = registry.find(name).counter();
        return c == null ? 0 : c.count();
    }

    private static void await(BooleanSupplier cond) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 3000;
        while (!cond.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError("condition not met");
            }
            Thread.sleep(10);
        }
    }

    @Test
    void inOrderEventsAreSentImmediately() {
        delivery.deliver(conn, ev(1));
        delivery.deliver(conn, ev(2));
        delivery.deliver(conn, ev(3));

        assertThat(sent).containsExactly(1L, 2L, 3L);
        assertThat(conn.lastSentSeqSnapshot()).isEqualTo(3);
    }

    @Test
    void alreadySentEventsAreDropped() {
        delivery.deliver(conn, ev(1));
        delivery.deliver(conn, ev(1));
        delivery.deliver(conn, ev(2));
        delivery.deliver(conn, ev(1));

        assertThat(sent).containsExactly(1L, 2L);
    }

    @Test
    void outOfOrderArrivalIsReorderedWithoutTouchingTheLedger() {
        delivery.deliver(conn, ev(3));
        delivery.deliver(conn, ev(2));
        assertThat(sent).isEmpty();

        delivery.deliver(conn, ev(1));

        assertThat(sent).containsExactly(1L, 2L, 3L);
        assertThat(counter("chat.delivery.reordered")).isEqualTo(2);
        verify(events, never()).findRange(any(), anyLong(), anyLong(), anyInt());
    }

    @Test
    void missingEventIsFilledFromLedgerAfterHoldTimeout() throws InterruptedException {
        delivery.deliver(conn, ev(1));
        when(events.findRange(SID, 1, 3, 500)).thenReturn(range(1, 3));

        delivery.deliver(conn, ev(4)); // 2, 3 은 Redis 에서 유실됐다고 가정
        assertThat(sent).containsExactly(1L);

        await(() -> sent.size() == 4);
        assertThat(sent).containsExactly(1L, 2L, 3L, 4L);
        assertThat(counter("chat.delivery.gap.filled")).isEqualTo(2);
    }

    @Test
    void catchUpReplaysLedgerThenFlushesHeldEvents() {
        when(events.findRange(SID, 0, 5, 500)).thenReturn(range(0, 5));
        delivery.deliver(conn, ev(7));
        delivery.deliver(conn, ev(6));

        long replayed = delivery.catchUp(conn, 5);

        assertThat(replayed).isEqualTo(5);
        assertThat(sent).containsExactly(1L, 2L, 3L, 4L, 5L, 6L, 7L);
    }

    @Test
    void resumeUsesClientsLastSeqAsBaseline() {
        when(events.findRange(SID, 0, 2, 500)).thenReturn(range(0, 2));
        when(events.findRange(SID, 2, 4, 500)).thenReturn(range(2, 4));
        delivery.catchUp(conn, 2);
        sent.clear();

        // 클라이언트는 seq 2 까지 받았다고 알림 → 3, 4 를 받는다
        delivery.resumeFrom(conn, 2, 4);
        assertThat(sent).containsExactly(3L, 4L);

        // 클라이언트가 실제로는 seq 0 까지만 가졌다면 처음부터 다시 받는다
        sent.clear();
        when(events.findRange(SID, 0, 4, 500)).thenReturn(range(0, 4));
        delivery.resumeFrom(conn, 0, 4);
        assertThat(sent).containsExactly(1L, 2L, 3L, 4L);
    }

    @Test
    void resumeBaselineIsCappedToServerLastSeq() {
        delivery.resumeFrom(conn, 99, 4);

        assertThat(conn.lastSentSeqSnapshot()).isEqualTo(4);
        assertThat(sent).isEmpty();
    }

    @Test
    void sendFailureClosesConnectionForClientResume() throws IOException {
        doThrow(new IOException("broken pipe")).when(ws).sendMessage(any());

        delivery.deliver(conn, ev(1));

        verify(ws).close(CloseStatus.SESSION_NOT_RELIABLE);
        assertThat(conn.lastSentSeqSnapshot()).isZero();
    }

    @Test
    void closedConnectionReceivesNothing() throws IOException {
        when(ws.isOpen()).thenReturn(false);

        delivery.deliver(conn, ev(1));

        verify(ws, never()).sendMessage(any());
    }

    @Test
    void ledgerFillFailureIsRetried() throws InterruptedException {
        delivery.deliver(conn, ev(1));
        when(events.findRange(eq(SID), eq(1L), eq(2L), anyInt()))
                .thenThrow(new RuntimeException("db down"))
                .thenReturn(range(1, 2));

        delivery.deliver(conn, ev(3));

        await(() -> sent.size() == 3);
        assertThat(sent).containsExactly(1L, 2L, 3L);
    }
}
