package com.example.chat.it;

import java.net.URI;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import com.example.chat.api.SessionService;
import com.example.chat.domain.EventType;
import com.example.chat.domain.NewEvent;
import com.example.chat.domain.ParticipantState;
import com.example.chat.domain.SessionStatus;
import com.example.chat.domain.StoredEvent;
import com.example.chat.ingest.EventAppendService;
import com.example.chat.restore.RestoreService;
import com.example.chat.restore.SessionState;
import com.example.chat.store.EventRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import static org.assertj.core.api.Assertions.assertThat;

class WebSocketIntegrationTest extends IntegrationTestBase {

    @LocalServerPort
    int port;
    @Autowired
    SessionService sessions;
    @Autowired
    EventAppendService appender;
    @Autowired
    ObjectMapper mapper;
    @Autowired
    EventRepository events;
    @Autowired
    RestoreService restore;

    class Recorder extends TextWebSocketHandler {
        final List<JsonNode> frames = new CopyOnWriteArrayList<>();
        volatile CloseStatus closed;

        @Override
        protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
            frames.add(mapper.readTree(message.getPayload()));
        }

        @Override
        public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
            closed = status;
        }

        List<Long> eventSeqs() {
            return frames.stream().filter(f -> f.path("type").asText().equals("event"))
                    .map(f -> f.get("seq").asLong()).toList();
        }
    }

    private WebSocketSession connect(Recorder r, UUID sid, String user, Long lastSeq) throws Exception {
        String uri = "ws://localhost:" + port + "/ws?sessionId=" + sid + "&userId=" + user
                + (lastSeq == null ? "" : "&lastSeq=" + lastSeq);
        return new StandardWebSocketClient().execute(r, null, URI.create(uri)).get(5, TimeUnit.SECONDS);
    }

    private void send(WebSocketSession ws, String clientEventId, String text) throws Exception {
        ws.sendMessage(new TextMessage(mapper.writeValueAsString(mapper.createObjectNode()
                .put("type", "send").put("clientEventId", clientEventId).put("eventType", "MESSAGE")
                .set("payload", mapper.createObjectNode().put("text", text)))));
    }

    @Test
    void resumeAfterDisconnectDeliversMissedEventsInOrderWithoutDuplicates() throws Exception {
        UUID sid = sessions.create("alice", "ws", null).session().id();
        appender.append(sid, new NewEvent("ja", EventType.JOIN, "alice", null, null, null), "it");
        appender.append(sid, new NewEvent("jb", EventType.JOIN, "bob", null, null, null), "it");

        Recorder alice = new Recorder();
        WebSocketSession aliceWs = connect(alice, sid, "alice", 0L);
        Recorder bob1 = new Recorder();
        WebSocketSession bobWs = connect(bob1, sid, "bob", 0L);

        send(aliceWs, "m1", "hello");
        send(aliceWs, "m1", "hello"); // 재전송
        EventStoreIntegrationTest.await(() -> bob1.eventSeqs().contains(4L));
        long bobLast = bob1.eventSeqs().getLast();
        bobWs.close(new CloseStatus(3001));

        EventStoreIntegrationTest.await(() -> alice.frames.stream()
                .anyMatch(f -> "DISCONNECT".equals(f.path("eventType").asText())));
        for (int i = 0; i < 5; i++) {
            appender.append(sid, new NewEvent("away-" + i, EventType.MESSAGE, "alice",
                    JsonNodeFactory.instance.objectNode().put("text", "away " + i), null, null), "it");
        }

        Recorder bob2 = new Recorder();
        WebSocketSession bobWs2 = connect(bob2, sid, "bob", bobLast);
        long last = sessions.require(sid).lastSeq();
        EventStoreIntegrationTest.await(() -> !bob2.eventSeqs().isEmpty()
                && bob2.eventSeqs().getLast() >= last);

        List<Long> seqs = bob2.eventSeqs();
        assertThat(seqs.getFirst()).isEqualTo(bobLast + 1);
        for (int i = 1; i < seqs.size(); i++) {
            assertThat(seqs.get(i)).isEqualTo(seqs.get(i - 1) + 1);
        }
        assertThat(bob1.frames.stream().filter(f -> "m1".equals(f.path("clientEventId").asText())
                && "event".equals(f.path("type").asText()))).hasSize(1);
        assertThat(alice.frames.stream().filter(f -> "ack".equals(f.path("type").asText()))
                .map(f -> f.get("status").asText())).containsExactly("ACCEPTED", "DUPLICATE");

        aliceWs.close();
        bobWs2.close();
    }

    @Test
    void disconnectThenReconnectRecordsEventsAndRestoresActiveStatus() throws Exception {
        UUID sid = sessions.create("alice", "ws", null).session().id();
        appender.append(sid, new NewEvent("ja", EventType.JOIN, "alice", null, null, null), "it");
        appender.append(sid, new NewEvent("jb", EventType.JOIN, "bob", null, null, null), "it");

        Recorder alice = new Recorder();
        WebSocketSession aliceWs = connect(alice, sid, "alice", 0L);
        Recorder bob1 = new Recorder();
        WebSocketSession bobWs = connect(bob1, sid, "bob", 0L);
        assertThat(sessions.require(sid).status()).isEqualTo(SessionStatus.ACTIVE);

        bobWs.close(new CloseStatus(3001)); // leave 프레임 없는 종료 = 비정상 끊김
        EventStoreIntegrationTest.await(() -> hasEvent(alice, "DISCONNECT", "bob"));
        assertThat(sessions.require(sid).status()).isEqualTo(SessionStatus.INTERRUPTED);
        long disconnectSeq = sessions.require(sid).lastSeq();

        Recorder bob2 = new Recorder();
        WebSocketSession bobWs2 = connect(bob2, sid, "bob", bob1.eventSeqs().getLast());
        EventStoreIntegrationTest.await(() -> hasEvent(alice, "RECONNECT", "bob"));
        assertThat(sessions.require(sid).status()).isEqualTo(SessionStatus.ACTIVE);
        long reconnectSeq = sessions.require(sid).lastSeq();

        // 원장: bob 의 DISCONNECT, RECONNECT 가 순서대로 정확히 1건씩
        List<StoredEvent> bobLifecycle = events.findRange(sid, 0, reconnectSeq, 100).stream()
                .filter(e -> e.userId().equals("bob")
                        && (e.type() == EventType.DISCONNECT || e.type() == EventType.RECONNECT))
                .toList();
        assertThat(bobLifecycle).extracting(StoredEvent::type)
                .containsExactly(EventType.DISCONNECT, EventType.RECONNECT);
        assertThat(bobLifecycle).extracting(StoredEvent::seq).containsExactly(disconnectSeq, reconnectSeq);

        // 시점 복원도 같은 전이를 재현한다
        SessionState atDisconnect = restore.restoreAtSeq(sid, disconnectSeq).state();
        assertThat(atDisconnect.status).isEqualTo(SessionStatus.INTERRUPTED);
        assertThat(atDisconnect.participants.get("bob").state).isEqualTo(ParticipantState.DISCONNECTED);
        SessionState atReconnect = restore.restoreAtSeq(sid, reconnectSeq).state();
        assertThat(atReconnect.status).isEqualTo(SessionStatus.ACTIVE);
        assertThat(atReconnect.participants.get("bob").state).isEqualTo(ParticipantState.JOINED);

        aliceWs.close();
        bobWs2.close();
    }

    private static boolean hasEvent(Recorder r, String eventType, String userId) {
        return r.frames.stream().anyMatch(f -> "event".equals(f.path("type").asText())
                && eventType.equals(f.path("eventType").asText()) && userId.equals(f.path("userId").asText()));
    }

    @Test
    void nonParticipantIsRejected() throws Exception {
        UUID sid = sessions.create("alice", "ws", null).session().id();
        Recorder r = new Recorder();
        connect(r, sid, "mallory", null);
        EventStoreIntegrationTest.await(() -> r.closed != null);
        assertThat(r.closed.getCode()).isEqualTo(4403);
    }
}
