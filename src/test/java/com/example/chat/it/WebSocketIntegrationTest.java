package com.example.chat.it;

import java.net.URI;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import com.example.chat.api.SessionService;
import com.example.chat.domain.EventType;
import com.example.chat.domain.NewEvent;
import com.example.chat.ingest.EventAppendService;
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
    void nonParticipantIsRejected() throws Exception {
        UUID sid = sessions.create("alice", "ws", null).session().id();
        Recorder r = new Recorder();
        connect(r, sid, "mallory", null);
        EventStoreIntegrationTest.await(() -> r.closed != null);
        assertThat(r.closed.getCode()).isEqualTo(4403);
    }
}
