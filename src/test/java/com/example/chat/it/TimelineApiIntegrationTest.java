package com.example.chat.it;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * GET /sessions/{id}/timeline — HTTP 수준에서 "특정 시점 대화 상태 복원"을 검증한다.
 *
 * <pre>
 * seq 1 SESSION_CREATED  2 JOIN alice  3 JOIN bob  4 MESSAGE m1  5 MESSAGE m2
 *   ── t1 ──
 * seq 6 EDITED m1  7 DELETED m2  8 READ bob(upToSeq 4)  9 LEAVE bob
 * </pre>
 */
class TimelineApiIntegrationTest extends IntegrationTestBase {

    @Autowired
    TestRestTemplate http;

    private UUID sid;
    private Instant t1;

    @BeforeEach
    void conversation() throws InterruptedException {
        sid = UUID.fromString(post("/sessions", "alice", Map.of("title", "timeline")).get("id").asText());
        post("/sessions/" + sid + "/join", "alice", null);
        post("/sessions/" + sid + "/join", "bob", null);
        events("alice", Map.of("clientEventId", "m1", "type", "MESSAGE", "payload", Map.of("text", "original")));
        events("alice", Map.of("clientEventId", "m2", "type", "MESSAGE", "payload", Map.of("text", "to be deleted")));
        Thread.sleep(20);
        t1 = Instant.now();
        Thread.sleep(20);
        events("alice", Map.of("clientEventId", "e1", "type", "MESSAGE_EDITED",
                "payload", Map.of("messageId", "m1", "text", "edited later")));
        events("alice", Map.of("clientEventId", "d2", "type", "MESSAGE_DELETED", "payload", Map.of("messageId", "m2")));
        events("bob", Map.of("clientEventId", "r1", "type", "READ", "payload", Map.of("upToSeq", 4)));
        post("/sessions/" + sid + "/leave", "bob", null);
    }

    @Test
    void restoresMessagesParticipantsAndUnreadAsTheyWereAtTimeT() {
        JsonNode s = timeline("at=" + t1 + "&verify=true").getBody();

        assertThat(s.get("resolvedSeq").asLong()).isEqualTo(5);
        assertThat(message(s, "m1").get("text").asText()).isEqualTo("original");     // 수정은 t1 이후
        assertThat(message(s, "m1").get("status").asText()).isEqualTo("SENT");
        assertThat(message(s, "m2").get("status").asText()).isEqualTo("SENT");       // 삭제는 t1 이후
        assertThat(participant(s, "bob").get("state").asText()).isEqualTo("JOINED"); // 퇴장은 t1 이후
        assertThat(participant(s, "bob").get("unreadCount").asLong()).isEqualTo(2);  // 읽음은 t1 이후
        assertThat(s.get("restore").get("verifiedAgainstFullReplay").asBoolean()).isTrue();
    }

    @Test
    void currentStateReflectsEditDeleteReadAndLeave() {
        JsonNode s = timeline("verify=true").getBody();

        assertThat(s.get("resolvedSeq").asLong()).isEqualTo(9);
        assertThat(message(s, "m1").get("text").asText()).isEqualTo("edited later");
        assertThat(message(s, "m1").get("status").asText()).isEqualTo("EDITED");
        assertThat(message(s, "m2").get("status").asText()).isEqualTo("DELETED");
        assertThat(message(s, "m2").has("text")).isFalse();
        assertThat(participant(s, "bob").get("state").asText()).isEqualTo("LEFT");
        assertThat(participant(s, "bob").get("lastReadSeq").asLong()).isEqualTo(4);
        assertThat(participant(s, "bob").get("unreadCount").asLong()).isZero(); // m2 는 삭제되어 제외
        assertThat(s.get("restore").get("verifiedAgainstFullReplay").asBoolean()).isTrue();
    }

    @Test
    void everySeqCutMatchesFullReplay() {
        for (int seq = 1; seq <= 9; seq++) {
            JsonNode s = timeline("seq=" + seq + "&verify=true").getBody();
            assertThat(s.get("resolvedSeq").asLong()).isEqualTo(seq);
            assertThat(s.get("restore").get("verifiedAgainstFullReplay").asBoolean()).as("seq %d", seq).isTrue();
        }
    }

    @Test
    void atIsInclusiveOfEventsReceivedAtExactlyThatInstant() {
        JsonNode seq5 = http.getForObject("/sessions/" + sid + "/events?fromSeq=5&toSeq=5", JsonNode.class)
                .get("items").get(0);

        JsonNode s = timeline("at=" + seq5.get("serverReceivedAt").asText()).getBody();

        assertThat(s.get("resolvedSeq").asLong()).isEqualTo(5);
    }

    @Test
    void boundaryInputs() {
        assertThat(timeline("at=" + Instant.now().plusSeconds(86_400)).getBody().get("resolvedSeq").asLong())
                .as("미래 시각 → 현재 상태").isEqualTo(9);
        assertThat(timeline("seq=999").getBody().get("resolvedSeq").asLong())
                .as("마지막 seq 초과 → 마지막으로 제한").isEqualTo(9);
        assertThat(timeline("seq=3&at=" + Instant.now()).getBody().get("resolvedSeq").asLong())
                .as("seq 가 at 보다 우선").isEqualTo(3);

        JsonNode limited = timeline("limit=1").getBody();
        assertThat(limited.get("messages")).hasSize(1);
        assertThat(limited.get("messages").get(0).get("messageId").asText()).isEqualTo("m2"); // 최근 1개
        assertThat(limited.get("totalMessages").asInt()).isEqualTo(2);
    }

    @Test
    void errors() {
        ResponseEntity<JsonNode> beforeCreation = timeline("at=" + Instant.now().minusSeconds(3_600));
        assertThat(beforeCreation.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(beforeCreation.getBody().get("code").asText()).isEqualTo("NO_STATE_AT");

        assertThat(timeline("seq=0").getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(timeline("at=not-a-date").getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);

        ResponseEntity<JsonNode> unknown = http.getForEntity("/sessions/" + UUID.randomUUID() + "/timeline",
                JsonNode.class);
        assertThat(unknown.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(unknown.getBody().get("code").asText()).isEqualTo("SESSION_NOT_FOUND");
    }

    private ResponseEntity<JsonNode> timeline(String query) {
        return http.getForEntity("/sessions/" + sid + "/timeline?" + query.replace("+", "%2B"), JsonNode.class);
    }

    private static JsonNode message(JsonNode state, String messageId) {
        for (JsonNode m : state.get("messages")) {
            if (m.get("messageId").asText().equals(messageId)) {
                return m;
            }
        }
        throw new AssertionError("message not found: " + messageId);
    }

    private static JsonNode participant(JsonNode state, String userId) {
        for (JsonNode p : state.get("participants")) {
            if (p.get("userId").asText().equals(userId)) {
                return p;
            }
        }
        throw new AssertionError("participant not found: " + userId);
    }

    private JsonNode post(String path, String user, Object body) {
        HttpHeaders h = new HttpHeaders();
        h.set("X-User-Id", user);
        h.set("Content-Type", "application/json");
        ResponseEntity<JsonNode> r = http.exchange(path, HttpMethod.POST, new HttpEntity<>(body, h), JsonNode.class);
        assertThat(r.getStatusCode().is2xxSuccessful()).as("POST %s", path).isTrue();
        return r.getBody();
    }

    private void events(String user, Map<String, Object> event) {
        JsonNode r = post("/sessions/" + sid + "/events", user, Map.of("events", List.of(event)));
        assertThat(r.get("results").get(0).get("status").asText()).isEqualTo("ACCEPTED");
    }
}
