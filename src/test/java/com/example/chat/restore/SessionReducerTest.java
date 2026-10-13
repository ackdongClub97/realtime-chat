package com.example.chat.restore;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import com.example.chat.domain.EventType;
import com.example.chat.domain.ParticipantState;
import com.example.chat.domain.SessionStatus;
import com.example.chat.domain.StoredEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 복원 로직의 결정성(Determinism) 검증. DB 없이 순수 reducer 만 테스트한다.
 */
class SessionReducerTest {

    private static final JsonMapper MAPPER = JsonMapper.builder().findAndAddModules()
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS).build();
    private static final UUID SID = UUID.fromString("00000000-0000-0000-0000-000000000001");
    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");

    private final List<StoredEvent> log = new ArrayList<>();

    private StoredEvent add(EventType type, String user, String clientEventId, ObjectNode payload) {
        StoredEvent e = new StoredEvent(SID, log.size() + 1, clientEventId, type, user,
                payload == null ? JsonNodeFactory.instance.objectNode() : payload, null, null,
                T0.plusSeconds(log.size()));
        log.add(e);
        return e;
    }

    private static ObjectNode json(String... kv) {
        ObjectNode n = JsonNodeFactory.instance.objectNode();
        for (int i = 0; i < kv.length; i += 2) {
            n.put(kv[i], kv[i + 1]);
        }
        return n;
    }

    /** 대표 시나리오: 입장, 메시지, 수정, 삭제, 읽음, 끊김/재연결, 퇴장, 종료 + 규칙 위반 이벤트 */
    private void conversation() {
        add(EventType.SESSION_CREATED, "alice", "c", json("title", "t"));
        add(EventType.JOIN, "alice", "j1", null);
        add(EventType.JOIN, "bob", "j2", null);
        add(EventType.MESSAGE, "alice", "m1", json("text", "hi"));
        add(EventType.MESSAGE_EDITED, "alice", "e1", json("messageId", "m1", "text", "hi!"));
        add(EventType.MESSAGE, "bob", "m2", json("text", "yo"));
        add(EventType.MESSAGE_EDITED, "alice", "e-unknown", json("messageId", "nope", "text", "x")); // 대상 없음
        add(EventType.MESSAGE_DELETED, "bob", "d2", json("messageId", "m2"));
        add(EventType.MESSAGE_EDITED, "bob", "e2", json("messageId", "m2", "text", "zombie"));    // 삭제 후 수정
        add(EventType.MESSAGE_EDITED, "bob", "e3", json("messageId", "m1", "text", "hijack"));    // 남의 메시지
        ObjectNode read = JsonNodeFactory.instance.objectNode().put("upToSeq", 4);
        add(EventType.READ, "bob", "r1", read);
        add(EventType.DISCONNECT, "bob", "dc", null);
        add(EventType.MESSAGE, "alice", "m3", json("text", "are you there?"));
        add(EventType.RECONNECT, "bob", "rc", null);
        add(EventType.JOIN, "bob", "j-dup", null);                                                  // 이미 참여 중
        add(EventType.LEAVE, "alice", "l1", null);
        add(EventType.SESSION_ENDED, "bob", "end", null);
    }

    private JsonNode replayToJson(List<StoredEvent> events) {
        return MAPPER.valueToTree(SessionReducer.replay(SessionState.empty(SID), events));
    }

    @Test
    void appliesConversationRules() {
        conversation();
        SessionState s = SessionReducer.replay(SessionState.empty(SID), log);

        assertThat(s.lastSeq).isEqualTo(log.size());
        assertThat(s.status).isEqualTo(SessionStatus.ENDED);
        assertThat(s.messages.get("m1").text).isEqualTo("hi!");
        assertThat(s.messages.get("m1").status).isEqualTo(SessionState.MessageStatus.EDITED);
        assertThat(s.messages.get("m2").status).isEqualTo(SessionState.MessageStatus.DELETED);
        assertThat(s.messages.get("m2").text).isNull();
        assertThat(s.participants.get("alice").state).isEqualTo(ParticipantState.LEFT);
        assertThat(s.participants.get("bob").state).isEqualTo(ParticipantState.JOINED);
        assertThat(s.participants.get("bob").lastReadSeq).isEqualTo(4);
        assertThat(s.ignoredEvents).isEqualTo(4); // 대상 없음, 삭제 후 수정, 남의 메시지, 중복 JOIN
    }

    @Test
    void sameResultRegardlessOfArrivalOrderAndDuplicates() {
        conversation();
        JsonNode expected = replayToJson(log);
        Random random = new Random(42);
        for (int round = 0; round < 200; round++) {
            List<StoredEvent> scrambled = new ArrayList<>(log);
            for (int i = 0; i < 1 + random.nextInt(10); i++) {
                scrambled.add(log.get(random.nextInt(log.size()))); // 중복
            }
            Collections.shuffle(scrambled, random);                    // 순서 뒤바뀜
            assertThat(replayToJson(scrambled)).isEqualTo(expected);
        }
    }

    @Test
    void reapplyingAnAlreadyAppliedEventIsNoop() {
        conversation();
        SessionState s = SessionReducer.replay(SessionState.empty(SID), log);
        JsonNode before = MAPPER.valueToTree(s);
        for (StoredEvent e : log) {
            assertThat(SessionReducer.apply(s, e)).isFalse();
        }
        assertThat((JsonNode) MAPPER.valueToTree(s)).isEqualTo(before);
    }

    @Test
    void snapshotPlusReplayEqualsFullReplayAtEveryCut() throws Exception {
        conversation();
        JsonNode full = replayToJson(log);
        for (int cut = 0; cut <= log.size(); cut++) {
            SessionState partial = SessionReducer.replay(SessionState.empty(SID), log.subList(0, cut));
            // 스냅샷 저장/로드 (JSON 왕복) 후 나머지 리플레이
            SessionState restored = MAPPER.readValue(MAPPER.writeValueAsString(partial), SessionState.class);
            SessionReducer.replay(restored, log.subList(cut, log.size()));
            assertThat((JsonNode) MAPPER.valueToTree(restored)).as("cut at %d", cut).isEqualTo(full);
        }
    }

    @Test
    void missingEventFailsInsteadOfProducingWrongState() {
        conversation();
        List<StoredEvent> withGap = new ArrayList<>(log);
        withGap.remove(5);
        assertThatThrownBy(() -> SessionReducer.replay(SessionState.empty(SID), withGap))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("gap");
    }

    @Test
    void readPointerOnlyMovesForward() {
        add(EventType.SESSION_CREATED, "a", "c", null);
        add(EventType.JOIN, "a", "j", null);
        add(EventType.MESSAGE, "a", "m1", json("text", "1"));
        add(EventType.MESSAGE, "a", "m2", json("text", "2"));
        add(EventType.READ, "a", "r-new", JsonNodeFactory.instance.objectNode().put("upToSeq", 4));
        add(EventType.READ, "a", "r-old", JsonNodeFactory.instance.objectNode().put("upToSeq", 3));
        SessionState s = SessionReducer.replay(SessionState.empty(SID), log);
        assertThat(s.participants.get("a").lastReadSeq).isEqualTo(4);
    }

    @Test
    void disconnectedParticipantMakesSessionInterrupted() {
        add(EventType.SESSION_CREATED, "a", "c", null);
        add(EventType.JOIN, "a", "j", null);
        add(EventType.DISCONNECT, "a", "d", null);
        SessionState s = SessionReducer.replay(SessionState.empty(SID), log);
        assertThat(s.status).isEqualTo(SessionStatus.INTERRUPTED);
        add(EventType.RECONNECT, "a", "r", null);
        SessionReducer.apply(s, log.getLast());
        assertThat(s.status).isEqualTo(SessionStatus.ACTIVE);
    }
}
