package com.example.chat.restore;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.TreeMap;
import java.util.UUID;

import com.example.chat.domain.ParticipantState;
import com.example.chat.domain.SessionStatus;

/**
 * 특정 seq 시점의 세션 상태. 스냅샷(JSONB)으로 그대로 직렬화된다.
 * <p>
 * 직렬화 결과가 항상 같도록 participants 는 TreeMap(userId 정렬), messages 는 seq 순 삽입 순서를 유지한다.
 * 구조를 바꾸면 {@link #SCHEMA_VERSION} 을 올려 기존 스냅샷을 무시하게 한다(이벤트 원장에서 다시 만들 수 있음).
 */
public class SessionState {

    public static final int SCHEMA_VERSION = 1;

    public UUID sessionId;
    public long lastSeq;
    public Instant lastEventAt;
    public SessionStatus status;
    public String title;
    public String createdBy;
    public Instant createdAt;
    public Instant endedAt;
    public TreeMap<String, Participant> participants = new TreeMap<>();
    public LinkedHashMap<String, Message> messages = new LinkedHashMap<>();
    /** 규칙에 맞지 않아 상태에 반영되지 않은 이벤트 수 (예: 존재하지 않는 메시지 수정) */
    public long ignoredEvents;

    public static SessionState empty(UUID sessionId) {
        SessionState s = new SessionState();
        s.sessionId = sessionId;
        return s;
    }

    public static class Participant {
        public String userId;
        public ParticipantState state;
        public Instant joinedAt;
        public Instant leftAt;
        public Instant disconnectedAt;
        public long lastReadSeq;
    }

    public static class Message {
        public String messageId;
        public long seq;
        public String senderId;
        public String text;
        public MessageStatus status;
        public Instant createdAt;
        public Instant editedAt;
        public Instant deletedAt;
        public int editCount;
    }

    public enum MessageStatus {
        SENT, EDITED, DELETED
    }
}
