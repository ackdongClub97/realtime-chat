package com.example.chat.realtime;

import java.time.Instant;
import java.util.UUID;

import com.example.chat.domain.StoredEvent;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * WebSocket 프로토콜 프레임 (JSON text frame). 상세 명세는 docs/websocket-protocol.md.
 */
public final class Frames {

    private Frames() {
    }

    /** 클라이언트 → 서버 */
    public record ClientFrame(String type, String clientEventId, String eventType, JsonNode payload, Long clientSeq,
                              Instant clientTs, Long lastSeq) {
    }

    public record Welcome(String type, String connId, UUID sessionId, String userId, long serverLastSeq,
                          long resumeFrom) {
        public Welcome(String connId, UUID sessionId, String userId, long serverLastSeq, long resumeFrom) {
            this("welcome", connId, sessionId, userId, serverLastSeq, resumeFrom);
        }
    }

    public record Ack(String type, String clientEventId, Long seq, String status) {
        public Ack(String clientEventId, Long seq, String status) {
            this("ack", clientEventId, seq, status);
        }
    }

    public record Error(String type, String clientEventId, String code, String message, boolean retryable) {
        public Error(String clientEventId, String code, String message, boolean retryable) {
            this("error", clientEventId, code, message, retryable);
        }
    }

    public record Event(String type, long seq, String eventType, String clientEventId, String userId,
                        JsonNode payload, Instant serverReceivedAt) {
        public static Event of(StoredEvent e) {
            return new Event("event", e.seq(), e.type().name(), e.clientEventId(), e.userId(), e.payload(),
                    e.serverReceivedAt());
        }
    }

    public record Presence(String type, String userId, String status) {
        public Presence(String userId, String status) {
            this("presence", userId, status);
        }
    }

    public record Typing(String type, String userId, boolean typing) {
        public Typing(String userId, boolean typing) {
            this("typing", userId, typing);
        }
    }

    public record Pong(String type, long serverLastSeq) {
        public Pong(long serverLastSeq) {
            this("pong", serverLastSeq);
        }
    }
}
