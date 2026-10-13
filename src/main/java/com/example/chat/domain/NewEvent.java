package com.example.chat.domain;

import java.time.Instant;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

/**
 * append 요청. clientEventId 는 멱등성 키로, 클라이언트가 재전송할 때 반드시 같은 값을 사용해야 한다.
 */
public record NewEvent(
        String clientEventId,
        EventType type,
        String userId,
        JsonNode payload,
        Long clientSeq,
        Instant clientTs
) {

    public NewEvent {
        if (payload == null || payload.isNull()) {
            payload = JsonNodeFactory.instance.objectNode();
        }
    }

    public static NewEvent system(String clientEventId, EventType type, String userId, JsonNode payload) {
        return new NewEvent(clientEventId, type, userId, payload, null, null);
    }
}
