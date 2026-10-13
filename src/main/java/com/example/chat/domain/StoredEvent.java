package com.example.chat.domain;

import java.time.Instant;
import java.util.UUID;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * 이벤트 원장에 저장된 이벤트. seq 는 세션 내에서 서버가 발급한 gap-free 단조 증가 번호이며 유일한 정렬 기준이다.
 */
public record StoredEvent(
        UUID sessionId,
        long seq,
        String clientEventId,
        EventType type,
        String userId,
        JsonNode payload,
        Long clientSeq,
        Instant clientTs,
        Instant serverReceivedAt
) {

    public String text(String field) {
        JsonNode node = payload == null ? null : payload.get(field);
        return node == null || node.isNull() ? null : node.asText();
    }

    public Long number(String field) {
        JsonNode node = payload == null ? null : payload.get(field);
        return node == null || !node.canConvertToLong() ? null : node.asLong();
    }
}
