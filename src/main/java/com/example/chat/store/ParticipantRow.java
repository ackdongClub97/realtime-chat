package com.example.chat.store;

import java.time.Instant;
import java.util.UUID;

import com.example.chat.domain.ParticipantState;

public record ParticipantRow(
        UUID sessionId,
        String userId,
        ParticipantState state,
        Instant joinedAt,
        Instant leftAt,
        long lastEventSeq
) {
}
