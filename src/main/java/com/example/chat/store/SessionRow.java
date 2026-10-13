package com.example.chat.store;

import java.time.Instant;
import java.util.UUID;

import com.example.chat.domain.SessionStatus;

public record SessionRow(
        UUID id,
        String title,
        SessionStatus status,
        String createdBy,
        Instant createdAt,
        Instant endedAt,
        long lastSeq,
        Instant lastEventAt
) {
}
