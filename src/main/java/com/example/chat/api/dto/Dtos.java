package com.example.chat.api.dto;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import com.example.chat.domain.EventType;
import com.example.chat.domain.ParticipantState;
import com.example.chat.domain.SessionStatus;
import com.example.chat.domain.StoredEvent;
import com.example.chat.restore.SessionState;
import com.fasterxml.jackson.databind.JsonNode;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

/**
 * REST 요청/응답 DTO 모음.
 */
public final class Dtos {

    private Dtos() {
    }

    public record CreateSessionRequest(@Size(max = 200) String title) {
    }

    public record SessionResponse(UUID id, String title, SessionStatus status, String createdBy, Instant createdAt,
                                  Instant endedAt, long lastSeq, List<ParticipantResponse> participants) {
    }

    public record ParticipantResponse(String userId, ParticipantState state, Instant joinedAt, Instant leftAt,
                                      @Schema(description = "ONLINE | OFFLINE | UNKNOWN(redis 장애)") String presence) {
    }

    public record SessionPage(List<SessionResponse> items, String nextCursor) {
    }

    public record MembershipRequest(@Size(max = 64) String clientEventId) {
    }

    public record AppendResponse(String clientEventId,
                                 @Schema(description = "ACCEPTED | DUPLICATE | NOOP") String status,
                                 Long seq, long sessionLastSeq) {
    }

    public record IngestRequest(@NotEmpty @Size(max = 100) List<@Valid IngestEvent> events) {
    }

    public record IngestEvent(
            @NotBlank @Size(max = 64) @Schema(description = "멱등성 키. 재전송 시 반드시 같은 값") String clientEventId,
            @NotNull @Schema(description = "MESSAGE | MESSAGE_EDITED | MESSAGE_DELETED | READ") EventType type,
            @Schema(description = "MESSAGE: {text}, MESSAGE_EDITED: {messageId, text}, MESSAGE_DELETED: {messageId}, READ: {upToSeq}")
            JsonNode payload,
            Long clientSeq,
            Instant clientTs) {
    }

    public record IngestResult(String clientEventId,
                               @Schema(description = "ACCEPTED | DUPLICATE | NOOP | REJECTED") String status,
                               Long seq, String errorCode, String message, Boolean retryable) {
    }

    public record IngestResponse(List<IngestResult> results) {
    }

    public record EventView(long seq, EventType type, String clientEventId, String userId, JsonNode payload,
                            Long clientSeq, Instant clientTs, Instant serverReceivedAt) {
        public static EventView of(StoredEvent e) {
            return new EventView(e.seq(), e.type(), e.clientEventId(), e.userId(), e.payload(), e.clientSeq(),
                    e.clientTs(), e.serverReceivedAt());
        }
    }

    public record EventPage(List<EventView> items, Long nextFromSeq) {
    }

    public record MessageView(String messageId, long seq, String senderId, String text, String status,
                              Instant createdAt, Instant updatedAt) {
    }

    public record MessagePage(List<MessageView> items, Long nextBeforeSeq,
                              @Schema(description = "projection 이 반영한 마지막 seq (비동기 지연 확인용)") long projectedSeq,
                              long sessionLastSeq) {
    }

    public record TimelineResponse(
            UUID sessionId,
            Instant requestedAt,
            Long requestedSeq,
            @Schema(description = "복원 기준이 된 마지막 이벤트 seq") long resolvedSeq,
            @Schema(description = "resolvedSeq 이벤트의 서버 수신 시각") Instant resolvedAt,
            SessionStatus status,
            String title,
            String createdBy,
            Instant createdAt,
            Instant endedAt,
            List<TimelineParticipant> participants,
            @Schema(description = "seq 오름차순, 최근 limit 개") List<SessionState.Message> messages,
            int totalMessages,
            long ignoredEvents,
            RestoreMeta restore) {
    }

    public record TimelineParticipant(String userId, ParticipantState state, Instant joinedAt, Instant leftAt,
                                      Instant disconnectedAt, long lastReadSeq, long unreadCount) {
    }

    public record RestoreMeta(long snapshotSeq, int replayedEvents, long durationMicros,
                              @Schema(description = "verify=true 일 때 전체 리플레이 결과와 일치 여부") Boolean verifiedAgainstFullReplay) {
    }

    public record SnapshotResponse(long seq, boolean created, int replayedEvents) {
    }
}
