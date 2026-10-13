package com.example.chat.domain;

import java.util.Optional;

/**
 * 참여자 상태 머신. write model(participant 테이블)과 복원 reducer 가 같은 전이 규칙을 공유한다.
 *
 * <pre>
 *   (none|LEFT) --JOIN--> JOINED --DISCONNECT--> DISCONNECTED --RECONNECT--> JOINED
 *   (JOINED|DISCONNECTED) --LEAVE--> LEFT
 * </pre>
 * 규칙에 맞지 않는 전이(이미 JOINED 인데 JOIN 등)는 no-op 이다. 따라서 같은 이벤트가 중복 반영되어도 상태가 변하지 않는다.
 */
public enum ParticipantState {
    JOINED,
    DISCONNECTED,
    LEFT;

    public boolean isMember() {
        return this == JOINED || this == DISCONNECTED;
    }

    public static Optional<ParticipantState> transition(ParticipantState current, EventType type) {
        return switch (type) {
            case JOIN -> (current == null || current == LEFT) ? Optional.of(JOINED) : Optional.empty();
            case LEAVE -> (current != null && current.isMember()) ? Optional.of(LEFT) : Optional.empty();
            case DISCONNECT -> current == JOINED ? Optional.of(DISCONNECTED) : Optional.empty();
            case RECONNECT -> current == DISCONNECTED ? Optional.of(JOINED) : Optional.empty();
            default -> Optional.empty();
        };
    }
}
