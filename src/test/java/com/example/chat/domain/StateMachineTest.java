package com.example.chat.domain;

import java.util.List;
import java.util.Optional;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 참여자 상태 머신의 모든 (현재 상태 × 이벤트) 조합과 세션 상태 파생 규칙.
 * write model 과 복원 reducer 가 이 규칙을 공유하므로 여기서 한 번에 고정한다.
 */
class StateMachineTest {

    @ParameterizedTest(name = "{0} --{1}--> {2}")
    @CsvSource({
            // 현재 상태(NONE = 참여 이력 없음), 이벤트, 다음 상태(NOOP = 전이 없음)
            "NONE,         JOIN,       JOINED",
            "JOINED,       JOIN,       NOOP",
            "DISCONNECTED, JOIN,       NOOP",
            "LEFT,         JOIN,       JOINED",
            "NONE,         LEAVE,      NOOP",
            "JOINED,       LEAVE,      LEFT",
            "DISCONNECTED, LEAVE,      LEFT",
            "LEFT,         LEAVE,      NOOP",
            "NONE,         DISCONNECT, NOOP",
            "JOINED,       DISCONNECT, DISCONNECTED",
            "DISCONNECTED, DISCONNECT, NOOP",
            "LEFT,         DISCONNECT, NOOP",
            "NONE,         RECONNECT,  NOOP",
            "JOINED,       RECONNECT,  NOOP",
            "DISCONNECTED, RECONNECT,  JOINED",
            "LEFT,         RECONNECT,  NOOP",
            "JOINED,       MESSAGE,    NOOP",
            "JOINED,       READ,       NOOP"
    })
    void participantTransitions(String from, EventType event, String to) {
        ParticipantState current = from.equals("NONE") ? null : ParticipantState.valueOf(from);
        Optional<ParticipantState> next = ParticipantState.transition(current, event);
        if (to.equals("NOOP")) {
            assertThat(next).isEmpty();
        } else {
            assertThat(next).contains(ParticipantState.valueOf(to));
        }
    }

    @Test
    void membershipIncludesDisconnectedButNotLeft() {
        assertThat(ParticipantState.JOINED.isMember()).isTrue();
        assertThat(ParticipantState.DISCONNECTED.isMember()).isTrue();
        assertThat(ParticipantState.LEFT.isMember()).isFalse();
    }

    @Test
    void sessionStatusDerivation() {
        assertThat(SessionStatus.derive(false, List.of())).isEqualTo(SessionStatus.ACTIVE);
        assertThat(SessionStatus.derive(false, List.of(ParticipantState.JOINED, ParticipantState.LEFT)))
                .isEqualTo(SessionStatus.ACTIVE);
        assertThat(SessionStatus.derive(false, List.of(ParticipantState.JOINED, ParticipantState.DISCONNECTED)))
                .isEqualTo(SessionStatus.INTERRUPTED);
        // 종료는 최종 상태: 끊긴 참여자가 있어도 ENDED
        assertThat(SessionStatus.derive(true, List.of(ParticipantState.DISCONNECTED)))
                .isEqualTo(SessionStatus.ENDED);
    }

    @Test
    void onlyMessageLevelEventsAreClientSendable() {
        assertThat(EventType.CLIENT_SENDABLE).containsExactlyInAnyOrder(
                EventType.MESSAGE, EventType.MESSAGE_EDITED, EventType.MESSAGE_DELETED, EventType.READ);
    }
}
