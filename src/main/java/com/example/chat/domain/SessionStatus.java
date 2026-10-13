package com.example.chat.domain;

import java.util.Collection;

public enum SessionStatus {
    ACTIVE,
    /** 참여 중인 사용자 중 비정상 연결 종료(DISCONNECT) 후 아직 재연결하지 않은 사용자가 있음 */
    INTERRUPTED,
    ENDED;

    public static SessionStatus derive(boolean ended, Collection<ParticipantState> participantStates) {
        if (ended) {
            return ENDED;
        }
        return participantStates.contains(ParticipantState.DISCONNECTED) ? INTERRUPTED : ACTIVE;
    }
}
