package com.example.chat.domain;

import java.util.EnumSet;
import java.util.Set;

public enum EventType {
    SESSION_CREATED,
    JOIN,
    LEAVE,
    DISCONNECT,
    RECONNECT,
    MESSAGE,
    MESSAGE_EDITED,
    MESSAGE_DELETED,
    READ,
    SESSION_ENDED;

    /** 클라이언트가 이벤트 수집 API / WebSocket send 로 직접 보낼 수 있는 타입. 나머지는 전용 API 또는 서버가 생성한다. */
    public static final Set<EventType> CLIENT_SENDABLE = EnumSet.of(MESSAGE, MESSAGE_EDITED, MESSAGE_DELETED, READ);

    public boolean affectsParticipant() {
        return this == JOIN || this == LEAVE || this == DISCONNECT || this == RECONNECT;
    }
}
