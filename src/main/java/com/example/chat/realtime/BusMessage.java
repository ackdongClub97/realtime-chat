package com.example.chat.realtime;

import java.util.UUID;

import com.example.chat.domain.StoredEvent;

/**
 * 인스턴스 간 Redis Pub/Sub 메시지. traceId 를 함께 전달해 수신 인스턴스 로그에서도 같은 요청을 추적한다.
 */
public record BusMessage(Kind kind, UUID sessionId, StoredEvent event, String userId, String connId, String status,
                         Boolean typing, String origin, String traceId) {

    public enum Kind {
        /** 저장된 이벤트 팬아웃 */
        EVENT,
        /** 입력 중 표시 (저장하지 않는 휘발성 신호) */
        TYPING,
        /** online/offline 변화 */
        PRESENCE,
        /** 같은 사용자의 다른 인스턴스 연결 종료 요청 (사용자당 세션 연결 1개 규칙) */
        KICK
    }
}
