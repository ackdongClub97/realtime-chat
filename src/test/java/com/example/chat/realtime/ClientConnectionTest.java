package com.example.chat.realtime;

import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.web.socket.WebSocketSession;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class ClientConnectionTest {

    private final ClientConnection conn = new ClientConnection(UUID.randomUUID(), "bob",
            mock(WebSocketSession.class), 0);

    @Test
    void detectsGapsInClientSequenceWithoutRegressingOnLateArrivals() {
        assertThat(conn.trackClientSeq(null)).isFalse();   // 순번 없는 클라이언트
        assertThat(conn.trackClientSeq(1L)).isFalse();
        assertThat(conn.trackClientSeq(2L)).isFalse();
        assertThat(conn.trackClientSeq(4L)).isTrue();      // 3 누락
        assertThat(conn.trackClientSeq(3L)).isFalse();     // 늦게 도착 (기준을 되돌리지 않음)
        assertThat(conn.trackClientSeq(5L)).isFalse();
        assertThat(conn.trackClientSeq(5L)).isFalse();     // 재전송
    }

    @Test
    void presenceMemberEncodesSessionUserAndConnection() {
        assertThat(conn.presenceKeyMember().split("\\|"))
                .containsExactly(conn.sessionId().toString(), "bob", conn.connId());
    }
}
