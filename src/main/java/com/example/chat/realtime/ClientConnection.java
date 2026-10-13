package com.example.chat.realtime;

import java.io.IOException;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

import com.example.chat.domain.StoredEvent;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

/**
 * WebSocket 연결 1개. lastSentSeq 는 이 연결로 전송을 마친 마지막 seq 로, 전송 순서·중복·누락 판단의 기준이다.
 */
public class ClientConnection {

    private final String connId = UUID.randomUUID().toString();
    private final UUID sessionId;
    private final String userId;
    private final WebSocketSession ws;
    private final ReentrantLock lock = new ReentrantLock();

    private long lastSentSeq;
    /** 순서가 앞서 도착한 이벤트 보관 (lock 보유 중에만 접근) */
    private final TreeMap<Long, StoredEvent> pending = new TreeMap<>();
    private boolean reorderTimerArmed;
    private Long lastClientSeq;
    /** 같은 사용자의 새 연결로 교체됨 → 종료 시 DISCONNECT 를 기록하지 않는다 */
    private volatile boolean replaced;
    /** LEAVE / 세션 종료로 정상 이탈 → 종료 시 DISCONNECT 를 기록하지 않는다 */
    private volatile boolean departed;

    public ClientConnection(UUID sessionId, String userId, WebSocketSession ws, long initialSeq) {
        this.sessionId = sessionId;
        this.userId = userId;
        this.ws = ws;
        this.lastSentSeq = initialSeq;
    }

    public String connId() {
        return connId;
    }

    public UUID sessionId() {
        return sessionId;
    }

    public String userId() {
        return userId;
    }

    public String presenceKeyMember() {
        return sessionId + "|" + userId + "|" + connId;
    }

    public ReentrantLock lock() {
        return lock;
    }

    /** lock 보유 중에만 호출 */
    long lastSentSeq() {
        return lastSentSeq;
    }

    /** lock 보유 중에만 호출 */
    void lastSentSeq(long seq) {
        this.lastSentSeq = seq;
    }

    /** lock 보유 중에만 호출 */
    TreeMap<Long, StoredEvent> pending() {
        return pending;
    }

    /** lock 보유 중에만 호출 */
    boolean reorderTimerArmed() {
        return reorderTimerArmed;
    }

    /** lock 보유 중에만 호출 */
    void reorderTimerArmed(boolean armed) {
        this.reorderTimerArmed = armed;
    }

    public long lastSentSeqSnapshot() {
        lock.lock();
        try {
            return lastSentSeq;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 클라이언트 송신 순번(clientSeq)의 구멍을 감지한다. 서버 정렬 기준은 seq 이므로 보정하지 않고 관측만 한다.
     *
     * @return 구멍이 있으면 true
     */
    public synchronized boolean trackClientSeq(Long clientSeq) {
        if (clientSeq == null) {
            return false;
        }
        boolean gap = lastClientSeq != null && clientSeq > lastClientSeq + 1;
        if (lastClientSeq == null || clientSeq > lastClientSeq) {
            lastClientSeq = clientSeq;
        }
        return gap;
    }

    public boolean isOpen() {
        return ws.isOpen();
    }

    public void sendRaw(String json) throws IOException {
        ws.sendMessage(new TextMessage(json));
    }

    public void close(CloseStatus status) {
        try {
            ws.close(status);
        } catch (IOException ignored) {
            // 이미 끊긴 연결
        }
    }

    public boolean replaced() {
        return replaced;
    }

    public void markReplaced() {
        this.replaced = true;
    }

    public boolean departed() {
        return departed;
    }

    public void markDeparted() {
        this.departed = true;
    }
}
