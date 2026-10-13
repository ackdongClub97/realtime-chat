package com.example.chat.realtime;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Component;

/**
 * 이 인스턴스에 붙어 있는 연결 목록 (로컬 메모리). 인스턴스 간 공유 상태는 Redis(presence) 와 DB(원장)에만 둔다.
 */
@Component
public class ConnectionRegistry {

    private final Map<String, ClientConnection> byUser = new ConcurrentHashMap<>();
    private final Map<UUID, Set<ClientConnection>> bySession = new ConcurrentHashMap<>();

    /** @return 같은 (세션, 사용자)의 이전 연결 (있으면 호출자가 닫는다) */
    public ClientConnection register(ClientConnection c) {
        bySession.computeIfAbsent(c.sessionId(), k -> ConcurrentHashMap.newKeySet()).add(c);
        return byUser.put(key(c.sessionId(), c.userId()), c);
    }

    public void unregister(ClientConnection c) {
        byUser.remove(key(c.sessionId(), c.userId()), c);
        bySession.computeIfPresent(c.sessionId(), (k, set) -> {
            set.remove(c);
            return set.isEmpty() ? null : set;
        });
    }

    public Collection<ClientConnection> forSession(UUID sessionId) {
        Set<ClientConnection> set = bySession.get(sessionId);
        return set == null ? List.of() : List.copyOf(set);
    }

    public ClientConnection find(UUID sessionId, String userId) {
        return byUser.get(key(sessionId, userId));
    }

    public Collection<ClientConnection> all() {
        return List.copyOf(byUser.values());
    }

    private static String key(UUID sessionId, String userId) {
        return sessionId + "|" + userId;
    }
}
