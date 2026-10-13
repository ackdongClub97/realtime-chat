package com.example.chat.realtime;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.example.chat.config.ChatProperties;
import com.example.chat.config.InstanceId;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

/**
 * Redis 기반 presence.
 * <pre>
 *   presence:{sessionId}:{userId} = "{instanceId}|{connId}"  (TTL 30s, 소유 연결이 주기적으로 갱신)
 *   instance:{instanceId}          = "alive"                    (TTL 15s, 인스턴스 하트비트)
 *   instance:{instanceId}:conns    = SET of "{sessionId}|{userId}|{connId}"
 *   instances                      = SET of instanceId
 * </pre>
 * 인스턴스가 죽으면 하트비트 키가 만료되고, 살아있는 인스턴스의 sweeper 가 그 인스턴스의 연결들을 DISCONNECT 처리한다.
 * 키 값에 connId 를 넣어 "소유자만 갱신/삭제" (compare-and-set) 하므로 재연결과 경합해도 새 연결의 presence 를 지우지 않는다.
 */
@Component
public class PresenceService {

    private static final String INSTANCES = "instances";

    private static final DefaultRedisScript<Long> RELEASE_IF_OWNER = new DefaultRedisScript<>(
            "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) else return 0 end",
            Long.class);
    private static final DefaultRedisScript<Long> REFRESH_IF_OWNER = new DefaultRedisScript<>(
            "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('EXPIRE', KEYS[1], ARGV[2]) else return 0 end",
            Long.class);

    private final StringRedisTemplate redis;
    private final InstanceId instanceId;
    private final ChatProperties.Presence props;

    public PresenceService(StringRedisTemplate redis, InstanceId instanceId, ChatProperties props) {
        this.redis = redis;
        this.instanceId = instanceId;
        this.props = props.presence();
    }

    public void markOnline(ClientConnection c) {
        redis.opsForValue().set(presenceKey(c.sessionId(), c.userId()), owner(c), Duration.ofSeconds(props.ttlSeconds()));
        redis.opsForSet().add(connsKey(instanceId.value()), c.presenceKeyMember());
    }

    /** @return 이 연결이 presence 의 소유자였으면 true (→ DISCONNECT 를 기록해야 함) */
    public boolean release(ClientConnection c) {
        redis.opsForSet().remove(connsKey(instanceId.value()), c.presenceKeyMember());
        Long r = redis.execute(RELEASE_IF_OWNER, List.of(presenceKey(c.sessionId(), c.userId())), owner(c));
        return r != null && r > 0;
    }

    public boolean refresh(ClientConnection c) {
        Long r = redis.execute(REFRESH_IF_OWNER, List.of(presenceKey(c.sessionId(), c.userId())), owner(c),
                String.valueOf(props.ttlSeconds()));
        return r != null && r > 0;
    }

    public boolean isOnline(UUID sessionId, String userId) {
        return Boolean.TRUE.equals(redis.hasKey(presenceKey(sessionId, userId)));
    }

    public String ownerOf(UUID sessionId, String userId) {
        return redis.opsForValue().get(presenceKey(sessionId, userId));
    }

    public void heartbeat() {
        redis.opsForValue().set("instance:" + instanceId.value(), "alive",
                Duration.ofSeconds(props.instanceTtlSeconds()));
        redis.opsForSet().add(INSTANCES, instanceId.value());
    }

    public Set<String> knownInstances() {
        Set<String> s = redis.opsForSet().members(INSTANCES);
        return s == null ? Set.of() : s;
    }

    public boolean isAlive(String instance) {
        return Boolean.TRUE.equals(redis.hasKey("instance:" + instance));
    }

    /** 여러 인스턴스가 동시에 같은 죽은 인스턴스를 정리하지 않도록 짧은 락 */
    public boolean tryLockSweep(String deadInstance) {
        return Boolean.TRUE.equals(redis.opsForValue()
                .setIfAbsent("sweep-lock:" + deadInstance, instanceId.value(), Duration.ofSeconds(30)));
    }

    public Set<String> connectionsOf(String instance) {
        Set<String> s = redis.opsForSet().members(connsKey(instance));
        return s == null ? Set.of() : s;
    }

    public void forgetInstance(String instance) {
        redis.delete(connsKey(instance));
        redis.opsForSet().remove(INSTANCES, instance);
    }

    /** 죽은 인스턴스가 소유하던 presence 키만 삭제한다. */
    public void releaseStale(UUID sessionId, String userId, String instance, String connId) {
        redis.execute(RELEASE_IF_OWNER, List.of(presenceKey(sessionId, userId)), instance + "|" + connId);
    }

    private String owner(ClientConnection c) {
        return instanceId.value() + "|" + c.connId();
    }

    private static String presenceKey(UUID sessionId, String userId) {
        return "presence:" + sessionId + ":" + userId;
    }

    private static String connsKey(String instance) {
        return "instance:" + instance + ":conns";
    }
}
