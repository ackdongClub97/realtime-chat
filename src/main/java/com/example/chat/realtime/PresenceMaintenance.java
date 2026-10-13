package com.example.chat.realtime;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import com.example.chat.common.ApiException;
import com.example.chat.common.Json;
import com.example.chat.config.ChatMetrics;
import com.example.chat.config.InstanceId;
import com.example.chat.domain.EventType;
import com.example.chat.domain.NewEvent;
import com.example.chat.ingest.EventAppendService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * presence 유지 작업.
 * <ul>
 *   <li>instanceHeartbeat: 인스턴스 생존 키 갱신 (3s 주기, TTL 15s)</li>
 *   <li>refreshConnections: 로컬 연결 presence TTL 갱신</li>
 *   <li>sweep: 하트비트가 끊긴(죽은) 인스턴스의 연결을 DISCONNECT 처리 — 서버 다운 시 상대방이 "끊김"을 알 수 있게 한다.</li>
 * </ul>
 * sweep 이 기록하는 DISCONNECT 의 clientEventId 는 정상 종료 경로와 같은 "sys:disconnect:{connId}" 이므로,
 * 여러 인스턴스가 동시에 sweep 하거나 정상 종료 처리와 겹쳐도 원장에는 한 번만 저장된다.
 */
@Component
public class PresenceMaintenance {

    private static final Logger log = LoggerFactory.getLogger(PresenceMaintenance.class);

    private final PresenceService presence;
    private final ConnectionRegistry registry;
    private final EventAppendService appender;
    private final RealtimeService realtime;
    private final InstanceId instanceId;
    private final ChatMetrics metrics;
    private final Json json;
    private final Set<String> suspects = new HashSet<>();

    public PresenceMaintenance(PresenceService presence, ConnectionRegistry registry, EventAppendService appender,
                               RealtimeService realtime, InstanceId instanceId, ChatMetrics metrics, Json json) {
        this.presence = presence;
        this.registry = registry;
        this.appender = appender;
        this.realtime = realtime;
        this.instanceId = instanceId;
        this.metrics = metrics;
        this.json = json;
    }

    /** 인스턴스 생존 신호: TTL(15s) 대비 짧은 주기(3s)로 갱신해 일시적 정체로 오판되지 않게 한다. */
    @Scheduled(fixedRateString = "${chat.presence.instance-heartbeat-ms:3000}", initialDelay = 0)
    public void instanceHeartbeat() {
        try {
            presence.heartbeat();
        } catch (Exception e) {
            log.warn("instance heartbeat failed (redis unavailable?): {}", e.getMessage());
        }
    }

    @Scheduled(fixedDelayString = "${chat.presence.refresh-interval-ms:10000}", initialDelay = 0)
    public void refreshConnections() {
        try {
            for (ClientConnection c : registry.all()) {
                if (c.isOpen() && !presence.refresh(c)) {
                    // TTL 만료 등으로 키를 잃었으면 다시 소유 (Redis 재시작 후 복구 경로)
                    presence.markOnline(c);
                }
            }
        } catch (Exception e) {
            log.warn("presence heartbeat failed (redis unavailable?): {}", e.getMessage());
        }
    }

    @Scheduled(fixedDelayString = "${chat.presence.sweep-interval-ms:5000}", initialDelay = 5000)
    public void sweepDeadInstances() {
        try {
            Set<String> missing = new HashSet<>();
            for (String instance : presence.knownInstances()) {
                if (instance.equals(instanceId.value()) || presence.isAlive(instance)) {
                    continue;
                }
                missing.add(instance);
                // 연속 2회(sweep 주기 이상) 하트비트가 없을 때만 죽은 것으로 판정 (오탐 방지)
                if (!suspects.contains(instance) || !presence.tryLockSweep(instance)) {
                    continue;
                }
                int n = 0;
                for (String member : presence.connectionsOf(instance)) {
                    n += sweepConnection(instance, member) ? 1 : 0;
                }
                presence.forgetInstance(instance);
                metrics.instanceSwept();
                log.warn("swept dead instance={} disconnectedConnections={}", instance, n);
            }
            suspects.retainAll(missing);
            suspects.addAll(missing);
        } catch (Exception e) {
            log.warn("sweep failed: {}", e.getMessage());
        }
    }

    private boolean sweepConnection(String instance, String member) {
        String[] parts = member.split("\\|");
        UUID sessionId = UUID.fromString(parts[0]);
        String userId = parts[1];
        String connId = parts[2];

        String owner = presence.ownerOf(sessionId, userId);
        if (owner != null && !owner.equals(instance + "|" + connId)) {
            return false; // 이미 다른 연결로 재접속함
        }
        presence.releaseStale(sessionId, userId, instance, connId);
        try {
            appender.append(sessionId, NewEvent.system("sys:disconnect:" + connId, EventType.DISCONNECT, userId,
                    json.object().put("reason", "instance_down").put("instance", instance).put("connId", connId)),
                    "system");
            realtime.publishPresence(sessionId, userId, "OFFLINE");
            return true;
        } catch (ApiException e) {
            log.debug("sweep disconnect skipped session={} user={}: {}", sessionId, userId, e.code());
            return false;
        }
    }
}
