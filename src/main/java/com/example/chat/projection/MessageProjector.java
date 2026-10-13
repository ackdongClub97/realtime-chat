package com.example.chat.projection;

import java.util.List;
import java.util.UUID;

import com.example.chat.admin.ChaosSettings;
import com.example.chat.config.ChatMetrics;
import com.example.chat.config.ChatProperties;
import com.example.chat.domain.EventType;
import com.example.chat.domain.StoredEvent;
import com.example.chat.restore.SnapshotService;
import com.example.chat.store.EventRepository;
import com.example.chat.store.ProjectionRepository;
import com.example.chat.store.ProjectionRepository.Offset;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * message_projection 갱신 + 스냅샷 자동 생성.
 * <p>
 * outbox 항목은 "이 세션에 새 이벤트가 있다"는 신호로만 쓰고, 실제 적용 범위는 체크포인트(last_applied_seq) 이후의
 * 원장 이벤트로 결정한다(catch-up). 그래서
 * <ul>
 *   <li>outbox 가 중복 전달되어도 이미 적용한 seq 는 건너뛴다 (멱등)</li>
 *   <li>outbox 처리 순서가 섞여도 항상 seq 순서로 적용된다 (순서 보장)</li>
 *   <li>체크포인트 행 락으로 같은 세션을 두 워커가 동시에 처리하지 않는다 (중복 실행 방지)</li>
 * </ul>
 */
@Component
public class MessageProjector {

    private static final int PAGE_SIZE = 500;

    private final EventRepository events;
    private final ProjectionRepository projections;
    private final SnapshotService snapshots;
    private final ChatProperties props;
    private final ChaosSettings chaos;
    private final ChatMetrics metrics;

    public MessageProjector(EventRepository events, ProjectionRepository projections, SnapshotService snapshots,
                            ChatProperties props, ChaosSettings chaos, ChatMetrics metrics) {
        this.events = events;
        this.projections = projections;
        this.snapshots = snapshots;
        this.props = props;
        this.chaos = chaos;
        this.metrics = metrics;
    }

    /**
     * @return 이 세션에 대해 projection 이 반영을 마친 마지막 seq
     */
    @Transactional
    public long catchUp(UUID sessionId) {
        Offset offset = projections.lockOffset(sessionId, ProjectionRepository.MESSAGES);
        long applied = offset.lastAppliedSeq();
        long lastSnapshot = offset.lastSnapshotSeq();
        boolean ended = false;
        long count = 0;

        while (true) {
            List<StoredEvent> page = events.findRange(sessionId, applied, Long.MAX_VALUE, PAGE_SIZE);
            if (page.isEmpty()) {
                break;
            }
            for (StoredEvent e : page) {
                project(e);
                applied = e.seq();
                ended |= e.type() == EventType.SESSION_ENDED;
                count++;
            }
        }

        if (count > 0) {
            // 스냅샷 자동화: seq 가 N 의 배수가 될 때마다 정확히 그 seq 에서 생성 → 어떤 시점이든 리플레이는 최대 N-1 개.
            // 배치로 여러 경계를 한 번에 지나쳐도 경계마다 생성한다 (이전 스냅샷 + N 개 증분이라 비용 일정).
            int interval = props.snapshot().interval();
            for (long next = (lastSnapshot / interval + 1) * interval; next <= applied; next += interval) {
                snapshots.createAt(sessionId, next, "interval");
                lastSnapshot = next;
            }
            if (ended && lastSnapshot < applied) {
                snapshots.createAt(sessionId, applied, "session_end");
                lastSnapshot = applied;
            }
            projections.saveOffset(sessionId, ProjectionRepository.MESSAGES, applied, lastSnapshot);
            metrics.projectionApplied(count);
        }
        return applied;
    }

    private void project(StoredEvent e) {
        injectFaults(e);
        switch (e.type()) {
            case MESSAGE -> projections.insertMessage(e.sessionId(), e.clientEventId(), e.seq(), e.userId(),
                    e.text("text"), e.serverReceivedAt());
            case MESSAGE_EDITED -> projections.editMessage(e.sessionId(), e.text("messageId"), e.userId(),
                    e.text("text"), e.serverReceivedAt(), e.seq());
            case MESSAGE_DELETED -> projections.deleteMessage(e.sessionId(), e.text("messageId"), e.userId(),
                    e.serverReceivedAt(), e.seq());
            default -> {
                // 참여자/세션 상태는 write model 에서 동기 처리되므로 이 projection 의 대상이 아니다.
            }
        }
    }

    private void injectFaults(StoredEvent e) {
        long delay = chaos.projectionDelayMs();
        if (delay > 0) {
            try {
                Thread.sleep(delay);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }
        String poison = chaos.poisonText();
        String text = e.text("text");
        if (poison != null && text != null && text.contains(poison)) {
            throw new IllegalStateException("chaos: poison message at seq " + e.seq());
        }
    }
}
