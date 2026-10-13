package com.example.chat.projection;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import com.example.chat.config.ChatMetrics;
import com.example.chat.config.ChatProperties;
import com.example.chat.config.InstanceId;
import com.example.chat.store.OutboxRepository;
import com.example.chat.store.OutboxRepository.OutboxItem;
import com.example.chat.store.OutboxRepository.OutboxStats;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Transactional Outbox 폴링 워커. 모든 인스턴스에서 동시에 돌아도 안전하다(SKIP LOCKED + 리스 + 세션 체크포인트 락).
 * <p>
 * 실패 처리: 지수 백오프(+jitter)로 재시도, max-attempts 초과 시 dead_letter 로 이동.
 */
@Component
public class OutboxWorker {

    private static final Logger log = LoggerFactory.getLogger(OutboxWorker.class);

    private final OutboxRepository outbox;
    private final MessageProjector projector;
    private final ChatProperties.Outbox props;
    private final InstanceId instanceId;
    private final ChatMetrics metrics;
    private final ExecutorService pool;

    public OutboxWorker(OutboxRepository outbox, MessageProjector projector, ChatProperties props,
                        InstanceId instanceId, ChatMetrics metrics) {
        this.outbox = outbox;
        this.projector = projector;
        this.props = props.outbox();
        this.instanceId = instanceId;
        this.metrics = metrics;
        this.pool = Executors.newFixedThreadPool(this.props.workerThreads(), r -> {
            Thread t = new Thread(r, "outbox-worker");
            t.setDaemon(true);
            return t;
        });
    }

    @Scheduled(fixedDelayString = "${chat.outbox.poll-interval-ms:200}")
    public void poll() {
        List<OutboxItem> items;
        try {
            items = outbox.claim(instanceId.value(), props.batchSize(), props.leaseSeconds());
        } catch (Exception e) {
            log.warn("outbox claim failed (db unavailable?): {}", e.getMessage());
            return;
        }
        if (items.isEmpty()) {
            return;
        }
        Map<UUID, List<OutboxItem>> bySession = items.stream()
                .collect(Collectors.groupingBy(OutboxItem::sessionId, LinkedHashMap::new, Collectors.toList()));
        // 세션끼리는 독립적이므로 병렬 처리. 이번 배치를 모두 끝낸 뒤 다음 claim 을 한다.
        List<Callable<Void>> tasks = bySession.entrySet().stream()
                .<Callable<Void>>map(e -> () -> {
                    process(e.getKey(), e.getValue());
                    return null;
                })
                .toList();
        try {
            pool.invokeAll(tasks);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @PreDestroy
    void shutdown() {
        pool.shutdown();
    }

    private void process(UUID sessionId, List<OutboxItem> items) {
        MDC.put("sessionId", sessionId.toString());
        try {
            long applied = projector.catchUp(sessionId);
            outbox.delete(items.stream().map(OutboxItem::id).toList());
            outbox.deleteCovered(sessionId, applied);
        } catch (Exception e) {
            metrics.projectionFailed();
            String error = e.getClass().getSimpleName() + ": " + e.getMessage();
            for (OutboxItem item : items) {
                handleFailure(item, error);
            }
        } finally {
            MDC.remove("sessionId");
        }
    }

    private void handleFailure(OutboxItem item, String error) {
        int attempts = item.attempts() + 1;
        try {
            if (attempts >= props.maxAttempts()) {
                outbox.moveToDeadLetter(item, attempts, error);
                metrics.deadLettered();
                log.error("outbox item moved to DLQ id={} session={} seq={} attempts={} error={}",
                        item.id(), item.sessionId(), item.eventSeq(), attempts, error);
            } else {
                long backoff = backoffMillis(attempts);
                outbox.scheduleRetry(item.id(), attempts, Instant.now().plusMillis(backoff), error);
                log.warn("outbox item retry scheduled id={} session={} attempt={} backoffMs={} error={}",
                        item.id(), item.sessionId(), attempts, backoff, error);
            }
        } catch (Exception e) {
            // 실패 기록조차 못 하면 리스 만료 후 자동 회수된다.
            log.error("failed to record outbox failure id={}: {}", item.id(), e.getMessage());
        }
    }

    /** base * 2^(attempt-1), 상한 max, ±20% jitter (동시 재시도 폭주 방지). */
    long backoffMillis(int attempt) {
        long exp = props.baseBackoffMs() * (1L << Math.min(attempt - 1, 20));
        long capped = Math.min(exp, props.maxBackoffMs());
        double jitter = 0.8 + ThreadLocalRandom.current().nextDouble() * 0.4;
        return (long) (capped * jitter);
    }

    @Scheduled(fixedDelay = 5000)
    public void refreshStats() {
        try {
            OutboxStats s = outbox.stats();
            metrics.outboxStats(s.pending(), s.oldestAgeSeconds(), s.deadLetters());
        } catch (Exception e) {
            log.debug("outbox stats unavailable: {}", e.getMessage());
        }
    }
}
