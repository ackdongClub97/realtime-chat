package com.example.chat.config;

import java.util.concurrent.atomic.AtomicLong;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

/**
 * 도메인 메트릭. 장애 대응 문서의 "감지" 단계 지표들이다.
 */
@Component
public class ChatMetrics {

    private final MeterRegistry registry;
    private final Timer appendLatency;
    private final Timer restoreLatency;
    private final DistributionSummary restoreReplayed;
    private final AtomicLong wsConnections = new AtomicLong();
    private final AtomicLong outboxPending = new AtomicLong();
    private final AtomicLong outboxOldestAgeMs = new AtomicLong();
    private final AtomicLong deadLetters = new AtomicLong();

    public ChatMetrics(MeterRegistry registry) {
        this.registry = registry;
        this.appendLatency = Timer.builder("chat.append.latency")
                .description("이벤트 append 트랜잭션 소요 시간").register(registry);
        this.restoreLatency = Timer.builder("chat.restore.latency")
                .description("timeline 복원 소요 시간").register(registry);
        this.restoreReplayed = DistributionSummary.builder("chat.restore.replayed.events")
                .description("복원 1회당 리플레이한 이벤트 수 (스냅샷 효율 지표)").register(registry);
        Gauge.builder("chat.ws.connections", wsConnections, AtomicLong::get).register(registry);
        Gauge.builder("chat.outbox.pending", outboxPending, AtomicLong::get).register(registry);
        Gauge.builder("chat.outbox.oldest.age.seconds", outboxOldestAgeMs, v -> v.get() / 1000.0)
                .description("가장 오래된 미처리 outbox 작업의 나이 = projection 지연").register(registry);
        Gauge.builder("chat.dead.letters", deadLetters, AtomicLong::get).register(registry);
    }

    public Timer appendLatency() {
        return appendLatency;
    }

    public Timer restoreLatency() {
        return restoreLatency;
    }

    public void eventAppended(String type) {
        counter("chat.events.appended", "type", type);
    }

    public void eventDuplicate(String channel) {
        counter("chat.events.duplicate", "channel", channel);
    }

    public void eventRejected(String code) {
        counter("chat.events.rejected", "code", code);
    }

    public void clientSeqGap() {
        counter("chat.client.seq.gap");
    }

    public void deliveryGapFilled(long events) {
        Counter.builder("chat.delivery.gap.filled").register(registry).increment(events);
    }

    public void deliveryReordered() {
        counter("chat.delivery.reordered");
    }

    public void resumeReplayed(long events) {
        Counter.builder("chat.ws.resume.replayed").register(registry).increment(events);
    }

    public void busPublishFailed() {
        counter("chat.bus.publish.failed");
    }

    public void projectionFailed() {
        counter("chat.projection.failures");
    }

    public void projectionApplied(long events) {
        Counter.builder("chat.projection.applied").register(registry).increment(events);
    }

    public void deadLettered() {
        counter("chat.outbox.dead.lettered");
    }

    public void snapshotCreated(String trigger) {
        counter("chat.snapshot.created", "trigger", trigger);
    }

    public void restoreReplayed(long events) {
        restoreReplayed.record(events);
    }

    public void instanceSwept() {
        counter("chat.presence.instance.swept");
    }

    public void wsConnected() {
        wsConnections.incrementAndGet();
    }

    public void wsDisconnected() {
        wsConnections.decrementAndGet();
    }

    public void outboxStats(long pending, double oldestAgeSeconds, long dead) {
        outboxPending.set(pending);
        outboxOldestAgeMs.set((long) (oldestAgeSeconds * 1000));
        deadLetters.set(dead);
    }

    private void counter(String name, String... tags) {
        Counter.builder(name).tags(tags).register(registry).increment();
    }
}
