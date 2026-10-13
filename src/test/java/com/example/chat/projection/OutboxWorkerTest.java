package com.example.chat.projection;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import com.example.chat.config.ChatMetrics;
import com.example.chat.config.ChatProperties;
import com.example.chat.config.InstanceId;
import com.example.chat.store.OutboxRepository;
import com.example.chat.store.OutboxRepository.OutboxItem;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OutboxWorkerTest {

    private static final UUID S1 = UUID.fromString("00000000-0000-0000-0000-000000000001");
    private static final UUID S2 = UUID.fromString("00000000-0000-0000-0000-000000000002");

    private final ChatProperties props = new ChatProperties("test",
            new ChatProperties.Snapshot(100),
            new ChatProperties.Outbox(200, 500, 5, 500, 60_000, 30, 2),
            new ChatProperties.Presence(30, 10_000, 15, 5_000),
            new ChatProperties.Chaos(false));
    private final OutboxRepository outbox = mock(OutboxRepository.class);
    private final MessageProjector projector = mock(MessageProjector.class);
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final OutboxWorker worker = new OutboxWorker(outbox, projector, props, new InstanceId(props),
            new ChatMetrics(registry));

    @AfterEach
    void tearDown() {
        worker.shutdown();
    }

    @Test
    void backoffGrowsExponentiallyWithJitterAndIsCapped() {
        for (int i = 0; i < 50; i++) {
            assertThat(worker.backoffMillis(1)).isBetween(400L, 600L);       // 500 ±20%
            assertThat(worker.backoffMillis(2)).isBetween(800L, 1_200L);     // 1000 ±20%
            assertThat(worker.backoffMillis(4)).isBetween(3_200L, 4_800L);   // 4000 ±20%
            assertThat(worker.backoffMillis(30)).isBetween(48_000L, 72_000L); // 상한 60s ±20%
        }
    }

    @Test
    void successfulCatchUpDeletesClaimedAndAllCoveredItemsPerSession() {
        when(outbox.claim(eq("test"), anyInt(), anyInt())).thenReturn(List.of(
                new OutboxItem(1, S1, 10, 0), new OutboxItem(2, S1, 11, 0), new OutboxItem(3, S2, 4, 0)));
        when(projector.catchUp(S1)).thenReturn(15L);
        when(projector.catchUp(S2)).thenReturn(4L);

        worker.poll();

        verify(projector).catchUp(S1); // 세션당 한 번만 따라잡는다 (outbox 항목 수와 무관)
        verify(outbox).delete(List.of(1L, 2L));
        verify(outbox).deleteCovered(S1, 15L);
        verify(outbox).delete(List.of(3L));
        verify(outbox).deleteCovered(S2, 4L);
        verify(outbox, never()).scheduleRetry(anyLong(), anyInt(), any(), anyString());
    }

    @Test
    void failureSchedulesRetryWithBackoffAndOtherSessionsAreUnaffected() {
        when(outbox.claim(eq("test"), anyInt(), anyInt())).thenReturn(List.of(
                new OutboxItem(1, S1, 10, 2), new OutboxItem(2, S2, 3, 0)));
        when(projector.catchUp(S1)).thenThrow(new IllegalStateException("boom"));
        when(projector.catchUp(S2)).thenReturn(3L);
        Instant before = Instant.now();

        worker.poll();

        ArgumentCaptor<Instant> next = ArgumentCaptor.forClass(Instant.class);
        verify(outbox).scheduleRetry(eq(1L), eq(3), next.capture(), contains("boom"));
        // attempt 3 → 2000ms ±20%
        assertThat(next.getValue()).isBetween(before.plusMillis(1_600), Instant.now().plusMillis(2_400));
        verify(outbox).deleteCovered(S2, 3L);
        assertThat(registry.find("chat.projection.failures").counter().count()).isEqualTo(1);
    }

    @Test
    void lastAttemptMovesItemToDeadLetterQueue() {
        OutboxItem poisoned = new OutboxItem(7, S1, 10, 4); // max-attempts=5
        when(outbox.claim(eq("test"), anyInt(), anyInt())).thenReturn(List.of(poisoned));
        when(projector.catchUp(S1)).thenThrow(new IllegalStateException("poison"));

        worker.poll();

        verify(outbox).moveToDeadLetter(eq(poisoned), eq(5), contains("poison"));
        verify(outbox, never()).scheduleRetry(anyLong(), anyInt(), any(), anyString());
        assertThat(registry.find("chat.outbox.dead.lettered").counter().count()).isEqualTo(1);
    }

    @Test
    void claimFailureIsSwallowedUntilNextPoll() {
        when(outbox.claim(anyString(), anyInt(), anyInt())).thenThrow(new RuntimeException("db down"));

        worker.poll();

        verify(projector, never()).catchUp(any());
    }
}
