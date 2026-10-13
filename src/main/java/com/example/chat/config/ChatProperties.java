package com.example.chat.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties("chat")
public record ChatProperties(
        String instanceId,
        @DefaultValue Snapshot snapshot,
        @DefaultValue Outbox outbox,
        @DefaultValue Presence presence,
        @DefaultValue Chaos chaos
) {

    public record Snapshot(@DefaultValue("100") int interval) {
    }

    public record Outbox(
            @DefaultValue("200") long pollIntervalMs,
            @DefaultValue("500") int batchSize,
            @DefaultValue("5") int maxAttempts,
            @DefaultValue("500") long baseBackoffMs,
            @DefaultValue("60000") long maxBackoffMs,
            @DefaultValue("30") int leaseSeconds,
            @DefaultValue("4") int workerThreads
    ) {
    }

    public record Presence(
            @DefaultValue("30") int ttlSeconds,
            @DefaultValue("10000") long refreshIntervalMs,
            @DefaultValue("15") int instanceTtlSeconds,
            @DefaultValue("5000") long sweepIntervalMs
    ) {
    }

    public record Chaos(@DefaultValue("false") boolean enabled) {
    }
}
