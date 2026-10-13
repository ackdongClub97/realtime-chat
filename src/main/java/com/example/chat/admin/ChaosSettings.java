package com.example.chat.admin;

import java.util.concurrent.atomic.AtomicReference;

import org.springframework.stereotype.Component;

/**
 * 장애 주입 설정 (chat.chaos.enabled=true 일 때만 /admin/chaos 로 변경 가능).
 * poisonText 가 포함된 메시지는 projection 단계에서 실패해 재시도 → DLQ 흐름을 재현한다.
 */
@Component
public class ChaosSettings {

    private final AtomicReference<String> poisonText = new AtomicReference<>();
    private volatile long projectionDelayMs;

    public String poisonText() {
        return poisonText.get();
    }

    public void poisonText(String value) {
        poisonText.set(value == null || value.isBlank() ? null : value);
    }

    public long projectionDelayMs() {
        return projectionDelayMs;
    }

    public void projectionDelayMs(long value) {
        this.projectionDelayMs = Math.max(0, value);
    }
}
