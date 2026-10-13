package com.example.chat.realtime;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Component;

/**
 * 세션 단위 순서를 보존하는 전송 스레드풀. 같은 세션의 작업은 항상 같은 단일 스레드 레인에서 순서대로 실행된다.
 * <p>
 * 일반 스레드풀로 팬아웃하면 같은 세션의 seq 10, 11 이 다른 스레드에서 역순으로 실행되어
 * 매번 DB gap-fill 이 일어난다(측정: 커넥션 풀 대기 급증, e2e p95 2.3s). 레인 고정으로 이를 없앤다.
 */
@Component
public class SessionStripedExecutor {

    private static final int LANES = 16;

    private final ExecutorService[] lanes = new ExecutorService[LANES];

    public SessionStripedExecutor() {
        for (int i = 0; i < LANES; i++) {
            int lane = i;
            lanes[i] = Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, "fanout-" + lane);
                t.setDaemon(true);
                return t;
            });
        }
    }

    public void execute(UUID sessionId, Runnable task) {
        lanes[Math.floorMod(sessionId.hashCode(), LANES)].execute(task);
    }

    @PreDestroy
    void shutdown() {
        for (ExecutorService lane : lanes) {
            lane.shutdown();
        }
    }
}
