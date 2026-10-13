package com.example.chat.admin;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.example.chat.common.ApiException;
import com.example.chat.config.ChatProperties;
import com.example.chat.store.OutboxRepository;
import com.example.chat.store.OutboxRepository.DeadLetter;
import com.example.chat.store.ProjectionRepository;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 운영용 엔드포인트 (운영 환경에서는 내부망/관리자 인증 뒤에 둔다).
 */
@RestController
@RequestMapping("/admin")
@Tag(name = "admin")
public class AdminController {

    private final OutboxRepository outbox;
    private final ProjectionRepository projections;
    private final ChaosSettings chaos;
    private final ChatProperties props;

    public AdminController(OutboxRepository outbox, ProjectionRepository projections, ChaosSettings chaos,
                           ChatProperties props) {
        this.outbox = outbox;
        this.projections = projections;
        this.chaos = chaos;
        this.props = props;
    }

    @GetMapping("/dlq")
    @Operation(summary = "DLQ 조회 (미처리 건)")
    public List<DeadLetter> deadLetters() {
        return outbox.openDeadLetters(100);
    }

    @PostMapping("/dlq/requeue")
    @Operation(summary = "DLQ 전체 재처리", description = "원인 수정 후 호출. outbox 로 되돌려 재시도 횟수 초기화")
    public Map<String, Integer> requeue() {
        return Map.of("requeued", outbox.requeueDeadLetters());
    }

    @PostMapping("/projections/{sessionId}/rebuild")
    @Transactional
    @Operation(summary = "세션 projection 재구축", description = "read model 삭제 후 원장에서 처음부터 다시 생성")
    public Map<String, String> rebuild(@PathVariable UUID sessionId) {
        projections.lockOffset(sessionId, ProjectionRepository.MESSAGES);
        projections.reset(sessionId, ProjectionRepository.MESSAGES);
        outbox.enqueue(sessionId, 0);
        return Map.of("status", "scheduled");
    }

    @GetMapping("/chaos")
    public Map<String, Object> chaos() {
        return Map.of("enabled", props.chaos().enabled(),
                "poisonText", String.valueOf(chaos.poisonText()),
                "projectionDelayMs", chaos.projectionDelayMs());
    }

    @PutMapping("/chaos")
    @Operation(summary = "장애 주입 설정", description = "chat.chaos.enabled=true 일 때만. {poisonText, projectionDelayMs}")
    public Map<String, Object> updateChaos(@RequestBody Map<String, Object> body) {
        if (!props.chaos().enabled()) {
            throw ApiException.forbidden("CHAOS_DISABLED", "set CHAOS_ENABLED=true to use fault injection");
        }
        if (body.containsKey("poisonText")) {
            Object v = body.get("poisonText");
            chaos.poisonText(v == null ? null : v.toString());
        }
        if (body.containsKey("projectionDelayMs")) {
            chaos.projectionDelayMs(((Number) body.get("projectionDelayMs")).longValue());
        }
        return chaos();
    }
}
