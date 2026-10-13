package com.example.chat.api;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

import com.example.chat.api.dto.Dtos.AppendResponse;
import com.example.chat.api.dto.Dtos.CreateSessionRequest;
import com.example.chat.api.dto.Dtos.EventPage;
import com.example.chat.api.dto.Dtos.EventView;
import com.example.chat.api.dto.Dtos.IngestEvent;
import com.example.chat.api.dto.Dtos.IngestRequest;
import com.example.chat.api.dto.Dtos.IngestResponse;
import com.example.chat.api.dto.Dtos.IngestResult;
import com.example.chat.api.dto.Dtos.MembershipRequest;
import com.example.chat.api.dto.Dtos.MessagePage;
import com.example.chat.api.dto.Dtos.MessageView;
import com.example.chat.api.dto.Dtos.RestoreMeta;
import com.example.chat.api.dto.Dtos.SessionPage;
import com.example.chat.api.dto.Dtos.SessionResponse;
import com.example.chat.api.dto.Dtos.SnapshotResponse;
import com.example.chat.api.dto.Dtos.TimelineParticipant;
import com.example.chat.api.dto.Dtos.TimelineResponse;
import com.example.chat.common.ApiException;
import com.example.chat.domain.EventType;
import com.example.chat.domain.NewEvent;
import com.example.chat.domain.SessionStatus;
import com.example.chat.domain.StoredEvent;
import com.example.chat.ingest.AppendResult;
import com.example.chat.ingest.EventAppendService;
import com.example.chat.restore.RestoreResult;
import com.example.chat.restore.RestoreService;
import com.example.chat.restore.SessionState;
import com.example.chat.restore.SnapshotService;
import com.example.chat.store.EventRepository;
import com.example.chat.store.ProjectionRepository;
import com.example.chat.store.SessionRow;
import com.example.chat.store.SnapshotRepository;
import com.example.chat.store.SnapshotRepository.SnapshotInfo;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.dao.DataAccessException;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.transaction.TransactionException;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/sessions")
@Tag(name = "sessions")
public class SessionController {

    private static final String USER = "X-User-Id";

    private final SessionService sessions;
    private final EventAppendService appender;
    private final EventRepository events;
    private final ProjectionRepository projections;
    private final RestoreService restore;
    private final SnapshotService snapshotService;
    private final SnapshotRepository snapshots;

    public SessionController(SessionService sessions, EventAppendService appender, EventRepository events,
                             ProjectionRepository projections, RestoreService restore,
                             SnapshotService snapshotService, SnapshotRepository snapshots) {
        this.sessions = sessions;
        this.appender = appender;
        this.events = events;
        this.projections = projections;
        this.restore = restore;
        this.snapshotService = snapshotService;
        this.snapshots = snapshots;
    }

    @PostMapping
    @Operation(summary = "세션 생성", description = "Idempotency-Key 헤더가 같으면 기존 세션을 200 으로 반환")
    public ResponseEntity<SessionResponse> create(@RequestHeader(USER) String userId,
                                                  @RequestHeader(value = "Idempotency-Key", required = false) String key,
                                                  @Valid @RequestBody(required = false) CreateSessionRequest body) {
        SessionService.Created c = sessions.create(userId, body == null ? null : body.title(), key);
        return ResponseEntity.status(c.created() ? HttpStatus.CREATED : HttpStatus.OK)
                .body(sessions.detail(c.session().id()));
    }

    @GetMapping
    @Operation(summary = "세션 목록", description = "기간(createdAt)/상태/참여자 필터, created_at DESC keyset 페이지네이션")
    public SessionPage list(@RequestParam(required = false) SessionStatus status,
                            @RequestParam(required = false) String participant,
                            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant from,
                            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant to,
                            @RequestParam(required = false) String cursor,
                            @RequestParam(defaultValue = "20") int limit) {
        return sessions.search(status, participant, from, to, cursor, limit);
    }

    @GetMapping("/{id}")
    @Operation(summary = "세션 상세 (참여자 + presence)")
    public SessionResponse get(@PathVariable UUID id) {
        return sessions.detail(id);
    }

    @PostMapping("/{id}/join")
    @Operation(summary = "참여", description = "이미 참여 중이면 NOOP (멱등). 1:1 세션이므로 최대 2명")
    public AppendResponse join(@PathVariable UUID id, @RequestHeader(USER) String userId,
                               @RequestBody(required = false) MembershipRequest body) {
        return membership(id, userId, EventType.JOIN, body);
    }

    @PostMapping("/{id}/leave")
    @Operation(summary = "퇴장", description = "참여 중이 아니면 NOOP (멱등)")
    public AppendResponse leave(@PathVariable UUID id, @RequestHeader(USER) String userId,
                                @RequestBody(required = false) MembershipRequest body) {
        return membership(id, userId, EventType.LEAVE, body);
    }

    @PostMapping("/{id}/end")
    @Operation(summary = "세션 종료", description = "종료 후에는 모든 이벤트 거절(409 SESSION_ENDED). 종료 시 최종 스냅샷 자동 생성")
    public AppendResponse end(@PathVariable UUID id, @RequestHeader(USER) String userId,
                              @RequestBody(required = false) MembershipRequest body) {
        SessionRow row = sessions.require(id);
        if (row.status() == SessionStatus.ENDED) {
            return new AppendResponse(null, AppendResult.Outcome.NOOP.name(), null, row.lastSeq());
        }
        return membership(id, userId, EventType.SESSION_ENDED, body);
    }

    @PostMapping("/{id}/events")
    @Operation(summary = "이벤트/메시지 수집 (배치)",
            description = "이벤트마다 독립 처리(부분 성공). clientEventId 중복은 DUPLICATE 로 기존 seq 를 반환")
    public IngestResponse ingest(@PathVariable UUID id, @RequestHeader(USER) String userId,
                                 @Valid @RequestBody IngestRequest body) {
        sessions.require(id);
        List<IngestResult> results = new ArrayList<>();
        // 같은 요청 안에서는 클라이언트 순번(clientSeq) 순서로 처리해 송신자 의도 순서를 최대한 보존
        List<IngestEvent> ordered = body.events().stream()
                .sorted(Comparator.comparing(e -> e.clientSeq() == null ? Long.MAX_VALUE : e.clientSeq()))
                .toList();
        for (IngestEvent e : ordered) {
            if (!EventType.CLIENT_SENDABLE.contains(e.type())) {
                results.add(new IngestResult(e.clientEventId(), "REJECTED", null, "INVALID_EVENT_TYPE",
                        "use dedicated endpoints for " + e.type(), false));
                continue;
            }
            try {
                AppendResult r = appender.append(id, new NewEvent(e.clientEventId(), e.type(), userId, e.payload(),
                        e.clientSeq(), e.clientTs()), "rest");
                results.add(new IngestResult(e.clientEventId(), r.outcome().name(), r.seq(), null, null, null));
            } catch (ApiException ex) {
                results.add(new IngestResult(e.clientEventId(), "REJECTED", null, ex.code(), ex.getMessage(),
                        "TARGET_NOT_FOUND".equals(ex.code())));
            } catch (DataAccessException | TransactionException ex) {
                results.add(new IngestResult(e.clientEventId(), "REJECTED", null, "DB_UNAVAILABLE",
                        "retry with the same clientEventId", true));
            }
        }
        return new IngestResponse(results);
    }

    @GetMapping("/{id}/events")
    @Operation(summary = "이벤트 원장 조회 (디버깅/검증)",
            description = "fromSeq/toSeq(seq 범위, 기본) 또는 from/to(서버 수신 시각 범위)")
    public EventPage events(@PathVariable UUID id,
                            @RequestParam(required = false) Long fromSeq,
                            @RequestParam(required = false) Long toSeq,
                            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant from,
                            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant to,
                            @RequestParam(defaultValue = "100") int limit) {
        sessions.require(id);
        int size = Math.clamp(limit, 1, 1000);
        List<StoredEvent> list;
        if (from != null || to != null) {
            list = events.findByTimeRange(id, from == null ? Instant.EPOCH : from, to == null ? Instant.now() : to,
                    size + 1);
        } else {
            long after = fromSeq == null ? 0 : Math.max(0, fromSeq - 1);
            list = events.findRange(id, after, toSeq == null ? Long.MAX_VALUE : toSeq, size + 1);
        }
        boolean hasMore = list.size() > size;
        List<StoredEvent> page = hasMore ? list.subList(0, size) : list;
        return new EventPage(page.stream().map(EventView::of).toList(),
                hasMore && from == null && to == null ? page.getLast().seq() + 1 : null);
    }

    @GetMapping("/{id}/messages")
    @Operation(summary = "대화창 메시지 페이지 (read model)",
            description = "비동기 projection 기반 최신 상태. projectedSeq < sessionLastSeq 이면 아직 반영 중")
    public MessagePage messages(@PathVariable UUID id,
                                @RequestParam(required = false) Long beforeSeq,
                                @RequestParam(defaultValue = "50") int limit) {
        SessionRow row = sessions.require(id);
        int size = Math.clamp(limit, 1, 200);
        var rows = projections.page(id, beforeSeq == null ? Long.MAX_VALUE : beforeSeq, size + 1);
        boolean hasMore = rows.size() > size;
        var page = hasMore ? rows.subList(0, size) : rows;
        List<MessageView> items = page.stream()
                .map(m -> new MessageView(m.messageId(), m.seq(), m.senderId(), m.content(), m.status(),
                        m.createdAt(), m.updatedAt()))
                .toList();
        return new MessagePage(items, hasMore ? page.getLast().seq() : null,
                projections.appliedSeq(id, ProjectionRepository.MESSAGES), row.lastSeq());
    }

    @GetMapping("/{id}/timeline")
    @Operation(summary = "특정 시점 상태 복원",
            description = "at(ISO-8601) 시각까지 서버가 수신한 이벤트로 상태를 복원. 스냅샷 + 리플레이. seq 지정 시 seq 기준")
    public TimelineResponse timeline(@PathVariable UUID id,
                                     @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant at,
                                     @RequestParam(required = false) Long seq,
                                     @Parameter(description = "반환할 최근 메시지 수") @RequestParam(defaultValue = "50") int limit,
                                     @Parameter(description = "스냅샷 없이 전체 리플레이한 결과와 비교 검증")
                                     @RequestParam(defaultValue = "false") boolean verify) {
        RestoreResult r = restore.restore(id, at, seq);
        Boolean verified = null;
        if (verify) {
            verified = restore.sameState(r.state(), restore.fullReplay(id, r.state().lastSeq));
        }
        return toTimeline(r, at, seq, Math.clamp(limit, 1, 1000), verified);
    }

    @PostMapping("/{id}/snapshots")
    @Operation(summary = "스냅샷 생성 (현재 시점)", description = "결정적 상태이므로 같은 seq 재요청은 created=false")
    public SnapshotResponse snapshot(@PathVariable UUID id) {
        var c = snapshotService.createLatest(id);
        return new SnapshotResponse(c.seq(), c.created(), c.replayedEvents());
    }

    @GetMapping("/{id}/snapshots")
    @Operation(summary = "스냅샷 목록")
    public List<SnapshotInfo> snapshots(@PathVariable UUID id) {
        sessions.require(id);
        return snapshots.list(id);
    }

    private AppendResponse membership(UUID id, String userId, EventType type, MembershipRequest body) {
        String clientEventId = body != null && body.clientEventId() != null
                ? body.clientEventId() : type.name().toLowerCase() + ":" + UUID.randomUUID();
        AppendResult r = appender.append(id, new NewEvent(clientEventId, type, userId, null, null, null), "rest");
        return new AppendResponse(clientEventId, r.outcome().name(), r.seq(), r.sessionLastSeq());
    }

    private TimelineResponse toTimeline(RestoreResult r, Instant at, Long seq, int limit, Boolean verified) {
        SessionState s = r.state();
        List<SessionState.Message> all = List.copyOf(s.messages.values());
        List<SessionState.Message> recent = all.subList(Math.max(0, all.size() - limit), all.size());
        List<TimelineParticipant> participants = s.participants.values().stream()
                .map(p -> new TimelineParticipant(p.userId, p.state, p.joinedAt, p.leftAt, p.disconnectedAt,
                        p.lastReadSeq, unread(all, p)))
                .toList();
        return new TimelineResponse(s.sessionId, at, seq, s.lastSeq, s.lastEventAt, s.status, s.title, s.createdBy,
                s.createdAt, s.endedAt, participants, recent, all.size(), s.ignoredEvents,
                new RestoreMeta(r.snapshotSeq(), r.replayedEvents(), r.durationMicros(), verified));
    }

    /** 상대방이 보낸, 삭제되지 않은, 읽음 포인터 이후의 메시지 수 */
    private static long unread(List<SessionState.Message> messages, SessionState.Participant p) {
        return messages.stream()
                .filter(m -> !m.senderId.equals(p.userId))
                .filter(m -> m.status != SessionState.MessageStatus.DELETED)
                .filter(m -> m.seq > p.lastReadSeq)
                .count();
    }
}
