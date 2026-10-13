package com.example.chat.restore;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import com.example.chat.domain.ParticipantState;
import com.example.chat.domain.SessionStatus;
import com.example.chat.domain.StoredEvent;

/**
 * 순수 함수형 reducer: (state, event) -> state'.
 * <p>
 * 결정성(Determinism) 보장 규칙
 * <ol>
 *   <li>적용 순서는 오직 서버 발급 seq. 시각(client_ts, server_received_at)은 정렬에 쓰지 않는다.</li>
 *   <li>seq &lt;= state.lastSeq 인 이벤트는 이미 반영된 것으로 보고 무시한다 → 중복 입력에 멱등.</li>
 *   <li>seq 에 구멍이 있으면 예외 → 이벤트 누락 상태로 "그럴듯한" 결과를 만들지 않는다.</li>
 *   <li>규칙 위반 이벤트(대상 없는 수정, 삭제 후 수정 등)는 항상 같은 방식으로 무시하고 ignoredEvents 를 센다.</li>
 *   <li>외부 상태(현재 시각, DB, 랜덤)를 참조하지 않는다.</li>
 * </ol>
 */
public final class SessionReducer {

    private SessionReducer() {
    }

    /** 이벤트 묶음을 seq 순으로 정렬해 적용한다. 순서가 섞이거나 중복이 있어도 결과는 같다. */
    public static SessionState replay(SessionState state, Collection<StoredEvent> events) {
        List<StoredEvent> sorted = events.stream().sorted(Comparator.comparingLong(StoredEvent::seq)).toList();
        for (StoredEvent e : sorted) {
            apply(state, e);
        }
        return state;
    }

    /**
     * @return 이벤트가 새로 적용되었으면 true, 이미 반영된(중복) 이벤트면 false
     */
    public static boolean apply(SessionState s, StoredEvent e) {
        if (e.seq() <= s.lastSeq) {
            return false;
        }
        if (e.seq() != s.lastSeq + 1) {
            throw new IllegalStateException("event gap detected: session=%s expected seq=%d but got %d"
                    .formatted(s.sessionId, s.lastSeq + 1, e.seq()));
        }

        boolean applied = switch (e.type()) {
            case SESSION_CREATED -> created(s, e);
            case JOIN, LEAVE, DISCONNECT, RECONNECT -> participant(s, e);
            case MESSAGE -> message(s, e);
            case MESSAGE_EDITED -> edited(s, e);
            case MESSAGE_DELETED -> deleted(s, e);
            case READ -> read(s, e);
            case SESSION_ENDED -> ended(s, e);
        };
        if (!applied) {
            s.ignoredEvents++;
        }

        s.lastSeq = e.seq();
        s.lastEventAt = e.serverReceivedAt();
        if (s.status != null) {
            s.status = SessionStatus.derive(s.status == SessionStatus.ENDED,
                    s.participants.values().stream().map(p -> p.state).toList());
        }
        return true;
    }

    private static boolean created(SessionState s, StoredEvent e) {
        s.status = SessionStatus.ACTIVE;
        s.title = e.text("title");
        s.createdBy = e.userId();
        s.createdAt = e.serverReceivedAt();
        return true;
    }

    private static boolean participant(SessionState s, StoredEvent e) {
        SessionState.Participant p = s.participants.get(e.userId());
        Optional<ParticipantState> next = ParticipantState.transition(p == null ? null : p.state, e.type());
        if (next.isEmpty()) {
            return false;
        }
        if (p == null) {
            p = new SessionState.Participant();
            p.userId = e.userId();
            s.participants.put(e.userId(), p);
        }
        switch (e.type()) {
            case JOIN -> {
                p.joinedAt = e.serverReceivedAt();
                p.leftAt = null;
                p.disconnectedAt = null;
            }
            case LEAVE -> {
                p.leftAt = e.serverReceivedAt();
                p.disconnectedAt = null;
            }
            case DISCONNECT -> p.disconnectedAt = e.serverReceivedAt();
            case RECONNECT -> p.disconnectedAt = null;
            default -> throw new IllegalArgumentException(e.type().name());
        }
        p.state = next.get();
        return true;
    }

    private static boolean message(SessionState s, StoredEvent e) {
        if (s.messages.containsKey(e.clientEventId())) {
            return false;
        }
        SessionState.Message m = new SessionState.Message();
        m.messageId = e.clientEventId();
        m.seq = e.seq();
        m.senderId = e.userId();
        m.text = e.text("text");
        m.status = SessionState.MessageStatus.SENT;
        m.createdAt = e.serverReceivedAt();
        s.messages.put(m.messageId, m);
        return true;
    }

    private static boolean edited(SessionState s, StoredEvent e) {
        SessionState.Message m = editableTarget(s, e);
        if (m == null) {
            return false;
        }
        m.text = e.text("text");
        m.status = SessionState.MessageStatus.EDITED;
        m.editedAt = e.serverReceivedAt();
        m.editCount++;
        return true;
    }

    private static boolean deleted(SessionState s, StoredEvent e) {
        SessionState.Message m = editableTarget(s, e);
        if (m == null) {
            return false;
        }
        m.text = null;
        m.status = SessionState.MessageStatus.DELETED;
        m.deletedAt = e.serverReceivedAt();
        return true;
    }

    /** 수정/삭제 대상: 존재하고, 삭제되지 않았고, 작성자 본인의 메시지여야 한다. 삭제는 최종 상태(terminal). */
    private static SessionState.Message editableTarget(SessionState s, StoredEvent e) {
        SessionState.Message m = s.messages.get(e.text("messageId"));
        if (m == null || m.status == SessionState.MessageStatus.DELETED || !m.senderId.equals(e.userId())) {
            return null;
        }
        return m;
    }

    /** 읽음 포인터는 max 로만 전진한다 → 순서/중복에 무관(교환법칙·멱등). */
    private static boolean read(SessionState s, StoredEvent e) {
        SessionState.Participant p = s.participants.get(e.userId());
        Long upTo = e.number("upToSeq");
        if (p == null || upTo == null) {
            return false;
        }
        p.lastReadSeq = Math.max(p.lastReadSeq, Math.min(upTo, e.seq()));
        return true;
    }

    private static boolean ended(SessionState s, StoredEvent e) {
        if (s.status == SessionStatus.ENDED) {
            return false;
        }
        s.status = SessionStatus.ENDED;
        s.endedAt = e.serverReceivedAt();
        return true;
    }
}
