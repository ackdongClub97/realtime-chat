package com.example.chat.ingest;

import com.example.chat.domain.StoredEvent;

/**
 * @param outcome  ACCEPTED: 새로 저장됨 / DUPLICATE: 같은 clientEventId 가 이미 저장됨(기존 결과 반환) /
 *                 NOOP: 상태 전이가 없어 저장하지 않음(이미 JOINED 인데 JOIN 등)
 * @param event    ACCEPTED, DUPLICATE 일 때 저장된 이벤트
 */
public record AppendResult(Outcome outcome, StoredEvent event, long sessionLastSeq) {

    public enum Outcome {
        ACCEPTED, DUPLICATE, NOOP
    }

    public static AppendResult accepted(StoredEvent e) {
        return new AppendResult(Outcome.ACCEPTED, e, e.seq());
    }

    public static AppendResult duplicate(StoredEvent e) {
        return new AppendResult(Outcome.DUPLICATE, e, e.seq());
    }

    public static AppendResult noop(long lastSeq) {
        return new AppendResult(Outcome.NOOP, null, lastSeq);
    }

    public Long seq() {
        return event == null ? null : event.seq();
    }
}
