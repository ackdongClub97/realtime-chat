package com.example.chat.restore;

/**
 * @param snapshotSeq    복원 시작점으로 사용한 스냅샷 seq (없으면 0 = 처음부터 리플레이)
 * @param replayedEvents 스냅샷 이후 리플레이한 이벤트 수
 */
public record RestoreResult(SessionState state, long snapshotSeq, int replayedEvents, long durationMicros) {
}
