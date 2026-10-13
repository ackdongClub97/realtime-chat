package com.example.chat.ingest;

import com.example.chat.domain.StoredEvent;

/**
 * 트랜잭션 커밋 후 실시간 전송 계층으로 넘기는 애플리케이션 이벤트. 커밋되지 않은 이벤트는 절대 전송되지 않는다.
 */
public record EventPublishedEvent(StoredEvent event) {
}
