package com.example.chat.api;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.example.chat.common.ApiException;
import com.example.chat.common.Json;
import com.example.chat.domain.SessionStatus;
import com.example.chat.realtime.PresenceService;
import com.example.chat.store.EventRepository;
import com.example.chat.store.OutboxRepository;
import com.example.chat.store.ParticipantRepository;
import com.example.chat.store.SessionRepository;
import com.example.chat.store.SessionRepository.SessionSearch;
import com.example.chat.store.SessionRow;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.transaction.support.TransactionTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * 세션 목록 keyset 페이지네이션: 커서 인코딩/디코딩과 페이지 크기 경계.
 */
class SessionServiceTest {

    private final SessionRepository sessions = mock(SessionRepository.class);
    private final SessionService service = new SessionService(sessions, mock(ParticipantRepository.class),
            mock(EventRepository.class), mock(OutboxRepository.class), mock(PresenceService.class),
            mock(TransactionTemplate.class), new Json(JsonMapper.builder().findAndAddModules().build()));

    private static List<SessionRow> rows(int n) {
        List<SessionRow> list = new ArrayList<>();
        Instant base = Instant.parse("2026-01-01T00:00:00.123456Z");
        for (int i = 0; i < n; i++) {
            list.add(new SessionRow(UUID.randomUUID(), "s" + i, SessionStatus.ACTIVE, "alice",
                    base.minusSeconds(i), null, 1, base));
        }
        return list;
    }

    private SessionSearch lastSearch() {
        ArgumentCaptor<SessionSearch> captor = ArgumentCaptor.forClass(SessionSearch.class);
        verify(sessions, org.mockito.Mockito.atLeastOnce()).search(captor.capture());
        return captor.getValue();
    }

    @Test
    void fetchesOneExtraRowToDetectNextPageAndEncodesCursorFromLastItem() {
        List<SessionRow> fetched = rows(4);
        when(sessions.search(any())).thenReturn(fetched);

        var page = service.search(null, null, null, null, null, 3);

        assertThat(lastSearch().limit()).isEqualTo(4);
        assertThat(page.items()).hasSize(3);
        assertThat(page.nextCursor()).isNotNull();

        when(sessions.search(any())).thenReturn(List.of());
        service.search(null, null, null, null, page.nextCursor(), 3);

        SessionSearch next = lastSearch();
        assertThat(next.cursorCreatedAt()).isEqualTo(fetched.get(2).createdAt()); // 마이크로초 정밀도 유지
        assertThat(next.cursorId()).isEqualTo(fetched.get(2).id());
    }

    @Test
    void lastPageHasNoCursor() {
        when(sessions.search(any())).thenReturn(rows(2));

        assertThat(service.search(null, null, null, null, null, 3).nextCursor()).isNull();
    }

    @Test
    void pageSizeIsClamped() {
        when(sessions.search(any())).thenReturn(List.of());

        service.search(null, null, null, null, null, 0);
        assertThat(lastSearch().limit()).isEqualTo(2);
        service.search(null, null, null, null, null, 10_000);
        assertThat(lastSearch().limit()).isEqualTo(101);
    }

    @Test
    void filtersArePassedThrough() {
        when(sessions.search(any())).thenReturn(List.of());
        Instant from = Instant.parse("2026-01-01T00:00:00Z");
        Instant to = Instant.parse("2026-02-01T00:00:00Z");

        service.search(SessionStatus.ENDED, "bob", from, to, null, 20);

        SessionSearch q = lastSearch();
        assertThat(q.status()).isEqualTo(SessionStatus.ENDED);
        assertThat(q.participant()).isEqualTo("bob");
        assertThat(q.from()).isEqualTo(from);
        assertThat(q.to()).isEqualTo(to);
    }

    @Test
    void malformedCursorIsRejected() {
        assertThatThrownBy(() -> service.search(null, null, null, null, "not-a-cursor", 20))
                .isInstanceOf(ApiException.class)
                .satisfies(e -> assertThat(((ApiException) e).code()).isEqualTo("INVALID_CURSOR"));
    }
}
