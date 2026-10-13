-- =====================================================================
-- Event store (source of truth)  +  inline write model  +  async read models
-- =====================================================================

-- ---------------------------------------------------------------------
-- chat_session : 세션 메타 + 세션별 시퀀스 카운터 (write model, 동기 갱신)
-- ---------------------------------------------------------------------
CREATE TABLE chat_session (
    id                  UUID         PRIMARY KEY,
    title               VARCHAR(200),
    status              VARCHAR(16)  NOT NULL,              -- ACTIVE | INTERRUPTED | ENDED
    created_by          VARCHAR(64)  NOT NULL,
    created_at          TIMESTAMPTZ  NOT NULL,
    ended_at            TIMESTAMPTZ,
    last_seq            BIGINT       NOT NULL DEFAULT 0,    -- 마지막으로 발급한 seq (gap-free 카운터)
    last_event_at       TIMESTAMPTZ  NOT NULL,              -- 세션 내 server_received_at 단조 증가 보장용
    create_request_key  VARCHAR(64)  UNIQUE,                -- POST /sessions Idempotency-Key
    CONSTRAINT chk_session_status CHECK (status IN ('ACTIVE', 'INTERRUPTED', 'ENDED'))
);

-- GET /sessions : 상태 + 기간 필터, created_at DESC keyset 페이지네이션
CREATE INDEX idx_session_status_created ON chat_session (status, created_at DESC, id DESC);
CREATE INDEX idx_session_created        ON chat_session (created_at DESC, id DESC);

-- ---------------------------------------------------------------------
-- participant : 세션 참여자 현재 상태 (write model, 이벤트 append 와 같은 트랜잭션에서 갱신)
-- ---------------------------------------------------------------------
CREATE TABLE participant (
    session_id      UUID         NOT NULL REFERENCES chat_session (id),
    user_id         VARCHAR(64)  NOT NULL,
    state           VARCHAR(16)  NOT NULL,                  -- JOINED | DISCONNECTED | LEFT
    joined_at       TIMESTAMPTZ  NOT NULL,
    left_at         TIMESTAMPTZ,
    last_event_seq  BIGINT       NOT NULL,
    PRIMARY KEY (session_id, user_id),
    CONSTRAINT chk_participant_state CHECK (state IN ('JOINED', 'DISCONNECTED', 'LEFT'))
);

-- GET /sessions?participant=u1 : 사용자 기준 세션 조회
CREATE INDEX idx_participant_user ON participant (user_id, session_id);

-- ---------------------------------------------------------------------
-- event : append-only 이벤트 원장. 수정/삭제하지 않는다.
--   - (session_id, seq)             : 정렬/리플레이 기준 (서버 발급, gap-free)
--   - (session_id, client_event_id) : 멱등성 키 (중복 저장 방지)
-- ---------------------------------------------------------------------
CREATE TABLE event (
    session_id          UUID         NOT NULL REFERENCES chat_session (id),
    seq                 BIGINT       NOT NULL,
    client_event_id     VARCHAR(64)  NOT NULL,
    type                VARCHAR(32)  NOT NULL,
    user_id             VARCHAR(64)  NOT NULL,
    payload             JSONB        NOT NULL DEFAULT '{}'::jsonb,
    client_seq          BIGINT,                             -- 클라이언트 송신 순번 (gap 감지/디버깅용)
    client_ts           TIMESTAMPTZ,                        -- 클라이언트 시각 (참고용, 정렬에 사용하지 않음)
    server_received_at  TIMESTAMPTZ  NOT NULL,              -- 세션 내에서 seq 와 함께 단조 증가
    PRIMARY KEY (session_id, seq),
    CONSTRAINT uq_event_client_event_id UNIQUE (session_id, client_event_id)
);

-- timeline?at=... : 시각 -> seq 변환 (session_id, server_received_at) 범위 탐색
CREATE INDEX idx_event_session_received ON event (session_id, server_received_at, seq);

-- ---------------------------------------------------------------------
-- session_snapshot : 특정 seq 시점의 전체 세션 상태 (리플레이 시작점)
-- ---------------------------------------------------------------------
CREATE TABLE session_snapshot (
    session_id      UUID         NOT NULL REFERENCES chat_session (id),
    seq             BIGINT       NOT NULL,                  -- 이 seq 까지 반영된 상태
    schema_version  INT          NOT NULL,                  -- 상태 포맷 버전 (포맷 변경 시 무효화 판단)
    state           JSONB        NOT NULL,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (session_id, seq)                           -- 결정적 상태이므로 동일 seq 재생성은 ON CONFLICT DO NOTHING
);

-- ---------------------------------------------------------------------
-- message_projection : 대화창 조회용 read model (비동기 갱신, eventually consistent)
-- ---------------------------------------------------------------------
CREATE TABLE message_projection (
    session_id   UUID          NOT NULL,
    message_id   VARCHAR(64)   NOT NULL,                    -- MESSAGE 이벤트의 client_event_id
    seq          BIGINT        NOT NULL,                    -- MESSAGE 이벤트의 seq (표시 순서)
    sender_id    VARCHAR(64)   NOT NULL,
    content      TEXT,
    status       VARCHAR(16)   NOT NULL,                    -- SENT | EDITED | DELETED
    created_at   TIMESTAMPTZ   NOT NULL,
    updated_at   TIMESTAMPTZ   NOT NULL,
    updated_seq  BIGINT        NOT NULL,                    -- 마지막으로 반영한 이벤트 seq
    PRIMARY KEY (session_id, message_id)
);

-- 대화창 최근 N개 / 이전 페이지 : WHERE session_id = ? AND seq < ? ORDER BY seq DESC LIMIT N
CREATE UNIQUE INDEX idx_msgproj_session_seq ON message_projection (session_id, seq DESC);

-- ---------------------------------------------------------------------
-- projection_offset : projection 별 세션 체크포인트 (멱등 적용 + 세션 단위 직렬화 락)
-- ---------------------------------------------------------------------
CREATE TABLE projection_offset (
    session_id         UUID         NOT NULL,
    projection         VARCHAR(32)  NOT NULL,
    last_applied_seq   BIGINT       NOT NULL DEFAULT 0,
    last_snapshot_seq  BIGINT       NOT NULL DEFAULT 0,
    updated_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (session_id, projection)
);

-- ---------------------------------------------------------------------
-- outbox : 이벤트 append 와 같은 트랜잭션에 기록되는 비동기 작업 큐 (Transactional Outbox)
-- ---------------------------------------------------------------------
CREATE TABLE outbox (
    id               BIGSERIAL    PRIMARY KEY,
    session_id       UUID         NOT NULL,
    event_seq        BIGINT       NOT NULL,
    status           VARCHAR(16)  NOT NULL DEFAULT 'PENDING', -- PENDING | PROCESSING
    attempts         INT          NOT NULL DEFAULT 0,
    next_attempt_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    locked_by        VARCHAR(64),
    locked_until     TIMESTAMPTZ,
    last_error       TEXT,
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- 워커 폴링: 처리 가능한 PENDING 행만 부분 인덱스로 탐색
CREATE INDEX idx_outbox_ready   ON outbox (next_attempt_at, id) WHERE status = 'PENDING';
-- 리스 만료(워커 크래시) 회수
CREATE INDEX idx_outbox_leased  ON outbox (locked_until) WHERE status = 'PROCESSING';
-- 세션 따라잡기 후 일괄 정리 (coalescing delete)
CREATE INDEX idx_outbox_session ON outbox (session_id, event_seq);

-- ---------------------------------------------------------------------
-- dead_letter : 최대 재시도 초과 작업. 운영자가 원인 수정 후 재처리(requeue)
-- ---------------------------------------------------------------------
CREATE TABLE dead_letter (
    id             BIGSERIAL    PRIMARY KEY,
    outbox_id      BIGINT       NOT NULL,
    session_id     UUID         NOT NULL,
    event_seq      BIGINT       NOT NULL,
    attempts       INT          NOT NULL,
    last_error     TEXT,
    failed_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    requeued_at    TIMESTAMPTZ
);

CREATE INDEX idx_dead_letter_open ON dead_letter (failed_at) WHERE requeued_at IS NULL;
