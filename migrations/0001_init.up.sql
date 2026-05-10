-- 0001_init.up.sql
-- Ships every artifact from docs/design/01-schema.md §1, §2, §3, and §Cross-table.

CREATE TABLE notifications (
  id              UUID        PRIMARY KEY,
  batch_id        UUID,
  channel         TEXT        NOT NULL,
  recipient       TEXT        NOT NULL,
  priority        SMALLINT    NOT NULL DEFAULT 1,
  content         TEXT,
  template        TEXT,
  template_data   JSONB,
  status          TEXT        NOT NULL DEFAULT 'PENDING',
  attempt         INT         NOT NULL DEFAULT 0,
  eligible_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  scheduled_at    TIMESTAMPTZ,
  failure_reason  TEXT,
  idempotency_key TEXT        NOT NULL,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

  CONSTRAINT notifications_idempotency_key_unique
    UNIQUE (idempotency_key)
);

CREATE INDEX notifications_dispatcher_claim_idx
  ON notifications (channel, priority DESC, eligible_at)
  WHERE status = 'PENDING';

CREATE INDEX notifications_reaper_idx
  ON notifications (updated_at)
  WHERE status = 'DISPATCHED';

CREATE INDEX notifications_batch_idx
  ON notifications (batch_id)
  WHERE batch_id IS NOT NULL;

CREATE INDEX notifications_list_idx
  ON notifications (created_at DESC, id DESC);

CREATE TABLE delivery_attempts (
  notification_id UUID        NOT NULL REFERENCES notifications(id) ON DELETE CASCADE,
  attempt         INT         NOT NULL,
  started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at     TIMESTAMPTZ,
  classification  TEXT,
  response        JSONB,
  error_message   TEXT,

  PRIMARY KEY (notification_id, attempt)
);

CREATE TABLE outbox (
  id            BIGSERIAL    PRIMARY KEY,
  topic         TEXT         NOT NULL,
  partition_key TEXT,
  headers       JSONB,
  payload       JSONB        NOT NULL,
  created_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
  published_at  TIMESTAMPTZ
);

CREATE INDEX outbox_unpublished_idx
  ON outbox (id) WHERE published_at IS NULL;

CREATE OR REPLACE FUNCTION set_updated_at() RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER notifications_set_updated_at
  BEFORE UPDATE ON notifications
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();
