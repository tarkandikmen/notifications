-- 0001_init.down.sql
-- Reverses 0001_init.up.sql in reverse order: trigger, function, indexes,
-- tables. outbox -> delivery_attempts -> notifications because
-- delivery_attempts.notification_id references notifications(id).

DROP TRIGGER IF EXISTS notifications_set_updated_at ON notifications;

DROP FUNCTION IF EXISTS set_updated_at();

DROP INDEX IF EXISTS outbox_unpublished_idx;
DROP INDEX IF EXISTS notifications_list_idx;
DROP INDEX IF EXISTS notifications_batch_idx;
DROP INDEX IF EXISTS notifications_reaper_idx;
DROP INDEX IF EXISTS notifications_dispatcher_claim_idx;

DROP TABLE IF EXISTS outbox;
DROP TABLE IF EXISTS delivery_attempts;
DROP TABLE IF EXISTS notifications;
