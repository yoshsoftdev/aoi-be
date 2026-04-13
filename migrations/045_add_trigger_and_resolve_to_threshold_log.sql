-- Migration 045: Add trigger_at and resolve_at to threshold_log
ALTER TABLE threshold_log ADD COLUMN trigger_at DATETIME;
ALTER TABLE threshold_log ADD COLUMN resolve_at DATETIME;

CREATE INDEX IF NOT EXISTS idx_threshold_log_trigger_at ON threshold_log(trigger_at);
CREATE INDEX IF NOT EXISTS idx_threshold_log_resolve_at ON threshold_log(resolve_at);

-- Backfill trigger_at for old rows
UPDATE threshold_log
SET trigger_at = COALESCE(trigger_at, created_at)
WHERE trigger_at IS NULL;

