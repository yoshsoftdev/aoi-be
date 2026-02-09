-- Migration 039: Add write_set_point_id to bypass_action table
-- This allows bypass actions to write to set-points in addition to parameters

-- Add new column (nullable, since existing actions write to parameters)
ALTER TABLE bypass_action ADD COLUMN write_set_point_id INTEGER;

-- Create index for the new column
CREATE INDEX IF NOT EXISTS idx_bypass_action_write_set_point ON bypass_action(write_set_point_id);

-- Make write_parameter_id nullable (since now we can write to set-points instead)
-- Note: SQLite doesn't support ALTER COLUMN, so we need to recreate the table
-- But since we're adding a new column, we'll keep write_parameter_id as NOT NULL for now
-- and validate at application layer that either write_parameter_id or write_set_point_id is set
