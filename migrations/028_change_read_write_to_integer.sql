-- Migration: Change read_write from BOOLEAN to INTEGER
-- 0 = READ (polling/scheduler)
-- 1 = WRITE (command/on-demand)
-- 2 = READ_WRITE (both polling and writable, appears in dashboard, usable in workflow as write DYNAMIC)

-- Note: SQLite stores BOOLEAN as INTEGER internally, so we can directly update values
-- The application code will handle the integer type

-- Step 1: For parameters with direction='READ_WRITE', set read_write = 2
UPDATE parameters SET read_write = 2 WHERE direction = 'READ_WRITE';

-- Step 2: Ensure existing data is correct
-- Convert boolean to integer: false (0) -> 0, true (1) -> 1
-- This is already handled by SQLite's boolean storage, but we ensure consistency
UPDATE parameters SET read_write = 0 WHERE (read_write IS NULL OR read_write = 0) AND direction != 'READ_WRITE' AND (direction = 'READ' OR direction = '');
UPDATE parameters SET read_write = 1 WHERE read_write = 1 AND direction = 'WRITE';

