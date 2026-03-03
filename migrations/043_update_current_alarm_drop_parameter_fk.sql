-- Migration 043: Relax foreign key constraint on current_alarm.parameter_id
-- Purpose:
-- - Allow alarms that are not tied to a specific parameter (e.g., SETPOINT_CHANGED triggers)
-- - Prevent FOREIGN KEY constraint failures when parameter_id does not reference an existing parameter
--
-- Approach (SQLite limitation: constraints cannot be dropped directly):
-- 1. Create a new table current_alarm_new without the FOREIGN KEY on parameter_id
-- 2. Copy all existing data from current_alarm into current_alarm_new
-- 3. Drop the old current_alarm table
-- 4. Rename current_alarm_new to current_alarm
-- 5. Recreate necessary indexes and unique constraints

PRAGMA foreign_keys=off;

BEGIN TRANSACTION;

-- 1. Create new table without parameter_id foreign key
CREATE TABLE IF NOT EXISTS current_alarm_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    workflow_id INTEGER NOT NULL,
    workflow_name VARCHAR(100) NOT NULL,
    parameter_id INTEGER NOT NULL,
    parameter_name VARCHAR(100),
    alarm_message TEXT NOT NULL,
    triggered_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (workflow_id) REFERENCES workflow(id) ON DELETE CASCADE
    -- NOTE: parameter_id foreign key intentionally removed to allow alarms
    -- that are not directly tied to a specific parameter (e.g., set-point based alarms)
);

-- 2. Copy data from old table (if exists)
INSERT INTO current_alarm_new (
    id,
    workflow_id,
    workflow_name,
    parameter_id,
    parameter_name,
    alarm_message,
    triggered_at,
    created_at,
    updated_at
)
SELECT
    id,
    workflow_id,
    workflow_name,
    parameter_id,
    parameter_name,
    alarm_message,
    triggered_at,
    created_at,
    updated_at
FROM current_alarm;

-- 3. Drop old table
DROP TABLE current_alarm;

-- 4. Rename new table
ALTER TABLE current_alarm_new RENAME TO current_alarm;

-- 5. Recreate indexes and unique constraints
CREATE INDEX IF NOT EXISTS idx_current_alarm_workflow_id ON current_alarm(workflow_id);
CREATE INDEX IF NOT EXISTS idx_current_alarm_parameter_id ON current_alarm(parameter_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_current_alarm_workflow_unique ON current_alarm(workflow_id);

COMMIT;

PRAGMA foreign_keys=on;

