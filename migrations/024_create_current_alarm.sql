-- Migration: Create current_alarm table
-- Stores active alarms triggered by workflows
-- When workflow condition is no longer met, alarm is removed from this table

-- Table: current_alarm
CREATE TABLE IF NOT EXISTS current_alarm (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    workflow_id INTEGER NOT NULL,
    workflow_name VARCHAR(100) NOT NULL,
    parameter_id INTEGER NOT NULL,
    parameter_name VARCHAR(100),
    alarm_message TEXT NOT NULL,
    triggered_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (workflow_id) REFERENCES workflow(id) ON DELETE CASCADE,
    FOREIGN KEY (parameter_id) REFERENCES parameter(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_current_alarm_workflow_id ON current_alarm(workflow_id);
CREATE INDEX IF NOT EXISTS idx_current_alarm_parameter_id ON current_alarm(parameter_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_current_alarm_workflow_unique ON current_alarm(workflow_id);

