-- Migration: Create Workflow tables
-- Workflow Engine: IF condition THEN action
-- Workflow hanya bekerja pada parameter_id, tidak tahu register/slave/address

-- Table: workflow
CREATE TABLE IF NOT EXISTS workflow (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nama VARCHAR(100) NOT NULL,
    description TEXT,
    enabled BOOLEAN DEFAULT 1,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Table: workflow_condition
-- Condition: IF parameter_id operator threshold
CREATE TABLE IF NOT EXISTS workflow_condition (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    workflow_id INTEGER NOT NULL,
    parameter_id INTEGER NOT NULL, -- READ parameter yang dimonitor
    operator VARCHAR(10) NOT NULL, -- >, <, >=, <=, ==, !=
    threshold REAL NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (workflow_id) REFERENCES workflow(id) ON DELETE CASCADE,
    FOREIGN KEY (parameter_id) REFERENCES parameter(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_workflow_condition_workflow_id ON workflow_condition(workflow_id);
CREATE INDEX IF NOT EXISTS idx_workflow_condition_parameter_id ON workflow_condition(parameter_id);

-- Table: workflow_action
-- Action: THEN write parameter_id = value OR emit event OR delay
CREATE TABLE IF NOT EXISTS workflow_action (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    workflow_id INTEGER NOT NULL,
    action_type VARCHAR(20) NOT NULL, -- 'write', 'event', 'delay'
    parameter_id INTEGER, -- Untuk action_type='write', parameter WRITE yang akan di-write
    value REAL, -- Untuk action_type='write', nilai yang akan di-write
    event_name VARCHAR(100), -- Untuk action_type='event', nama event
    delay_ms INTEGER, -- Untuk action_type='delay', delay dalam milliseconds
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (workflow_id) REFERENCES workflow(id) ON DELETE CASCADE,
    FOREIGN KEY (parameter_id) REFERENCES parameter(id) ON DELETE CASCADE,
    CHECK (action_type IN ('write', 'event', 'delay'))
);

CREATE INDEX IF NOT EXISTS idx_workflow_action_workflow_id ON workflow_action(workflow_id);
CREATE INDEX IF NOT EXISTS idx_workflow_action_parameter_id ON workflow_action(parameter_id);

-- Table: workflow_state
-- State untuk debounce/edge trigger: menyimpan state terakhir workflow
CREATE TABLE IF NOT EXISTS workflow_state (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    workflow_id INTEGER NOT NULL UNIQUE,
    last_triggered_at DATETIME,
    last_condition_value REAL,
    last_condition_result BOOLEAN, -- true = condition met, false = condition not met
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (workflow_id) REFERENCES workflow(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_workflow_state_workflow_id ON workflow_state(workflow_id);

