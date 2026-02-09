-- Migration 032: Create bypass_action table

CREATE TABLE IF NOT EXISTS bypass_action (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    bypass_id INTEGER NOT NULL,
    write_parameter_id INTEGER NOT NULL,
    value_source_type VARCHAR(20) NOT NULL CHECK (value_source_type IN ('static', 'set_point')),
    value REAL,
    source_set_point_id INTEGER,
    action_order INTEGER DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (bypass_id) REFERENCES bypass(id) ON DELETE CASCADE,
    FOREIGN KEY (write_parameter_id) REFERENCES parameters(id) ON DELETE CASCADE,
    FOREIGN KEY (source_set_point_id) REFERENCES set_point(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_bypass_action_bypass_id ON bypass_action(bypass_id);
CREATE INDEX IF NOT EXISTS idx_bypass_action_write_param ON bypass_action(write_parameter_id);

