-- Migration 044: Create threshold_log table
CREATE TABLE IF NOT EXISTS threshold_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    parameter_id INTEGER NOT NULL,
    threshold_id INTEGER,
    threshold_type VARCHAR(20) NOT NULL,
    value REAL NOT NULL,
    alarm_status VARCHAR(30) NOT NULL,
    message TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (parameter_id) REFERENCES parameters(id) ON DELETE CASCADE,
    FOREIGN KEY (threshold_id) REFERENCES threshold(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_threshold_log_parameter_id ON threshold_log(parameter_id);
CREATE INDEX IF NOT EXISTS idx_threshold_log_threshold_id ON threshold_log(threshold_id);
CREATE INDEX IF NOT EXISTS idx_threshold_log_alarm_status ON threshold_log(alarm_status);
CREATE INDEX IF NOT EXISTS idx_threshold_log_created_at ON threshold_log(created_at);

