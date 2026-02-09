-- Migration 031: Create bypass_rule table
-- NOTE: Karena left_source_id & right_source_id bisa mengacu ke tabel berbeda
-- (set_point atau parameters), kita TIDAK membuat FOREIGN KEY di level DB.

CREATE TABLE IF NOT EXISTS bypass_rule (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    bypass_id INTEGER NOT NULL,
    condition_type VARCHAR(20) NOT NULL CHECK (condition_type IN ('always', 'compare')),
    left_source_type VARCHAR(20) NOT NULL CHECK (left_source_type IN ('set_point', 'parameter')),
    left_source_id INTEGER NOT NULL,
    operator VARCHAR(5), -- >, <, =, >=, <= (hanya untuk compare)
    right_value_type VARCHAR(20) NOT NULL CHECK (right_value_type IN ('static', 'set_point')),
    right_value REAL,
    right_source_id INTEGER,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (bypass_id) REFERENCES bypass(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_bypass_rule_bypass_id ON bypass_rule(bypass_id);
CREATE INDEX IF NOT EXISTS idx_bypass_rule_left_source ON bypass_rule(left_source_type, left_source_id);

