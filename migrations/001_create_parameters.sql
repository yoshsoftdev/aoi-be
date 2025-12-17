CREATE TABLE IF NOT EXISTS parameters (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nama VARCHAR(255) NOT NULL,
    type VARCHAR(50) NOT NULL,
    read_write BOOLEAN DEFAULT 0,
    custom_rumus TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_parameters_type ON parameters(type);
CREATE INDEX IF NOT EXISTS idx_parameters_id ON parameters(id);

