CREATE TABLE IF NOT EXISTS analog_input (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    id_parameter INTEGER NOT NULL,
    pin VARCHAR(20) NOT NULL,
    scale_min REAL NOT NULL,
    scale_max REAL NOT NULL,
    polling_ms INTEGER NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (id_parameter) REFERENCES parameters(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_analog_input_id_parameter ON analog_input(id_parameter);

CREATE TABLE IF NOT EXISTS analog_output (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    id_parameter INTEGER NOT NULL,
    pin VARCHAR(20) NOT NULL,
    min_value REAL NOT NULL,
    max_value REAL NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (id_parameter) REFERENCES parameters(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_analog_output_id_parameter ON analog_output(id_parameter);

