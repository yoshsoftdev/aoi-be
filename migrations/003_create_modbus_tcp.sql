CREATE TABLE IF NOT EXISTS modbus_tcp (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    id_parameter INTEGER NOT NULL,
    read_write BOOLEAN DEFAULT 0,
    ip_address VARCHAR(100) NOT NULL,
    port INTEGER NOT NULL,
    slave_id INTEGER NOT NULL,
    function_code INTEGER NOT NULL,
    register_address INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    byte_order VARCHAR(20) NOT NULL,
    polling_ms INTEGER NOT NULL,
    description VARCHAR(255),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (id_parameter) REFERENCES parameters(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_modbus_tcp_id_parameter ON modbus_tcp(id_parameter);

