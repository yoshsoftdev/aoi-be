CREATE TABLE IF NOT EXISTS snmp (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    id_parameter INTEGER NOT NULL,
    ip_address VARCHAR(100) NOT NULL,
    community VARCHAR(50) NOT NULL,
    oid VARCHAR(255) NOT NULL,
    polling_ms INTEGER NOT NULL,
    description VARCHAR(255),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (id_parameter) REFERENCES parameters(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_snmp_id_parameter ON snmp(id_parameter);

