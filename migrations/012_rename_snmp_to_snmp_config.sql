-- Rename snmp table to snmp_config and update fields
-- SQLite doesn't support RENAME TABLE directly, so we'll create new table and migrate

-- Create new snmp_config table with updated structure
CREATE TABLE IF NOT EXISTS snmp_config (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    id_parameter INTEGER NOT NULL,
    version VARCHAR(10) DEFAULT 'v2c', -- SNMP version: 'v1', 'v2c', 'v3'
    community VARCHAR(100) NOT NULL, -- SNMP community string (for v1/v2c)
    oid VARCHAR(200) NOT NULL, -- SNMP Object Identifier (OID) to query
    ip_address VARCHAR(50) NOT NULL, -- Target device IP address
    port INTEGER DEFAULT 161, -- SNMP port (default: 161)
    polling_ms INTEGER NOT NULL,
    description VARCHAR(255),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (id_parameter) REFERENCES parameters(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_snmp_config_id_parameter ON snmp_config(id_parameter);

-- Migrate data from old snmp table to new snmp_config table
INSERT INTO snmp_config (id_parameter, version, community, oid, ip_address, port, polling_ms, description, created_at, updated_at)
SELECT id_parameter, 'v2c', community, oid, ip_address, 161, polling_ms, description, created_at, updated_at
FROM snmp;

-- Note: Old snmp table will be kept for now, can be dropped later after verification
-- DROP TABLE IF EXISTS snmp;

