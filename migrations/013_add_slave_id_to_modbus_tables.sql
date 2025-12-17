-- Add slave_id column to modbus_rtu and modbus_tcp tables
-- This allows parameter-level override of device-level slave_id

-- Add slave_id to modbus_rtu if it doesn't exist
-- SQLite doesn't support IF NOT EXISTS for ALTER TABLE ADD COLUMN, so we check first
-- If column exists, this will fail silently in SQLite (which is acceptable)

-- For modbus_rtu
-- Note: In SQLite, we need to recreate the table if column doesn't exist
-- First, check if we need to add it (this is a simplified approach)
-- In production, you might want to check column existence first

-- Add slave_id to modbus_rtu (if not exists, will fail silently if already exists)
-- We'll use a more robust approach: recreate table if needed
CREATE TABLE IF NOT EXISTS modbus_rtu_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    id_parameter INTEGER NOT NULL UNIQUE,
    read_write BOOLEAN DEFAULT 0,
    slave_id INTEGER NOT NULL DEFAULT 1,
    function_code INTEGER NOT NULL,
    register_address INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    byte_order VARCHAR(20) NOT NULL,
    scaling REAL DEFAULT 1.0,
    polling_ms INTEGER,
    description VARCHAR(255),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (id_parameter) REFERENCES parameters(id) ON DELETE CASCADE
);

-- Copy data from old table if it exists, using device slave_id as default
INSERT INTO modbus_rtu_new (id, id_parameter, read_write, slave_id, function_code, register_address, quantity, byte_order, scaling, polling_ms, description, created_at, updated_at)
SELECT 
    m.id,
    m.id_parameter,
    m.read_write,
    COALESCE((SELECT d.slave_id FROM device d JOIN parameters p ON p.device_id = d.id WHERE p.id = m.id_parameter), 1) as slave_id,
    m.function_code,
    m.register_address,
    m.quantity,
    m.byte_order,
    COALESCE(m.scaling, 1.0) as scaling,
    m.polling_ms,
    m.description,
    m.created_at,
    m.updated_at
FROM modbus_rtu m
WHERE NOT EXISTS (SELECT 1 FROM modbus_rtu_new WHERE id = m.id);

-- Drop old table and rename new one
DROP TABLE IF EXISTS modbus_rtu;
ALTER TABLE modbus_rtu_new RENAME TO modbus_rtu;
CREATE UNIQUE INDEX IF NOT EXISTS idx_modbus_rtu_id_parameter ON modbus_rtu(id_parameter);

-- Same for modbus_tcp
CREATE TABLE IF NOT EXISTS modbus_tcp_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    id_parameter INTEGER NOT NULL UNIQUE,
    read_write BOOLEAN DEFAULT 0,
    slave_id INTEGER NOT NULL DEFAULT 1,
    function_code INTEGER NOT NULL,
    register_address INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    byte_order VARCHAR(20) NOT NULL,
    scaling REAL DEFAULT 1.0,
    polling_ms INTEGER,
    description VARCHAR(255),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (id_parameter) REFERENCES parameters(id) ON DELETE CASCADE
);

-- Copy data from old table if it exists
INSERT INTO modbus_tcp_new (id, id_parameter, read_write, slave_id, function_code, register_address, quantity, byte_order, scaling, polling_ms, description, created_at, updated_at)
SELECT 
    m.id,
    m.id_parameter,
    m.read_write,
    COALESCE((SELECT d.slave_id FROM device d JOIN parameters p ON p.device_id = d.id WHERE p.id = m.id_parameter), 1) as slave_id,
    m.function_code,
    m.register_address,
    m.quantity,
    m.byte_order,
    COALESCE(m.scaling, 1.0) as scaling,
    m.polling_ms,
    m.description,
    m.created_at,
    m.updated_at
FROM modbus_tcp m
WHERE NOT EXISTS (SELECT 1 FROM modbus_tcp_new WHERE id = m.id);

-- Drop old table and rename new one
DROP TABLE IF EXISTS modbus_tcp;
ALTER TABLE modbus_tcp_new RENAME TO modbus_tcp;
CREATE UNIQUE INDEX IF NOT EXISTS idx_modbus_tcp_id_parameter ON modbus_tcp(id_parameter);

