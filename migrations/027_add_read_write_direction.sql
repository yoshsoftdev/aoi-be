-- Migration: Add READ_WRITE direction to parameters table
-- READ_WRITE: Parameter that can be used for both reading (polling) and writing (command)
-- Note: SQLite doesn't support ALTER TABLE to modify CHECK constraints directly
-- We'll update the constraint by recreating the table

-- Step 1: Create new table with updated constraint
CREATE TABLE IF NOT EXISTS parameters_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id INTEGER NOT NULL,
    nama VARCHAR(100) NOT NULL,
    type VARCHAR(50) NOT NULL,
    direction VARCHAR(10) DEFAULT 'READ' CHECK(direction IN ('READ', 'WRITE', 'READ_WRITE')),
    read_write BOOLEAN DEFAULT 0,
    parameter_mode VARCHAR(20) DEFAULT 'DIRECT',
    write_mode VARCHAR(10),
    preset_value TEXT,
    unit VARCHAR(20),
    custom_rumus TEXT,
    is_formula BOOLEAN DEFAULT 0,
    description VARCHAR(255),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (device_id) REFERENCES koneksi(id)
);

-- Step 2: Copy data from old table
INSERT INTO parameters_new SELECT * FROM parameters;

-- Step 3: Drop old table
DROP TABLE parameters;

-- Step 4: Rename new table
ALTER TABLE parameters_new RENAME TO parameters;

-- Step 5: Recreate indexes
CREATE INDEX IF NOT EXISTS idx_parameters_device_id ON parameters(device_id);
CREATE INDEX IF NOT EXISTS idx_parameters_direction ON parameters(direction);
CREATE INDEX IF NOT EXISTS idx_parameters_type ON parameters(type);
CREATE INDEX IF NOT EXISTS idx_parameters_is_formula ON parameters(is_formula);
CREATE INDEX IF NOT EXISTS idx_parameters_parameter_mode ON parameters(parameter_mode);

