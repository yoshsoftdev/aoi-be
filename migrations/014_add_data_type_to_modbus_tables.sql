-- Migration: Add data_type column to modbus_rtu and modbus_tcp tables
-- This allows support for float32, float64, int32, and uint16/uint32 data types
-- Default is 'uint16' for backward compatibility

-- For SQLite, we'll use ALTER TABLE ADD COLUMN (supported since SQLite 3.1.16)
-- If column already exists, this will fail, but that's acceptable

-- Add data_type to modbus_rtu
-- Note: SQLite doesn't support DEFAULT in ALTER TABLE ADD COLUMN for existing rows
-- We'll set default for new rows and update existing rows
ALTER TABLE modbus_rtu ADD COLUMN data_type VARCHAR(20);

-- Update existing rows to have default value
UPDATE modbus_rtu SET data_type = 'uint16' WHERE data_type IS NULL;

-- Add data_type to modbus_tcp
ALTER TABLE modbus_tcp ADD COLUMN data_type VARCHAR(20);

-- Update existing rows to have default value
UPDATE modbus_tcp SET data_type = 'uint16' WHERE data_type IS NULL;

