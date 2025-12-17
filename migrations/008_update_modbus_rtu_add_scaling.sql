-- Update modbus_rtu table: remove device-level fields, add scaling
-- Note: This migration assumes device table already exists and device_id is set in parameters

-- Add scaling field
ALTER TABLE modbus_rtu ADD COLUMN scaling REAL DEFAULT 1.0;

-- Remove device-level fields (port, baud, data_bits, parity, stop_bits, slave_id)
-- These are now in device table
-- Note: In SQLite, we cannot drop columns directly, so we'll create a new table and migrate data
-- For now, we'll keep the old columns but mark them as deprecated
-- A full migration script would need to:
-- 1. Create new table structure
-- 2. Copy data
-- 3. Drop old table
-- 4. Rename new table

-- For now, we'll just add the scaling field and note that old fields should not be used

