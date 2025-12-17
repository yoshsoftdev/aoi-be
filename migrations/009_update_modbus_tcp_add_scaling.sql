-- Update modbus_tcp table: remove device-level fields, add scaling
-- Add scaling field
ALTER TABLE modbus_tcp ADD COLUMN scaling REAL DEFAULT 1.0;

-- Note: ip_address, port, slave_id are now in device table
-- Similar to modbus_rtu, old fields are kept for migration compatibility

