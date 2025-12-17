-- Update parameters table to add device_id, unit, and description
-- First, add device_id column (nullable initially for migration)
ALTER TABLE parameters ADD COLUMN device_id INTEGER;
ALTER TABLE parameters ADD COLUMN unit VARCHAR(20);
ALTER TABLE parameters ADD COLUMN description VARCHAR(255);

-- Create index for device_id
CREATE INDEX IF NOT EXISTS idx_parameters_device_id ON parameters(device_id);

-- Note: After migration, device_id should be set to NOT NULL and foreign key constraint added
-- This will be done in a separate migration after data migration

