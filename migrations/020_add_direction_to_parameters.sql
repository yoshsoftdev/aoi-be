-- Migration: Add direction field to parameters table
-- Direction: READ (for polling/scheduler) or WRITE (for command/on-demand)
-- This separates READ and WRITE parameters as distinct entities

ALTER TABLE parameters ADD COLUMN direction VARCHAR(10) DEFAULT 'READ' CHECK(direction IN ('READ', 'WRITE'));

-- Update existing parameters: if read_write=true, set direction=WRITE, otherwise READ
UPDATE parameters SET direction = 'WRITE' WHERE read_write = 1;
UPDATE parameters SET direction = 'READ' WHERE read_write = 0 OR read_write IS NULL;

-- Create index for better query performance
CREATE INDEX idx_parameters_direction ON parameters(direction);

