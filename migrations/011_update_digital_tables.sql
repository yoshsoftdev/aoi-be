-- Update digital_input and digital_output tables
-- Change pin to channel, add invert_logic

-- For digital_input: add new columns
ALTER TABLE digital_input ADD COLUMN channel INTEGER;
ALTER TABLE digital_input ADD COLUMN invert_logic BOOLEAN DEFAULT 0;

-- Migrate pin to channel (if pin exists)
-- UPDATE digital_input SET channel = CAST(REPLACE(REPLACE(pin, 'D', ''), 'IN', '') AS INTEGER) WHERE pin IS NOT NULL;

-- For digital_output: add new columns
ALTER TABLE digital_output ADD COLUMN channel INTEGER;
ALTER TABLE digital_output ADD COLUMN invert_logic BOOLEAN DEFAULT 0;

-- Migrate pin to channel (if pin exists)
-- UPDATE digital_output SET channel = CAST(REPLACE(REPLACE(pin, 'D', ''), 'OUT', '') AS INTEGER) WHERE pin IS NOT NULL;

