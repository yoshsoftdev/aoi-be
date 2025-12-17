-- Update analog_input and analog_output tables
-- Change pin to channel, add raw_min/max, eng_min/max, scaling

-- For analog_input: add new columns
ALTER TABLE analog_input ADD COLUMN channel INTEGER;
ALTER TABLE analog_input ADD COLUMN raw_min REAL DEFAULT 0;
ALTER TABLE analog_input ADD COLUMN raw_max REAL DEFAULT 1024;
ALTER TABLE analog_input ADD COLUMN eng_min REAL DEFAULT 0;
ALTER TABLE analog_input ADD COLUMN eng_max REAL DEFAULT 1024;
ALTER TABLE analog_input ADD COLUMN scaling REAL DEFAULT 1.0;

-- Migrate pin to channel (if pin exists)
-- UPDATE analog_input SET channel = CAST(REPLACE(pin, 'A', '') AS INTEGER) WHERE pin IS NOT NULL;

-- For analog_output: add new columns
ALTER TABLE analog_output ADD COLUMN channel INTEGER;
ALTER TABLE analog_output ADD COLUMN raw_min REAL DEFAULT 0;
ALTER TABLE analog_output ADD COLUMN raw_max REAL DEFAULT 1024;
ALTER TABLE analog_output ADD COLUMN eng_min REAL DEFAULT 0;
ALTER TABLE analog_output ADD COLUMN eng_max REAL DEFAULT 1024;
ALTER TABLE analog_output ADD COLUMN scaling REAL DEFAULT 1.0;

-- Migrate pin to channel (if pin exists)
-- UPDATE analog_output SET channel = CAST(REPLACE(pin, 'DAC', '') AS INTEGER) WHERE pin IS NOT NULL;

