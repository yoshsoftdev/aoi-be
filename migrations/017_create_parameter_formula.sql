-- Migration: Create parameter_formula table
-- This table stores formula expressions for derived parameters

CREATE TABLE IF NOT EXISTS parameter_formula (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    parameter_id INTEGER NOT NULL,
    expression TEXT NOT NULL,
    expression_type VARCHAR(20) DEFAULT 'math',
    description VARCHAR(255),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (parameter_id) REFERENCES parameters(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_parameter_formula_parameter_id ON parameter_formula(parameter_id);
CREATE INDEX IF NOT EXISTS idx_parameter_formula_id ON parameter_formula(id);

