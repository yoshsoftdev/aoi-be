-- Migration: Create parameter_formula_var table
-- This table maps formula variables to source parameters

CREATE TABLE IF NOT EXISTS parameter_formula_var (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    formula_id INTEGER NOT NULL,
    var_name VARCHAR(20) NOT NULL,
    source_parameter_id INTEGER NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (formula_id) REFERENCES parameter_formula(id) ON DELETE CASCADE,
    FOREIGN KEY (source_parameter_id) REFERENCES parameters(id) ON DELETE CASCADE,
    UNIQUE(formula_id, var_name)
);

CREATE INDEX IF NOT EXISTS idx_parameter_formula_var_formula_id ON parameter_formula_var(formula_id);
CREATE INDEX IF NOT EXISTS idx_parameter_formula_var_source_param_id ON parameter_formula_var(source_parameter_id);
CREATE INDEX IF NOT EXISTS idx_parameter_formula_var_var_name ON parameter_formula_var(var_name);

