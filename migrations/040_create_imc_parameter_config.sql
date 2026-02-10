-- Migration 040: Create imc_parameter_config table
-- Stores IMC card to parameter mapping configuration

CREATE TABLE IF NOT EXISTS imc_parameter_config (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    card_key VARCHAR(100) NOT NULL UNIQUE,
    parameter_id INTEGER,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (parameter_id) REFERENCES parameters(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_imc_parameter_config_card_key ON imc_parameter_config(card_key);
CREATE INDEX IF NOT EXISTS idx_imc_parameter_config_parameter_id ON imc_parameter_config(parameter_id);
