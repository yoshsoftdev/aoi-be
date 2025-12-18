-- Migration: Add is_formula field to parameters table
-- This migration adds the is_formula boolean field to distinguish between raw and derived parameters

ALTER TABLE parameters ADD COLUMN is_formula BOOLEAN DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_parameters_is_formula ON parameters(is_formula);

