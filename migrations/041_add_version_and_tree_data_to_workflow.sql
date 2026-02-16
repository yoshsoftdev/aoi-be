-- Migration: Add version and tree_data columns to workflow table
-- Version 1: Flat structure (existing conditions[], actions[])
-- Version 2: Tree structure (tree_data JSON)

ALTER TABLE workflow ADD COLUMN version INTEGER NOT NULL DEFAULT 1;
ALTER TABLE workflow ADD COLUMN tree_data TEXT;

-- Create index on version for faster filtering
CREATE INDEX IF NOT EXISTS idx_workflow_version ON workflow(version);
