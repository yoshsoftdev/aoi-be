-- Migration: Add action_order to workflow_action table
-- This field determines the execution order of actions in a workflow
-- Actions are executed sequentially based on this order

-- Add action_order column to workflow_action table
ALTER TABLE workflow_action ADD COLUMN action_order INTEGER DEFAULT 0;

-- Update existing records: set action_order based on created_at (oldest first = order 0, 1, 2, ...)
-- This ensures existing workflows maintain their current execution order
UPDATE workflow_action
SET action_order = (
    SELECT COUNT(*)
    FROM workflow_action wa2
    WHERE wa2.workflow_id = workflow_action.workflow_id
      AND wa2.created_at <= workflow_action.created_at
      AND (wa2.id < workflow_action.id OR (wa2.id = workflow_action.id AND wa2.created_at = workflow_action.created_at))
) - 1;

-- Create index for better query performance when sorting by order
CREATE INDEX IF NOT EXISTS idx_workflow_action_order ON workflow_action(workflow_id, action_order);

