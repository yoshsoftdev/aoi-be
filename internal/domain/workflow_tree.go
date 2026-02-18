package domain

// WorkflowTree represents the tree structure for version 2 workflows
// This is the JSON structure stored in tree_data field
type WorkflowTree struct {
	ID            string                  `json:"id"` // workflow_id as string
	Name          string                  `json:"name"`
	Trigger       *TriggerNode            `json:"trigger"`
	Root          *TreeNode               `json:"root"`
	NodePositions map[string]NodePosition `json:"node_positions,omitempty"` // Map of nodeId -> position for UI layout
}

// NodePosition represents the position of a node in the React Flow canvas
type NodePosition struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
}

// TriggerNode represents the trigger for a workflow
type TriggerNode struct {
	Type      string  `json:"type"`                // "PARAMETER_CHANGED", "SETPOINT_CHANGED"
	SourceID  uint    `json:"source_id"`           // parameter_id or setpoint_id
	Parameter *string `json:"parameter,omitempty"` // parameter name (optional, for display)
}

// TreeNode represents a node in the workflow tree
// Can be either an IF node or an Action node
type TreeNode struct {
	Type string `json:"type"`         // "if" or "action"
	ID   string `json:"id,omitempty"` // node ID (for reference)

	// For IF nodes
	Condition *Condition `json:"condition,omitempty"`
	Then      []TreeNode `json:"then,omitempty"` // THEN branch children
	Else      []TreeNode `json:"else,omitempty"` // ELSE branch children

	// For Action nodes
	ActionType string                 `json:"action_type,omitempty"` // "ALARM_NOTIFICATION", "WRITE_DYNAMIC", "WRITE_PRESET"
	ActionData map[string]interface{} `json:"action_data,omitempty"` // Action-specific data
}

// Condition represents a condition in an IF node
type Condition struct {
	Left     *ValueSource `json:"left"`     // Left side of condition
	Operator string       `json:"operator"` // ">", "<", ">=", "<=", "==", "!="
	Right    *ValueSource `json:"right"`    // Right side of condition
}

// ValueSource represents a value source (parameter, setpoint, or static value)
type ValueSource struct {
	Type      string   `json:"type"`                // "parameter", "setpoint", "static"
	SourceID  *uint    `json:"source_id,omitempty"` // parameter_id or setpoint_id (if type is parameter/setpoint)
	Value     *float64 `json:"value,omitempty"`     // static value (if type is static)
	Parameter *string  `json:"parameter,omitempty"` // parameter name (optional, for display)
}

// Action types constants
const (
	ActionTypeAlarmNotification = "ALARM_NOTIFICATION"
	ActionTypeWriteDynamic      = "WRITE_DYNAMIC"
	ActionTypeWritePreset       = "WRITE_PRESET"
)

// Trigger types constants
const (
	TriggerTypeParameterChanged = "PARAMETER_CHANGED"
	TriggerTypeSetpointChanged  = "SETPOINT_CHANGED"
)

// Workflow version constants
const (
	WorkflowVersionFlat = 1 // Flat structure (conditions[], actions[])
	WorkflowVersionTree = 2 // Tree structure (tree_data JSON)
)
