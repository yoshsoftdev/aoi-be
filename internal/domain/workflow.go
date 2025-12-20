package domain

import "time"

// Workflow represents a workflow rule
// Workflow hanya bekerja pada parameter_id, tidak tahu register/slave/address
type Workflow struct {
	ID          uint      `gorm:"primaryKey" json:"id"`
	Nama        string    `gorm:"size:100;not null" json:"nama"`
	Description string    `gorm:"type:text" json:"description"`
	Enabled     bool      `gorm:"default:1" json:"enabled"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`

	// Relations
	Conditions []WorkflowCondition `gorm:"foreignKey:WorkflowID;constraint:OnDelete:CASCADE" json:"conditions,omitempty"`
	Actions    []WorkflowAction    `gorm:"foreignKey:WorkflowID;constraint:OnDelete:CASCADE" json:"actions,omitempty"`
	State      *WorkflowState      `gorm:"foreignKey:WorkflowID;constraint:OnDelete:CASCADE" json:"state,omitempty"`
}

// TableName specifies the table name for Workflow
func (Workflow) TableName() string {
	return "workflow"
}

// WorkflowCondition represents a condition in a workflow
// IF parameter_id operator threshold
type WorkflowCondition struct {
	ID          uint      `gorm:"primaryKey" json:"id"`
	WorkflowID  uint      `gorm:"not null;index" json:"workflow_id"`
	ParameterID uint      `gorm:"not null;index" json:"parameter_id"` // READ parameter yang dimonitor
	Operator    string    `gorm:"size:10;not null" json:"operator"`   // >, <, >=, <=, ==, !=
	Threshold   float64   `gorm:"not null" json:"threshold"`
	CreatedAt   time.Time `json:"created_at"`

	// Relations
	Workflow  *Workflow  `gorm:"foreignKey:WorkflowID" json:"workflow,omitempty"`
	Parameter *Parameter `gorm:"foreignKey:ParameterID" json:"parameter,omitempty"`
}

// TableName specifies the table name for WorkflowCondition
func (WorkflowCondition) TableName() string {
	return "workflow_condition"
}

// WorkflowAction represents an action in a workflow
// THEN write parameter_id = value OR emit event OR delay
type WorkflowAction struct {
	ID          uint      `gorm:"primaryKey" json:"id"`
	WorkflowID  uint      `gorm:"not null;index" json:"workflow_id"`
	ActionType  string    `gorm:"size:20;not null" json:"action_type"`    // 'write', 'event', 'delay'
	ParameterID *uint     `gorm:"index" json:"parameter_id,omitempty"`    // Untuk action_type='write', parameter WRITE yang akan di-write
	Value       *float64  `gorm:"type:REAL" json:"value,omitempty"`       // Untuk action_type='write', nilai yang akan di-write
	EventName   *string   `gorm:"size:100" json:"event_name,omitempty"`   // Untuk action_type='event', nama event
	DelayMs     *int      `gorm:"type:INTEGER" json:"delay_ms,omitempty"` // Untuk action_type='delay', delay dalam milliseconds
	CreatedAt   time.Time `json:"created_at"`

	// Relations
	Workflow  *Workflow  `gorm:"foreignKey:WorkflowID" json:"workflow,omitempty"`
	Parameter *Parameter `gorm:"foreignKey:ParameterID" json:"parameter,omitempty"`
}

// TableName specifies the table name for WorkflowAction
func (WorkflowAction) TableName() string {
	return "workflow_action"
}

// WorkflowState stores the last state of a workflow for debounce/edge trigger
type WorkflowState struct {
	ID                  uint       `gorm:"primaryKey" json:"id"`
	WorkflowID          uint       `gorm:"not null;uniqueIndex" json:"workflow_id"`
	LastTriggeredAt     *time.Time `json:"last_triggered_at,omitempty"`
	LastConditionValue  *float64   `gorm:"type:REAL" json:"last_condition_value,omitempty"`
	LastConditionResult *bool      `gorm:"type:BOOLEAN" json:"last_condition_result,omitempty"` // true = condition met, false = condition not met
	CreatedAt           time.Time  `json:"created_at"`
	UpdatedAt           time.Time  `json:"updated_at"`

	// Relations
	Workflow *Workflow `gorm:"foreignKey:WorkflowID" json:"workflow,omitempty"`
}

// TableName specifies the table name for WorkflowState
func (WorkflowState) TableName() string {
	return "workflow_state"
}
