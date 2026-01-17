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
// THEN trigger(parameter_id) OR write(parameter_id, value) OR emit event OR delay OR alarm
// ActionType:
//   - TRIGGER_WRITE: trigger PRESET parameter (no value sent)
//   - WRITE_WITH_VALUE: write DYNAMIC parameter (value required)
//   - event: emit event
//   - delay: delay execution
//   - alarm: show notification alarm with custom message
type WorkflowAction struct {
	ID           uint      `gorm:"primaryKey" json:"id"`
	WorkflowID   uint      `gorm:"not null;index" json:"workflow_id"`
	ActionType   string    `gorm:"size:20;not null" json:"action_type"`      // 'TRIGGER_WRITE', 'WRITE_WITH_VALUE', 'event', 'delay', 'alarm'
	ParameterID  *uint     `gorm:"index" json:"parameter_id,omitempty"`      // Untuk action_type='TRIGGER_WRITE' atau 'WRITE_WITH_VALUE', parameter WRITE yang akan di-write
	Value        *float64  `gorm:"type:REAL" json:"value,omitempty"`         // Untuk action_type='WRITE_WITH_VALUE', nilai yang akan di-write (WAJIB untuk DYNAMIC)
	EventName    *string   `gorm:"size:100" json:"event_name,omitempty"`     // Untuk action_type='event', nama event
	DelayMs      *int      `gorm:"type:INTEGER" json:"delay_ms,omitempty"`   // Untuk action_type='delay', delay dalam milliseconds
	AlarmMessage *string   `gorm:"type:text" json:"alarm_message,omitempty"` // Untuk action_type='alarm', pesan notifikasi yang akan ditampilkan
	CreatedAt    time.Time `json:"created_at"`

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

// CurrentAlarm represents an active alarm triggered by a workflow
type CurrentAlarm struct {
	ID            uint      `gorm:"primaryKey" json:"id"`
	WorkflowID    uint      `gorm:"not null;uniqueIndex;index" json:"workflow_id"`
	WorkflowName  string    `gorm:"size:100;not null" json:"workflow_name"`
	ParameterID   uint      `gorm:"not null;index" json:"parameter_id"`
	ParameterName *string   `gorm:"size:100" json:"parameter_name,omitempty"`
	AlarmMessage  string    `gorm:"type:text;not null" json:"alarm_message"`
	TriggeredAt   time.Time `gorm:"default:CURRENT_TIMESTAMP" json:"triggered_at"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`

	// Relations
	Workflow  *Workflow  `gorm:"foreignKey:WorkflowID" json:"workflow,omitempty"`
	Parameter *Parameter `gorm:"foreignKey:ParameterID" json:"parameter,omitempty"`
}

// TableName specifies the table name for CurrentAlarm
func (CurrentAlarm) TableName() string {
	return "current_alarm"
}
