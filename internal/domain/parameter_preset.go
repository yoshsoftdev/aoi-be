package domain

import "time"

// ParameterPreset represents a preset value for write operations
type ParameterPreset struct {
	ID          uint      `gorm:"primaryKey" json:"id"`
	ParameterID uint      `gorm:"not null;index" json:"parameter_id"`
	Name        string    `gorm:"not null;size:50" json:"name"`       // START / STOP / ON / OFF
	ValueType   string    `gorm:"not null;size:20" json:"value_type"` // scalar | bitmask | raw_register
	Value       string    `gorm:"type:text;not null" json:"value"`    // JSON format
	Description *string   `gorm:"type:varchar(255)" json:"description,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`

	// Relationships
	Parameter Parameter `gorm:"foreignKey:ParameterID" json:"parameter,omitempty"`
}

func (ParameterPreset) TableName() string {
	return "parameter_preset"
}

// PresetValueScalar represents scalar preset value
type PresetValueScalar struct {
	Value float64 `json:"value"`
}

// PresetValueBitmask represents bitmask preset value (multi-register)
type PresetValueBitmask struct {
	RegisterValues []int `json:"register_values"`
}

// PresetValueRawRegister represents raw register preset value
type PresetValueRawRegister struct {
	RegisterValues []int `json:"register_values"`
}
