package domain

import "time"

// SetPoint merepresentasikan nilai state yang bisa diatur user dari UI,
// tidak terhubung langsung ke device / parameter.
type SetPoint struct {
	ID          uint      `gorm:"primaryKey" json:"id"`
	Name        string    `gorm:"size:100;not null" json:"name"`
	Unit        *string   `gorm:"size:20" json:"unit,omitempty"`
	Value       float64   `gorm:"type:REAL;not null" json:"value"`
	DataType    string    `gorm:"size:10;not null" json:"data_type"` // 'int', 'float', 'bool'
	MinValue    *float64  `gorm:"type:REAL" json:"min_value,omitempty"`
	MaxValue    *float64  `gorm:"type:REAL" json:"max_value,omitempty"`
	Description *string   `gorm:"type:text" json:"description,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

func (SetPoint) TableName() string {
	return "set_point"
}

