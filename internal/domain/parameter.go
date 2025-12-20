package domain

import "time"

type Parameter struct {
	ID          uint      `gorm:"primaryKey" json:"id"`
	DeviceID    uint      `gorm:"not null;index" json:"device_id"`
	Nama        string    `gorm:"not null" json:"nama"`
	Type        string    `gorm:"not null" json:"type"`                                   // modbus_rtu, modbus_tcp, analog_input, analog_output, snmp, digital_input, digital_output, formula
	Direction   string    `gorm:"type:varchar(10);default:'READ';index" json:"direction"` // READ (polling/scheduler) or WRITE (command/on-demand)
	ReadWrite   bool      `gorm:"default:false" json:"read_write"`                        // Deprecated: use Direction instead
	Unit        *string   `gorm:"type:varchar(20)" json:"unit,omitempty"`
	CustomRumus *string   `gorm:"type:text" json:"custom_rumus,omitempty"`
	IsFormula   bool      `gorm:"default:false;index" json:"is_formula"` // true = derived parameter (formula), false = raw parameter
	Description *string   `gorm:"type:varchar(255)" json:"description,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`

	// Relationships
	Device  Koneksi           `gorm:"foreignKey:DeviceID" json:"device,omitempty"`
	Formula *ParameterFormula `gorm:"foreignKey:ParameterID" json:"formula,omitempty"`
}

func (Parameter) TableName() string {
	return "parameters"
}
