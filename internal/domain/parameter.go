package domain

import "time"

type Parameter struct {
	ID            uint      `gorm:"primaryKey" json:"id"`
	DeviceID      uint      `gorm:"not null;index" json:"device_id"`
	Nama          string    `gorm:"not null" json:"nama"`
	Type          string    `gorm:"not null" json:"type"`                                          // modbus_rtu, modbus_tcp, analog_input, analog_output, snmp, digital_input, digital_output, formula, internal
	Direction     string    `gorm:"type:varchar(10);default:'READ';index" json:"direction"`        // READ (polling/scheduler) or WRITE (command/on-demand)
	ReadWrite     int       `gorm:"default:0" json:"read_write"`                                  // 0=READ, 1=WRITE, 2=READ_WRITE (both polling and writable)
	ParameterMode string    `gorm:"type:varchar(20);default:'DIRECT';index" json:"parameter_mode"` // SOURCE, DIRECT, CLEAN, FORMULA
	WriteMode     *string   `gorm:"type:varchar(10);index" json:"write_mode,omitempty"`            // PRESET (value stored) or DYNAMIC (value provided at write time) - only for WRITE parameters
	PresetValue   *string   `gorm:"type:text" json:"preset_value,omitempty"`                       // JSON-encoded preset value - only for PRESET mode
	Unit          *string   `gorm:"type:varchar(20)" json:"unit,omitempty"`
	CustomRumus   *string   `gorm:"type:text" json:"custom_rumus,omitempty"`
	IsFormula     bool      `gorm:"default:false;index" json:"is_formula"` // true = derived parameter (formula), false = raw parameter
	Description   *string   `gorm:"type:varchar(255)" json:"description,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`

	// Relationships
	Device  Koneksi           `gorm:"foreignKey:DeviceID" json:"device,omitempty"`
	Formula *ParameterFormula `gorm:"foreignKey:ParameterID" json:"formula,omitempty"`
}

// ParameterMode constants
const (
	ParameterModeSOURCE  = "SOURCE"  // Polling device, internal only, tidak dibroadcast
	ParameterModeDIRECT  = "DIRECT"  // Polling device, nilai sudah clean, dibroadcast langsung
	ParameterModeCLEAN   = "CLEAN"   // Logical parameter, tidak polling, dibroadcast, formula OPTIONAL
	ParameterModeFORMULA = "FORMULA" // Derived parameter, tidak polling, dibroadcast
)

func (Parameter) TableName() string {
	return "parameters"
}
