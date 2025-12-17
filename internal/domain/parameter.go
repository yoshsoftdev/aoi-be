package domain

import "time"

type Parameter struct {
	ID          uint      `gorm:"primaryKey" json:"id"`
	DeviceID    uint      `gorm:"not null;index" json:"device_id"`
	Nama        string    `gorm:"not null" json:"nama"`
	Type        string    `gorm:"not null" json:"type"` // modbus_rtu, modbus_tcp, analog_input, analog_output, snmp, digital_input, digital_output
	ReadWrite   bool      `gorm:"default:false" json:"read_write"`
	Unit        *string   `gorm:"type:varchar(20)" json:"unit,omitempty"`
	CustomRumus *string   `gorm:"type:text" json:"custom_rumus,omitempty"`
	Description *string   `gorm:"type:varchar(255)" json:"description,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`

	// Relationships
	Device Koneksi `gorm:"foreignKey:DeviceID" json:"device,omitempty"`
}

func (Parameter) TableName() string {
	return "parameters"
}
