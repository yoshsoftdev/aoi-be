package domain

import "time"

type Koneksi struct {
	ID          uint    `gorm:"primaryKey" json:"id"`
	Nama        string  `gorm:"not null" json:"nama"`
	Type        string  `gorm:"not null" json:"type"` // modbus_rtu, modbus_tcp, snmp, analog, digital
	Enabled     bool    `gorm:"default:true" json:"enabled"`
	Description *string `gorm:"type:varchar(255)" json:"description,omitempty"`

	// Modbus RTU Configuration
	Port     *string `gorm:"type:varchar(50)" json:"port,omitempty"`
	Baud     *int    `json:"baud,omitempty"`
	DataBits *int    `json:"data_bits,omitempty"`
	Parity   *string `gorm:"type:varchar(1)" json:"parity,omitempty"`
	StopBits *int    `json:"stop_bits,omitempty"`

	// Modbus TCP Configuration
	IPAddress *string `gorm:"type:varchar(50)" json:"ip_address,omitempty"`
	TCPPort   *int    `json:"tcp_port,omitempty"`

	// SNMP Configuration (for SNMP type connections)
	Community *string `gorm:"type:varchar(100)" json:"community,omitempty"` // SNMP community string
	Version   *string `gorm:"type:varchar(10)" json:"version,omitempty"`    // SNMP version: v1, v2c, v3

	// Common Modbus Settings
	SlaveID *int `json:"slave_id,omitempty"`

	// Polling Configuration
	PollingMs  int `gorm:"default:1000" json:"polling_ms"`
	TimeoutMs  int `gorm:"default:1000" json:"timeout_ms"`
	RetryCount int `gorm:"default:3" json:"retry_count"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`

	// Relationships
	Parameters []Parameter `gorm:"foreignKey:DeviceID" json:"parameters,omitempty"`
}

// TableName specifies the table name for Koneksi
func (Koneksi) TableName() string {
	return "koneksi"
}

// Device is an alias for Koneksi for backward compatibility during migration
// Deprecated: Use Koneksi instead
type Device = Koneksi
