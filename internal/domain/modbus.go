package domain

import "time"

type ModbusRTU struct {
	ID              uint      `gorm:"primaryKey" json:"id"`
	IDParameter     uint      `gorm:"not null;index" json:"id_parameter"`
	ReadWrite       bool      `gorm:"default:false" json:"read_write"`
	SlaveID         int       `gorm:"not null" json:"slave_id"` // Parameter-level slave ID (can override device-level)
	FunctionCode    int       `gorm:"not null" json:"function_code"`
	RegisterAddress int       `gorm:"not null" json:"register_address"`
	Quantity        int       `gorm:"not null" json:"quantity"`
	ByteOrder       string    `gorm:"size:20;not null" json:"byte_order"`
	DataType        string    `gorm:"size:20;default:'uint16'" json:"data_type"` // uint16, int32, float32, float64
	Scaling         float64   `gorm:"default:1.0" json:"scaling"`
	PollingMs       *int      `json:"polling_ms,omitempty"` // Optional: override device-level polling
	Description     *string   `gorm:"size:255" json:"description,omitempty"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
	Parameter       Parameter `gorm:"foreignKey:IDParameter" json:"parameter,omitempty"`
}

func (ModbusRTU) TableName() string {
	return "modbus_rtu"
}

type ModbusTCP struct {
	ID              uint      `gorm:"primaryKey" json:"id"`
	IDParameter     uint      `gorm:"not null;index" json:"id_parameter"`
	ReadWrite       bool      `gorm:"default:false" json:"read_write"`
	SlaveID         int       `gorm:"not null" json:"slave_id"` // Parameter-level slave ID (can override device-level)
	FunctionCode    int       `gorm:"not null" json:"function_code"`
	RegisterAddress int       `gorm:"not null" json:"register_address"`
	Quantity        int       `gorm:"not null" json:"quantity"`
	ByteOrder       string    `gorm:"size:20;not null" json:"byte_order"`
	DataType        string    `gorm:"size:20;default:'uint16'" json:"data_type"` // uint16, int32, float32, float64
	Scaling         float64   `gorm:"default:1.0" json:"scaling"`
	PollingMs       *int      `json:"polling_ms,omitempty"` // Optional: override device-level polling
	Description     *string   `gorm:"size:255" json:"description,omitempty"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
	Parameter       Parameter `gorm:"foreignKey:IDParameter" json:"parameter,omitempty"`
}

func (ModbusTCP) TableName() string {
	return "modbus_tcp"
}

