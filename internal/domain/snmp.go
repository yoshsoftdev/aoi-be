package domain

import "time"

type SNMPConfig struct {
	ID          uint      `gorm:"primaryKey" json:"id"`
	IDParameter uint      `gorm:"not null;index" json:"id_parameter"`
	Version     string    `gorm:"size:10;default:'v2c'" json:"version"` // v1, v2c, v3
	Community   string    `gorm:"size:100;not null" json:"community"`
	OID         string    `gorm:"size:200;not null" json:"oid"`
	IPAddress   string    `gorm:"size:50;not null" json:"ip_address"`
	Port        int       `gorm:"default:161" json:"port"`
	PollingMs   int       `gorm:"not null" json:"polling_ms"`
	Description *string   `gorm:"size:255" json:"description,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
	Parameter   Parameter `gorm:"foreignKey:IDParameter" json:"parameter,omitempty"`
}

func (SNMPConfig) TableName() string {
	return "snmp_config"
}

// Keep SNMP as alias for backward compatibility during migration
type SNMP = SNMPConfig

