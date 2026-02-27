package domain

import "time"

// HTTPConfig represents configuration for HTTP GET parameter read
type HTTPConfig struct {
	ID          uint      `gorm:"primaryKey" json:"id"`
	IDParameter uint      `gorm:"not null;index" json:"id_parameter"`
	URL         string    `gorm:"type:text;not null" json:"url"`
	Method      string    `gorm:"type:varchar(10);default:'GET'" json:"method"` // GET, POST, etc.
	
	// Authentication
	Username *string `gorm:"type:varchar(255)" json:"username,omitempty"`
	Password *string `gorm:"type:varchar(255)" json:"password,omitempty"` // Should be encrypted in production
	
	// HTTP Headers (JSON format: {"Header-Name": "value"})
	Headers *string `gorm:"type:text" json:"headers,omitempty"`
	
	// Timeout in milliseconds
	TimeoutMs int `gorm:"default:5000" json:"timeout_ms"`
	
	// Polling interval in milliseconds
	PollingMs int `gorm:"not null" json:"polling_ms"`
	
	// JSON Field Mappings (JSON format: {"id": "id", "name": "name", "value": "temperature", "timestamp": "timestamp"})
	// Maps internal fields (id, name, value, timestamp) to JSON response fields
	FieldMappings string `gorm:"type:text;not null" json:"field_mappings"`
	
	Description *string   `gorm:"type:varchar(255)" json:"description,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
	Parameter   Parameter `gorm:"foreignKey:IDParameter" json:"parameter,omitempty"`
}

func (HTTPConfig) TableName() string {
	return "http_config"
}
