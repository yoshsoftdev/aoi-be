package domain

import "time"

type DigitalInput struct {
	ID          uint      `gorm:"primaryKey" json:"id"`
	IDParameter uint      `gorm:"not null;index" json:"id_parameter"`
	Channel     int       `gorm:"not null" json:"channel"`
	InvertLogic bool      `gorm:"default:false" json:"invert_logic"`
	PollingMs   int       `gorm:"not null" json:"polling_ms"`
	Description *string   `gorm:"size:255" json:"description,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
	Parameter   Parameter `gorm:"foreignKey:IDParameter" json:"parameter,omitempty"`
}

func (DigitalInput) TableName() string {
	return "digital_input"
}

type DigitalOutput struct {
	ID          uint      `gorm:"primaryKey" json:"id"`
	IDParameter uint      `gorm:"not null;index" json:"id_parameter"`
	Channel     int       `gorm:"not null" json:"channel"`
	InvertLogic bool      `gorm:"default:false" json:"invert_logic"`
	Description *string   `gorm:"size:255" json:"description,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
	Parameter   Parameter `gorm:"foreignKey:IDParameter" json:"parameter,omitempty"`
}

func (DigitalOutput) TableName() string {
	return "digital_output"
}

