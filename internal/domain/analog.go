package domain

import "time"

type AnalogInput struct {
	ID          uint      `gorm:"primaryKey" json:"id"`
	IDParameter uint      `gorm:"not null;index" json:"id_parameter"`
	Channel     int       `gorm:"not null" json:"channel"`
	RawMin      float64   `gorm:"default:0" json:"raw_min"`
	RawMax      float64   `gorm:"default:1024" json:"raw_max"`
	EngMin      float64   `gorm:"default:0" json:"eng_min"`
	EngMax      float64   `gorm:"default:1024" json:"eng_max"`
	Scaling     float64   `gorm:"default:1.0" json:"scaling"`
	PollingMs   int       `gorm:"not null" json:"polling_ms"`
	Description *string   `gorm:"size:255" json:"description,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
	Parameter   Parameter `gorm:"foreignKey:IDParameter" json:"parameter,omitempty"`
}

func (AnalogInput) TableName() string {
	return "analog_input"
}

type AnalogOutput struct {
	ID          uint      `gorm:"primaryKey" json:"id"`
	IDParameter uint      `gorm:"not null;index" json:"id_parameter"`
	Channel     int       `gorm:"not null" json:"channel"`
	RawMin      float64   `gorm:"default:0" json:"raw_min"`
	RawMax      float64   `gorm:"default:1024" json:"raw_max"`
	EngMin      float64   `gorm:"default:0" json:"eng_min"`
	EngMax      float64   `gorm:"default:1024" json:"eng_max"`
	Scaling     float64   `gorm:"default:1.0" json:"scaling"`
	Description *string   `gorm:"size:255" json:"description,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
	Parameter   Parameter `gorm:"foreignKey:IDParameter" json:"parameter,omitempty"`
}

func (AnalogOutput) TableName() string {
	return "analog_output"
}

