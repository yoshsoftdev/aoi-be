package domain

import "time"

// IMCParameterConfig stores the mapping between IMC card keys and parameter IDs
type IMCParameterConfig struct {
	ID          uint      `gorm:"primaryKey" json:"id"`
	CardKey     string    `gorm:"size:100;not null;uniqueIndex" json:"card_key"`
	SourceType  string    `gorm:"size:20;not null;default:'parameter'" json:"source_type"`
	ParameterID *uint     `gorm:"index" json:"parameter_id,omitempty"`
	SetPointID  *uint     `gorm:"index" json:"set_point_id,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`

	// Relations
	Parameter *Parameter `gorm:"foreignKey:ParameterID" json:"parameter,omitempty"`
	SetPoint  *SetPoint  `gorm:"foreignKey:SetPointID" json:"set_point,omitempty"`
}

func (IMCParameterConfig) TableName() string {
	return "imc_parameter_config"
}
