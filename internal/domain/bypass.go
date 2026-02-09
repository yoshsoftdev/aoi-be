package domain

import "time"

// Bypass adalah workflow ringan untuk kontrol manual / override.
// Tidak menggantikan workflow utama, tapi dieksekusi oleh engine terpisah.

// Bypass merepresentasikan header / definisi rule bypass.
type Bypass struct {
	ID          uint      `gorm:"primaryKey" json:"id"`
	Name        string    `gorm:"size:100;not null" json:"name"`
	Enabled     bool      `gorm:"not null;default:1" json:"enabled"`
	Description *string   `gorm:"type:text" json:"description,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`

	// Relations
	Rules   []BypassRule   `gorm:"foreignKey:BypassID;constraint:OnDelete:CASCADE" json:"rules,omitempty"`
	Actions []BypassAction `gorm:"foreignKey:BypassID;constraint:OnDelete:CASCADE" json:"actions,omitempty"`
}

func (Bypass) TableName() string {
	return "bypass"
}

// BypassRule mendeskripsikan kondisi IF pada bypass.
type BypassRule struct {
	ID            uint   `gorm:"primaryKey" json:"id"`
	BypassID      uint   `gorm:"not null;index" json:"bypass_id"`
	ConditionType string `gorm:"size:20;not null" json:"condition_type"` // 'always' | 'compare'

	LeftSourceType string `gorm:"size:20;not null" json:"left_source_type"` // 'set_point' | 'parameter'
	LeftSourceID   uint   `gorm:"not null" json:"left_source_id"`

	Operator       string   `gorm:"size:5" json:"operator"`          // >, <, =, >=, <= (untuk compare)
	RightValueType string   `gorm:"size:20" json:"right_value_type"` // 'static' | 'set_point'
	RightValue     *float64 `gorm:"type:REAL" json:"right_value,omitempty"`
	RightSourceID  *uint    `json:"right_source_id,omitempty"`

	CreatedAt time.Time `json:"created_at"`

	// Relations untuk kemudahan akses (tanpa FK di level GORM, validasi di application layer)
	// Left set-point / parameter
	LeftSetPoint  *SetPoint  `gorm:"-" json:"left_set_point,omitempty"`
	LeftParameter *Parameter `gorm:"-" json:"left_parameter,omitempty"`
	// Right set-point (jika right_value_type = 'set_point')
	RightSetPoint *SetPoint `gorm:"-" json:"right_set_point,omitempty"`
}

func (BypassRule) TableName() string {
	return "bypass_rule"
}

// BypassAction mendeskripsikan aksi THEN WRITE pada bypass.
// Bisa menulis ke parameter ATAU set-point (salah satu harus diisi).
type BypassAction struct {
	ID               uint  `gorm:"primaryKey" json:"id"`
	BypassID         uint  `gorm:"not null;index" json:"bypass_id"`
	WriteParameterID uint  `gorm:"index;default:0" json:"write_parameter_id"` // Optional: jika menulis ke parameter (0 = tidak digunakan)
	WriteSetPointID  *uint `gorm:"index" json:"write_set_point_id,omitempty"` // Optional: jika menulis ke set-point

	ValueSourceType  string   `gorm:"size:20;not null" json:"value_source_type"` // 'static' | 'set_point'
	Value            *float64 `gorm:"type:REAL" json:"value,omitempty"`
	SourceSetPointID *uint    `json:"source_set_point_id,omitempty"`

	ActionOrder int       `gorm:"default:0;index" json:"action_order"`
	CreatedAt   time.Time `json:"created_at"`

	// Relations (tanpa FK otomatis ke set_point, parameter sudah punya FK di schema)
	WriteParameter *Parameter `gorm:"foreignKey:WriteParameterID" json:"write_parameter,omitempty"`
	WriteSetPoint  *SetPoint  `gorm:"-" json:"write_set_point,omitempty"`
	SourceSetPoint *SetPoint  `gorm:"-" json:"source_set_point,omitempty"`
}

func (BypassAction) TableName() string {
	return "bypass_action"
}
