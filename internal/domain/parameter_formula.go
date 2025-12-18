package domain

import "time"

// ParameterFormula represents a formula/derived parameter
type ParameterFormula struct {
	ID            uint      `gorm:"primaryKey" json:"id"`
	ParameterID   uint      `gorm:"not null;index" json:"parameter_id"`
	Expression    string    `gorm:"type:text;not null" json:"expression"` // Normalized expression like "(a + b) / 2"
	ExpressionType string   `gorm:"type:varchar(20);default:'math'" json:"expression_type"` // math, expr, js
	Description   *string   `gorm:"type:varchar(255)" json:"description,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`

	// Relationships
	Parameter Parameter              `gorm:"foreignKey:ParameterID" json:"parameter,omitempty"`
	Variables []ParameterFormulaVar  `gorm:"foreignKey:FormulaID" json:"variables,omitempty"`
}

func (ParameterFormula) TableName() string {
	return "parameter_formula"
}

// ParameterFormulaVar maps formula variables to source parameters
type ParameterFormulaVar struct {
	ID                uint      `gorm:"primaryKey" json:"id"`
	FormulaID         uint      `gorm:"not null;index" json:"formula_id"`
	VarName           string    `gorm:"type:varchar(20);not null" json:"var_name"` // a, b, c, etc.
	SourceParameterID uint      `gorm:"not null;index" json:"source_parameter_id"` // ID of the parameter this variable refers to
	CreatedAt         time.Time `json:"created_at"`

	// Relationships
	Formula         ParameterFormula `gorm:"foreignKey:FormulaID" json:"formula,omitempty"`
	SourceParameter Parameter        `gorm:"foreignKey:SourceParameterID" json:"source_parameter,omitempty"`
}

func (ParameterFormulaVar) TableName() string {
	return "parameter_formula_var"
}

