package repository

import (
	"aoi-backend/internal/core/database"
	"aoi-backend/internal/domain"
)

type FormulaRepository interface {
	GetByParameterID(parameterID uint) (*domain.ParameterFormula, error)
	GetAll() ([]domain.ParameterFormula, error)
	Create(formula *domain.ParameterFormula) error
	Update(formula *domain.ParameterFormula) error
	Delete(id uint) error
	GetVariablesByFormulaID(formulaID uint) ([]domain.ParameterFormulaVar, error)
	CreateVariable(var_ *domain.ParameterFormulaVar) error
	GetDependants(sourceParameterID uint) ([]uint, error) // Get all formula parameter IDs that depend on this source parameter
}

type formulaRepository struct{}

func NewFormulaRepository() FormulaRepository {
	return &formulaRepository{}
}

func (r *formulaRepository) GetByParameterID(parameterID uint) (*domain.ParameterFormula, error) {
	var formula domain.ParameterFormula
	err := database.GetDB().Preload("Variables").Where("parameter_id = ?", parameterID).First(&formula).Error
	if err != nil {
		return nil, err
	}
	return &formula, nil
}

func (r *formulaRepository) GetAll() ([]domain.ParameterFormula, error) {
	var formulas []domain.ParameterFormula
	err := database.GetDB().Preload("Variables").Preload("Variables.SourceParameter").Find(&formulas).Error
	return formulas, err
}

func (r *formulaRepository) Create(formula *domain.ParameterFormula) error {
	return database.GetDB().Create(formula).Error
}

func (r *formulaRepository) Update(formula *domain.ParameterFormula) error {
	return database.GetDB().Save(formula).Error
}

func (r *formulaRepository) Delete(id uint) error {
	return database.GetDB().Delete(&domain.ParameterFormula{}, id).Error
}

func (r *formulaRepository) GetVariablesByFormulaID(formulaID uint) ([]domain.ParameterFormulaVar, error) {
	var vars []domain.ParameterFormulaVar
	err := database.GetDB().Preload("SourceParameter").Where("formula_id = ?", formulaID).Find(&vars).Error
	return vars, err
}

func (r *formulaRepository) CreateVariable(var_ *domain.ParameterFormulaVar) error {
	return database.GetDB().Create(var_).Error
}

// GetDependants returns all formula parameter IDs that depend on the given source parameter
func (r *formulaRepository) GetDependants(sourceParameterID uint) ([]uint, error) {
	var formulaIDs []uint
	err := database.GetDB().Model(&domain.ParameterFormulaVar{}).
		Where("source_parameter_id = ?", sourceParameterID).
		Pluck("formula_id", &formulaIDs).Error
	
	if err != nil {
		return nil, err
	}

	// Get parameter IDs for these formulas
	var parameterIDs []uint
	if len(formulaIDs) > 0 {
		err = database.GetDB().Model(&domain.ParameterFormula{}).
			Where("id IN ?", formulaIDs).
			Pluck("parameter_id", &parameterIDs).Error
	}

	return parameterIDs, err
}

