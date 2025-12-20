package repository

import (
	"aoi-backend/internal/core/database"
	"aoi-backend/internal/domain"

	"gorm.io/gorm"
)

type PresetRepository interface {
	Create(preset *domain.ParameterPreset) error
	GetByID(id uint) (*domain.ParameterPreset, error)
	GetByParameterID(parameterID uint) ([]domain.ParameterPreset, error)
	GetByName(parameterID uint, name string) (*domain.ParameterPreset, error)
	Update(preset *domain.ParameterPreset) error
	Delete(id uint) error
}

type presetRepository struct {
	db *gorm.DB
}

func NewPresetRepository() PresetRepository {
	return &presetRepository{
		db: database.GetDB(),
	}
}

func (r *presetRepository) Create(preset *domain.ParameterPreset) error {
	return r.db.Create(preset).Error
}

func (r *presetRepository) GetByID(id uint) (*domain.ParameterPreset, error) {
	var preset domain.ParameterPreset
	err := r.db.Preload("Parameter").First(&preset, id).Error
	if err != nil {
		return nil, err
	}
	return &preset, nil
}

func (r *presetRepository) GetByParameterID(parameterID uint) ([]domain.ParameterPreset, error) {
	var presets []domain.ParameterPreset
	err := r.db.Where("parameter_id = ?", parameterID).Order("name ASC").Find(&presets).Error
	return presets, err
}

func (r *presetRepository) GetByName(parameterID uint, name string) (*domain.ParameterPreset, error) {
	var preset domain.ParameterPreset
	err := r.db.Where("parameter_id = ? AND name = ?", parameterID, name).First(&preset).Error
	if err != nil {
		return nil, err
	}
	return &preset, nil
}

func (r *presetRepository) Update(preset *domain.ParameterPreset) error {
	return r.db.Save(preset).Error
}

func (r *presetRepository) Delete(id uint) error {
	return r.db.Delete(&domain.ParameterPreset{}, id).Error
}
