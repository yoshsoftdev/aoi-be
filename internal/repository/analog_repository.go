package repository

import (
	"aoi-backend/internal/domain"
	"aoi-backend/internal/core/database"
	"gorm.io/gorm"
)

type AnalogInputRepository interface {
	Create(config *domain.AnalogInput) error
	GetByID(id uint) (*domain.AnalogInput, error)
	GetByParameterID(paramID uint) (*domain.AnalogInput, error)
	GetAll() ([]domain.AnalogInput, error)
	Update(config *domain.AnalogInput) error
	Delete(id uint) error
}

type AnalogOutputRepository interface {
	Create(config *domain.AnalogOutput) error
	GetByID(id uint) (*domain.AnalogOutput, error)
	GetByParameterID(paramID uint) (*domain.AnalogOutput, error)
	GetAll() ([]domain.AnalogOutput, error)
	Update(config *domain.AnalogOutput) error
	Delete(id uint) error
}

type analogInputRepository struct {
	db *gorm.DB
}

type analogOutputRepository struct {
	db *gorm.DB
}

func NewAnalogInputRepository() AnalogInputRepository {
	return &analogInputRepository{
		db: database.GetDB(),
	}
}

func NewAnalogOutputRepository() AnalogOutputRepository {
	return &analogOutputRepository{
		db: database.GetDB(),
	}
}

// Analog Input
func (r *analogInputRepository) Create(config *domain.AnalogInput) error {
	return r.db.Create(config).Error
}

func (r *analogInputRepository) GetByID(id uint) (*domain.AnalogInput, error) {
	var config domain.AnalogInput
	err := r.db.Preload("Parameter").First(&config, id).Error
	if err != nil {
		return nil, err
	}
	return &config, nil
}

func (r *analogInputRepository) GetByParameterID(paramID uint) (*domain.AnalogInput, error) {
	var config domain.AnalogInput
	err := r.db.Preload("Parameter").Where("id_parameter = ?", paramID).First(&config).Error
	if err != nil {
		return nil, err
	}
	return &config, nil
}

func (r *analogInputRepository) GetAll() ([]domain.AnalogInput, error) {
	var configs []domain.AnalogInput
	err := r.db.Preload("Parameter").Find(&configs).Error
	return configs, err
}

func (r *analogInputRepository) Update(config *domain.AnalogInput) error {
	return r.db.Save(config).Error
}

func (r *analogInputRepository) Delete(id uint) error {
	return r.db.Delete(&domain.AnalogInput{}, id).Error
}

// Analog Output
func (r *analogOutputRepository) Create(config *domain.AnalogOutput) error {
	return r.db.Create(config).Error
}

func (r *analogOutputRepository) GetByID(id uint) (*domain.AnalogOutput, error) {
	var config domain.AnalogOutput
	err := r.db.Preload("Parameter").First(&config, id).Error
	if err != nil {
		return nil, err
	}
	return &config, nil
}

func (r *analogOutputRepository) GetByParameterID(paramID uint) (*domain.AnalogOutput, error) {
	var config domain.AnalogOutput
	err := r.db.Preload("Parameter").Where("id_parameter = ?", paramID).First(&config).Error
	if err != nil {
		return nil, err
	}
	return &config, nil
}

func (r *analogOutputRepository) GetAll() ([]domain.AnalogOutput, error) {
	var configs []domain.AnalogOutput
	err := r.db.Preload("Parameter").Find(&configs).Error
	return configs, err
}

func (r *analogOutputRepository) Update(config *domain.AnalogOutput) error {
	return r.db.Save(config).Error
}

func (r *analogOutputRepository) Delete(id uint) error {
	return r.db.Delete(&domain.AnalogOutput{}, id).Error
}

