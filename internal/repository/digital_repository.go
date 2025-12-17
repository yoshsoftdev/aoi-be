package repository

import (
	"aoi-backend/internal/domain"
	"aoi-backend/internal/core/database"
	"gorm.io/gorm"
)

type DigitalInputRepository interface {
	Create(config *domain.DigitalInput) error
	GetByID(id uint) (*domain.DigitalInput, error)
	GetByParameterID(paramID uint) (*domain.DigitalInput, error)
	GetAll() ([]domain.DigitalInput, error)
	Update(config *domain.DigitalInput) error
	Delete(id uint) error
}

type DigitalOutputRepository interface {
	Create(config *domain.DigitalOutput) error
	GetByID(id uint) (*domain.DigitalOutput, error)
	GetByParameterID(paramID uint) (*domain.DigitalOutput, error)
	GetAll() ([]domain.DigitalOutput, error)
	Update(config *domain.DigitalOutput) error
	Delete(id uint) error
}

type digitalInputRepository struct {
	db *gorm.DB
}

type digitalOutputRepository struct {
	db *gorm.DB
}

func NewDigitalInputRepository() DigitalInputRepository {
	return &digitalInputRepository{
		db: database.GetDB(),
	}
}

func NewDigitalOutputRepository() DigitalOutputRepository {
	return &digitalOutputRepository{
		db: database.GetDB(),
	}
}

// Digital Input
func (r *digitalInputRepository) Create(config *domain.DigitalInput) error {
	return r.db.Create(config).Error
}

func (r *digitalInputRepository) GetByID(id uint) (*domain.DigitalInput, error) {
	var config domain.DigitalInput
	err := r.db.Preload("Parameter").First(&config, id).Error
	if err != nil {
		return nil, err
	}
	return &config, nil
}

func (r *digitalInputRepository) GetByParameterID(paramID uint) (*domain.DigitalInput, error) {
	var config domain.DigitalInput
	err := r.db.Preload("Parameter").Where("id_parameter = ?", paramID).First(&config).Error
	if err != nil {
		return nil, err
	}
	return &config, nil
}

func (r *digitalInputRepository) GetAll() ([]domain.DigitalInput, error) {
	var configs []domain.DigitalInput
	err := r.db.Preload("Parameter").Find(&configs).Error
	return configs, err
}

func (r *digitalInputRepository) Update(config *domain.DigitalInput) error {
	return r.db.Save(config).Error
}

func (r *digitalInputRepository) Delete(id uint) error {
	return r.db.Delete(&domain.DigitalInput{}, id).Error
}

// Digital Output
func (r *digitalOutputRepository) Create(config *domain.DigitalOutput) error {
	return r.db.Create(config).Error
}

func (r *digitalOutputRepository) GetByID(id uint) (*domain.DigitalOutput, error) {
	var config domain.DigitalOutput
	err := r.db.Preload("Parameter").First(&config, id).Error
	if err != nil {
		return nil, err
	}
	return &config, nil
}

func (r *digitalOutputRepository) GetByParameterID(paramID uint) (*domain.DigitalOutput, error) {
	var config domain.DigitalOutput
	err := r.db.Preload("Parameter").Where("id_parameter = ?", paramID).First(&config).Error
	if err != nil {
		return nil, err
	}
	return &config, nil
}

func (r *digitalOutputRepository) GetAll() ([]domain.DigitalOutput, error) {
	var configs []domain.DigitalOutput
	err := r.db.Preload("Parameter").Find(&configs).Error
	return configs, err
}

func (r *digitalOutputRepository) Update(config *domain.DigitalOutput) error {
	return r.db.Save(config).Error
}

func (r *digitalOutputRepository) Delete(id uint) error {
	return r.db.Delete(&domain.DigitalOutput{}, id).Error
}

