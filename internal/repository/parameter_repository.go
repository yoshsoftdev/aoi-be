package repository

import (
	"aoi-backend/internal/domain"
	"aoi-backend/internal/core/database"
	"gorm.io/gorm"
)

type ParameterRepository interface {
	Create(param *domain.Parameter) error
	GetByID(id uint) (*domain.Parameter, error)
	GetAll() ([]domain.Parameter, error)
	GetByDeviceID(deviceID uint) ([]domain.Parameter, error)
	Update(param *domain.Parameter) error
	Delete(id uint) error
}

type parameterRepository struct {
	db *gorm.DB
}

func NewParameterRepository() ParameterRepository {
	return &parameterRepository{
		db: database.GetDB(),
	}
}

func (r *parameterRepository) Create(param *domain.Parameter) error {
	return r.db.Create(param).Error
}

func (r *parameterRepository) GetByID(id uint) (*domain.Parameter, error) {
	var param domain.Parameter
	err := r.db.Preload("Device").First(&param, id).Error
	if err != nil {
		return nil, err
	}
	return &param, nil
}

func (r *parameterRepository) GetAll() ([]domain.Parameter, error) {
	var params []domain.Parameter
	// Use Find without any conditions to get all records
	// Explicitly remove any limit that might be set
	err := r.db.Preload("Device").Limit(-1).Find(&params).Error
	if err != nil {
		return nil, err
	}
	return params, nil
}

func (r *parameterRepository) GetByDeviceID(deviceID uint) ([]domain.Parameter, error) {
	var params []domain.Parameter
	err := r.db.Where("device_id = ?", deviceID).Preload("Device").Find(&params).Error
	return params, err
}

func (r *parameterRepository) Update(param *domain.Parameter) error {
	return r.db.Save(param).Error
}

func (r *parameterRepository) Delete(id uint) error {
	return r.db.Delete(&domain.Parameter{}, id).Error
}

