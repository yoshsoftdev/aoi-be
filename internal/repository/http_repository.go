package repository

import (
	"aoi-backend/internal/domain"
	"aoi-backend/internal/core/database"
	"gorm.io/gorm"
)

type HTTPRepository interface {
	Create(config *domain.HTTPConfig) error
	GetByID(id uint) (*domain.HTTPConfig, error)
	GetByParameterID(paramID uint) (*domain.HTTPConfig, error)
	GetAll() ([]domain.HTTPConfig, error)
	Update(config *domain.HTTPConfig) error
	Delete(id uint) error
}

type httpRepository struct {
	db *gorm.DB
}

func NewHTTPRepository() HTTPRepository {
	return &httpRepository{
		db: database.GetDB(),
	}
}

func (r *httpRepository) Create(config *domain.HTTPConfig) error {
	return r.db.Create(config).Error
}

func (r *httpRepository) GetByID(id uint) (*domain.HTTPConfig, error) {
	var config domain.HTTPConfig
	err := r.db.Preload("Parameter").First(&config, id).Error
	if err != nil {
		return nil, err
	}
	return &config, nil
}

func (r *httpRepository) GetByParameterID(paramID uint) (*domain.HTTPConfig, error) {
	var config domain.HTTPConfig
	err := r.db.Preload("Parameter").Where("id_parameter = ?", paramID).First(&config).Error
	if err != nil {
		return nil, err
	}
	return &config, nil
}

func (r *httpRepository) GetAll() ([]domain.HTTPConfig, error) {
	var configs []domain.HTTPConfig
	err := r.db.Preload("Parameter").Find(&configs).Error
	return configs, err
}

func (r *httpRepository) Update(config *domain.HTTPConfig) error {
	return r.db.Save(config).Error
}

func (r *httpRepository) Delete(id uint) error {
	return r.db.Delete(&domain.HTTPConfig{}, id).Error
}
