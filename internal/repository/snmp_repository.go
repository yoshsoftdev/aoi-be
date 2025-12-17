package repository

import (
	"aoi-backend/internal/domain"
	"aoi-backend/internal/core/database"
	"gorm.io/gorm"
)

type SNMPRepository interface {
	Create(config *domain.SNMPConfig) error
	GetByID(id uint) (*domain.SNMPConfig, error)
	GetByParameterID(paramID uint) (*domain.SNMPConfig, error)
	GetAll() ([]domain.SNMPConfig, error)
	Update(config *domain.SNMPConfig) error
	Delete(id uint) error
}

type snmpRepository struct {
	db *gorm.DB
}

func NewSNMPRepository() SNMPRepository {
	return &snmpRepository{
		db: database.GetDB(),
	}
}

func (r *snmpRepository) Create(config *domain.SNMPConfig) error {
	return r.db.Create(config).Error
}

func (r *snmpRepository) GetByID(id uint) (*domain.SNMPConfig, error) {
	var config domain.SNMPConfig
	err := r.db.Preload("Parameter").First(&config, id).Error
	if err != nil {
		return nil, err
	}
	return &config, nil
}

func (r *snmpRepository) GetByParameterID(paramID uint) (*domain.SNMPConfig, error) {
	var config domain.SNMPConfig
	err := r.db.Preload("Parameter").Where("id_parameter = ?", paramID).First(&config).Error
	if err != nil {
		return nil, err
	}
	return &config, nil
}

func (r *snmpRepository) GetAll() ([]domain.SNMPConfig, error) {
	var configs []domain.SNMPConfig
	err := r.db.Preload("Parameter").Find(&configs).Error
	return configs, err
}

func (r *snmpRepository) Update(config *domain.SNMPConfig) error {
	return r.db.Save(config).Error
}

func (r *snmpRepository) Delete(id uint) error {
	return r.db.Delete(&domain.SNMPConfig{}, id).Error
}

