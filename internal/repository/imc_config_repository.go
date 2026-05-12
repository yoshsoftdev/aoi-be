package repository

import (
	"aoi-backend/internal/core/database"
	"aoi-backend/internal/domain"
	"errors"
	"gorm.io/gorm"
)

type IMCConfigRepository interface {
	GetAll() ([]domain.IMCParameterConfig, error)
	GetByCardKey(cardKey string) (*domain.IMCParameterConfig, error)
	CreateOrUpdate(config *domain.IMCParameterConfig) error
	Delete(cardKey string) error
}

type imcConfigRepository struct {
	db *gorm.DB
}

func NewIMCConfigRepository() IMCConfigRepository {
	return &imcConfigRepository{
		db: database.GetDB(),
	}
}

func (r *imcConfigRepository) GetAll() ([]domain.IMCParameterConfig, error) {
	var configs []domain.IMCParameterConfig
	if err := r.db.Preload("Parameter").Preload("SetPoint").Find(&configs).Error; err != nil {
		return nil, err
	}
	return configs, nil
}

func (r *imcConfigRepository) GetByCardKey(cardKey string) (*domain.IMCParameterConfig, error) {
	var config domain.IMCParameterConfig
	if err := r.db.Preload("Parameter").Preload("SetPoint").Where("card_key = ?", cardKey).First(&config).Error; err != nil {
		return nil, err
	}
	return &config, nil
}

func (r *imcConfigRepository) CreateOrUpdate(config *domain.IMCParameterConfig) error {
	var existing domain.IMCParameterConfig
	err := r.db.Where("card_key = ?", config.CardKey).First(&existing).Error
	
	if err != nil {
		// Not found, create new
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return r.db.Create(config).Error
		}
		return err
	}
	
	// Update existing
	existing.SourceType = config.SourceType
	existing.ParameterID = config.ParameterID
	existing.SetPointID = config.SetPointID
	return r.db.Save(&existing).Error
}

func (r *imcConfigRepository) Delete(cardKey string) error {
	return r.db.Where("card_key = ?", cardKey).Delete(&domain.IMCParameterConfig{}).Error
}
