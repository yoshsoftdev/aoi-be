package repository

import (
	"aoi-backend/internal/core/database"
	"aoi-backend/internal/domain"
	"gorm.io/gorm"
)

type ThresholdRepository interface {
	Create(threshold *domain.Threshold) error
	GetByID(id uint) (*domain.Threshold, error)
	GetByParameterID(paramID uint) (*domain.Threshold, error)
	GetAll() ([]domain.Threshold, error)
	Update(threshold *domain.Threshold) error
	Delete(id uint) error
	DeleteByParameterID(paramID uint) error // Delete by parameter ID
	CreateOrUpdate(threshold *domain.Threshold) error // Upsert: create if not exists, update if exists
}

type thresholdRepository struct {
	db *gorm.DB
}

func NewThresholdRepository() ThresholdRepository {
	return &thresholdRepository{
		db: database.GetDB(),
	}
}

func (r *thresholdRepository) Create(threshold *domain.Threshold) error {
	return r.db.Create(threshold).Error
}

func (r *thresholdRepository) GetByID(id uint) (*domain.Threshold, error) {
	var threshold domain.Threshold
	err := r.db.Preload("Parameter").First(&threshold, id).Error
	if err != nil {
		return nil, err
	}
	return &threshold, nil
}

func (r *thresholdRepository) GetByParameterID(paramID uint) (*domain.Threshold, error) {
	var threshold domain.Threshold
	err := r.db.Preload("Parameter").Where("id_parameter = ?", paramID).First(&threshold).Error
	if err != nil {
		return nil, err
	}
	return &threshold, nil
}

func (r *thresholdRepository) GetAll() ([]domain.Threshold, error) {
	var thresholds []domain.Threshold
	err := r.db.Preload("Parameter").Find(&thresholds).Error
	return thresholds, err
}

func (r *thresholdRepository) Update(threshold *domain.Threshold) error {
	return r.db.Save(threshold).Error
}

func (r *thresholdRepository) Delete(id uint) error {
	return r.db.Delete(&domain.Threshold{}, id).Error
}

func (r *thresholdRepository) DeleteByParameterID(paramID uint) error {
	return r.db.Where("id_parameter = ?", paramID).Delete(&domain.Threshold{}).Error
}

// CreateOrUpdate creates a new threshold or updates existing one based on id_parameter
func (r *thresholdRepository) CreateOrUpdate(threshold *domain.Threshold) error {
	var existing domain.Threshold
	err := r.db.Where("id_parameter = ?", threshold.IDParameter).First(&existing).Error
	
	if err == gorm.ErrRecordNotFound {
		// Create new
		return r.db.Create(threshold).Error
	} else if err != nil {
		return err
	}
	
	// Update existing
	threshold.ID = existing.ID
	return r.db.Save(threshold).Error
}

