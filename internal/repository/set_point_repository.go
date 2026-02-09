package repository

import (
	"aoi-backend/internal/core/database"
	"aoi-backend/internal/domain"

	"gorm.io/gorm"
)

// SetPointRepository mengelola operasi DB untuk set_point.
type SetPointRepository interface {
	GetAll() ([]domain.SetPoint, error)
	GetByID(id uint) (*domain.SetPoint, error)
	Create(sp *domain.SetPoint) error
	Update(sp *domain.SetPoint) error
	Delete(id uint) error
}

type setPointRepository struct {
	db *gorm.DB
}

func NewSetPointRepository() SetPointRepository {
	return &setPointRepository{
		db: database.GetDB(),
	}
}

func (r *setPointRepository) GetAll() ([]domain.SetPoint, error) {
	var sps []domain.SetPoint
	if err := r.db.Find(&sps).Error; err != nil {
		return nil, err
	}
	return sps, nil
}

func (r *setPointRepository) GetByID(id uint) (*domain.SetPoint, error) {
	var sp domain.SetPoint
	if err := r.db.First(&sp, id).Error; err != nil {
		return nil, err
	}
	return &sp, nil
}

func (r *setPointRepository) Create(sp *domain.SetPoint) error {
	return r.db.Create(sp).Error
}

func (r *setPointRepository) Update(sp *domain.SetPoint) error {
	return r.db.Save(sp).Error
}

func (r *setPointRepository) Delete(id uint) error {
	return r.db.Delete(&domain.SetPoint{}, id).Error
}

