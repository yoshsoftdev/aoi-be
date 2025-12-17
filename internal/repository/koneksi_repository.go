package repository

import (
	"aoi-backend/internal/core/database"
	"aoi-backend/internal/domain"

	"gorm.io/gorm"
)

type KoneksiRepository interface {
	Create(koneksi *domain.Koneksi) error
	GetByID(id uint) (*domain.Koneksi, error)
	GetAll() ([]domain.Koneksi, error)
	GetByType(koneksiType string) ([]domain.Koneksi, error)
	GetEnabled() ([]domain.Koneksi, error)
	Update(koneksi *domain.Koneksi) error
	Delete(id uint) error
}

type koneksiRepository struct {
	db *gorm.DB
}

func NewKoneksiRepository() KoneksiRepository {
	return &koneksiRepository{
		db: database.GetDB(),
	}
}

func (r *koneksiRepository) Create(koneksi *domain.Koneksi) error {
	return r.db.Create(koneksi).Error
}

func (r *koneksiRepository) GetByID(id uint) (*domain.Koneksi, error) {
	var koneksi domain.Koneksi
	err := r.db.Preload("Parameters").First(&koneksi, id).Error
	if err != nil {
		return nil, err
	}
	return &koneksi, nil
}

func (r *koneksiRepository) GetAll() ([]domain.Koneksi, error) {
	var koneksis []domain.Koneksi
	err := r.db.Preload("Parameters").Find(&koneksis).Error
	return koneksis, err
}

func (r *koneksiRepository) GetByType(koneksiType string) ([]domain.Koneksi, error) {
	var koneksis []domain.Koneksi
	err := r.db.Where("type = ?", koneksiType).Preload("Parameters").Find(&koneksis).Error
	return koneksis, err
}

func (r *koneksiRepository) GetEnabled() ([]domain.Koneksi, error) {
	var koneksis []domain.Koneksi
	err := r.db.Where("enabled = ?", true).Preload("Parameters").Find(&koneksis).Error
	return koneksis, err
}

func (r *koneksiRepository) Update(koneksi *domain.Koneksi) error {
	return r.db.Save(koneksi).Error
}

func (r *koneksiRepository) Delete(id uint) error {
	return r.db.Delete(&domain.Koneksi{}, id).Error
}

// DeviceRepository is an alias for backward compatibility
// Deprecated: Use KoneksiRepository instead
type DeviceRepository = KoneksiRepository

// NewDeviceRepository returns a KoneksiRepository for backward compatibility
// Deprecated: Use NewKoneksiRepository instead
func NewDeviceRepository() DeviceRepository {
	return NewKoneksiRepository()
}
