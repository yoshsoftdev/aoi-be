package service

import (
	"aoi-backend/internal/core/errors"
	"aoi-backend/internal/domain"
	"aoi-backend/internal/repository"
)

type KoneksiService interface {
	Create(koneksi *domain.Koneksi) error
	GetByID(id uint) (*domain.Koneksi, error)
	GetAll() ([]domain.Koneksi, error)
	GetByType(koneksiType string) ([]domain.Koneksi, error)
	GetEnabled() ([]domain.Koneksi, error)
	Update(id uint, koneksi *domain.Koneksi) error
	Delete(id uint) error
}

type koneksiService struct {
	repo repository.KoneksiRepository
}

func NewKoneksiService(repo repository.KoneksiRepository) KoneksiService {
	return &koneksiService{
		repo: repo,
	}
}

func (s *koneksiService) Create(koneksi *domain.Koneksi) error {
	// Validate type
	validTypes := map[string]bool{
		"modbus_rtu": true,
		"modbus_tcp": true,
		"snmp":       true,
		"analog":     true,
		"digital":    true,
	}

	if !validTypes[koneksi.Type] {
		return errors.NewValidationError("Invalid koneksi type", koneksi.Type)
	}

	// Validate required fields based on type
	if koneksi.Type == "modbus_rtu" {
		if koneksi.Port == nil || *koneksi.Port == "" {
			return errors.NewValidationError("Port is required for Modbus RTU", "")
		}
		if koneksi.Baud == nil || *koneksi.Baud == 0 {
			return errors.NewValidationError("Baud rate is required for Modbus RTU", "")
		}
		// Set default slave_id if not provided (can be overridden at parameter level)
		if koneksi.SlaveID == nil {
			defaultSlaveID := 1
			koneksi.SlaveID = &defaultSlaveID
		}
	}

	if koneksi.Type == "modbus_tcp" {
		if koneksi.IPAddress == nil || *koneksi.IPAddress == "" {
			return errors.NewValidationError("IP address is required for Modbus TCP", "")
		}
		// Set default slave_id if not provided (can be overridden at parameter level)
		if koneksi.SlaveID == nil {
			defaultSlaveID := 1
			koneksi.SlaveID = &defaultSlaveID
		}
	}

	return s.repo.Create(koneksi)
}

func (s *koneksiService) GetByID(id uint) (*domain.Koneksi, error) {
	koneksi, err := s.repo.GetByID(id)
	if err != nil {
		return nil, errors.NewNotFoundError("Koneksi not found")
	}
	return koneksi, nil
}

func (s *koneksiService) GetAll() ([]domain.Koneksi, error) {
	return s.repo.GetAll()
}

func (s *koneksiService) GetByType(koneksiType string) ([]domain.Koneksi, error) {
	return s.repo.GetByType(koneksiType)
}

func (s *koneksiService) GetEnabled() ([]domain.Koneksi, error) {
	return s.repo.GetEnabled()
}

func (s *koneksiService) Update(id uint, koneksi *domain.Koneksi) error {
	existing, err := s.repo.GetByID(id)
	if err != nil {
		return errors.NewNotFoundError("Koneksi not found")
	}

	koneksi.ID = existing.ID
	return s.repo.Update(koneksi)
}

func (s *koneksiService) Delete(id uint) error {
	_, err := s.repo.GetByID(id)
	if err != nil {
		return errors.NewNotFoundError("Koneksi not found")
	}
	return s.repo.Delete(id)
}

// DeviceService is an alias for backward compatibility
// Deprecated: Use KoneksiService instead
type DeviceService = KoneksiService

// NewDeviceService returns a KoneksiService for backward compatibility
// Deprecated: Use NewKoneksiService instead
func NewDeviceService(repo repository.DeviceRepository) DeviceService {
	return NewKoneksiService(repo)
}
