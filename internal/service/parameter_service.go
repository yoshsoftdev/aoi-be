package service

import (
	"aoi-backend/internal/core/errors"
	"aoi-backend/internal/domain"
	"aoi-backend/internal/repository"
)

type ParameterService interface {
	Create(param *domain.Parameter) error
	GetByID(id uint) (*domain.Parameter, error)
	GetAll() ([]domain.Parameter, error)
	Update(id uint, param *domain.Parameter) error
	Delete(id uint) error
}

type parameterService struct {
	repo repository.ParameterRepository
}

func NewParameterService(repo repository.ParameterRepository) ParameterService {
	return &parameterService{
		repo: repo,
	}
}

func (s *parameterService) Create(param *domain.Parameter) error {
	// Validate type
	validTypes := map[string]bool{
		"modbus_rtu":     true,
		"modbus_tcp":     true,
		"analog_input":   true,
		"analog_output":  true,
		"digital_input":  true,
		"digital_output": true,
		"snmp":           true,
	}

	if !validTypes[param.Type] {
		return errors.NewValidationError("Invalid parameter type", param.Type)
	}

	// Validate device_id exists
	if param.DeviceID == 0 {
		return errors.NewValidationError("Device ID is required", "")
	}

	// Set direction based on read_write if direction is not explicitly set
	// CRITICAL: direction determines if parameter is scheduled (READ) or command-based (WRITE)
	if param.Direction == "" {
		if param.ReadWrite {
			param.Direction = "WRITE"
		} else {
			param.Direction = "READ"
		}
	}

	// Validate direction
	if param.Direction != "READ" && param.Direction != "WRITE" {
		return errors.NewValidationError("Invalid direction. Must be 'READ' or 'WRITE'", param.Direction)
	}

	return s.repo.Create(param)
}

func (s *parameterService) GetByID(id uint) (*domain.Parameter, error) {
	param, err := s.repo.GetByID(id)
	if err != nil {
		return nil, errors.NewNotFoundError("Parameter not found")
	}
	return param, nil
}

func (s *parameterService) GetAll() ([]domain.Parameter, error) {
	return s.repo.GetAll()
}

func (s *parameterService) Update(id uint, param *domain.Parameter) error {
	existing, err := s.repo.GetByID(id)
	if err != nil {
		return errors.NewNotFoundError("Parameter not found")
	}

	param.ID = existing.ID
	return s.repo.Update(param)
}

func (s *parameterService) Delete(id uint) error {
	_, err := s.repo.GetByID(id)
	if err != nil {
		return errors.NewNotFoundError("Parameter not found")
	}
	return s.repo.Delete(id)
}
