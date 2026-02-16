package service

import (
	"aoi-backend/internal/core/cache"
	"aoi-backend/internal/domain"
	"aoi-backend/internal/repository"
	"context"
	"fmt"
	"time"
)

// InternalParameterService manages internal virtual parameters for workflow chaining
type InternalParameterService struct {
	paramRepo repository.ParameterRepository
	cache     cache.Cache
}

// NewInternalParameterService creates a new internal parameter service
func NewInternalParameterService(
	paramRepo repository.ParameterRepository,
	cache cache.Cache,
) *InternalParameterService {
	return &InternalParameterService{
		paramRepo: paramRepo,
		cache:     cache,
	}
}

// CreateInternalParameter creates a new internal parameter
// Internal parameters are virtual - they don't poll devices, only used for workflow logic
func (s *InternalParameterService) CreateInternalParameter(ctx context.Context, name string, deviceID uint) (*domain.Parameter, error) {
	param := &domain.Parameter{
		DeviceID:      deviceID,
		Nama:          name,
		Type:          "internal",
		Direction:     "READ",
		ReadWrite:     0,
		ParameterMode: domain.ParameterModeDIRECT, // Use DIRECT mode so it appears in cache
		IsFormula:     false,
	}

	if err := s.paramRepo.Create(param); err != nil {
		return nil, fmt.Errorf("failed to create internal parameter: %w", err)
	}

	// Initialize cache with value 0
	cleanCacheKey := fmt.Sprintf("parameter:clean:%d", param.ID)
	initialData := domain.RealtimeData{
		ParameterID:    param.ID,
		ParameterNama:  param.Nama,
		ParameterType:  "internal",
		DeviceNama:     "",
		RawValue:       nil,
		ProcessedValue: float64(0),
		Timestamp:      time.Now(),
		Status:         "online",
		Error:          nil,
		AlarmStatus:    domain.AlarmStatusNormal,
	}

	if err := s.cache.Set(cleanCacheKey, initialData, 24*time.Hour); err != nil {
		// Silent fail - cache will be set on first write
	}

	return param, nil
}

// SetValue sets the value of an internal parameter
func (s *InternalParameterService) SetValue(ctx context.Context, paramID uint, value float64) error {
	param, err := s.paramRepo.GetByID(paramID)
	if err != nil {
		return fmt.Errorf("failed to get parameter %d: %w", paramID, err)
	}

	if param.Type != "internal" {
		return fmt.Errorf("parameter %d is not an internal parameter", paramID)
	}

	// Update cache
	cleanCacheKey := fmt.Sprintf("parameter:clean:%d", paramID)
	cleanData := domain.RealtimeData{
		ParameterID:    paramID,
		ParameterNama:  param.Nama,
		ParameterType:  "internal",
		DeviceNama:     "",
		RawValue:       nil,
		ProcessedValue: value,
		Timestamp:      time.Now(),
		Status:         "online",
		Error:          nil,
		AlarmStatus:    domain.AlarmStatusNormal,
	}

	if err := s.cache.Set(cleanCacheKey, cleanData, 24*time.Hour); err != nil {
		return fmt.Errorf("failed to update internal parameter cache: %w", err)
	}

	return nil
}

// ResetValue resets an internal parameter to 0
func (s *InternalParameterService) ResetValue(ctx context.Context, paramID uint) error {
	return s.SetValue(ctx, paramID, 0)
}

// GetValue gets the current value of an internal parameter
func (s *InternalParameterService) GetValue(ctx context.Context, paramID uint) (float64, error) {
	cleanCacheKey := fmt.Sprintf("parameter:clean:%d", paramID)
	var data *domain.RealtimeData
	if err := s.cache.Get(cleanCacheKey, &data); err != nil {
		return 0, fmt.Errorf("internal parameter %d not found in cache", paramID)
	}

	if data.ProcessedValue == nil {
		return 0, nil
	}

	switch v := data.ProcessedValue.(type) {
	case float64:
		return v, nil
	case int:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	default:
		return 0, fmt.Errorf("invalid value type for internal parameter %d", paramID)
	}
}
