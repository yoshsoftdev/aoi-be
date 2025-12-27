package service

import (
	"strconv"

	"aoi-backend/internal/core/logger"
	"aoi-backend/internal/domain"
	"aoi-backend/internal/repository"
)

// ThresholdService handles threshold and alarm status calculation
type ThresholdService struct {
	thresholdRepo repository.ThresholdRepository
}

// NewThresholdService creates a new ThresholdService
func NewThresholdService(thresholdRepo repository.ThresholdRepository) *ThresholdService {
	return &ThresholdService{
		thresholdRepo: thresholdRepo,
	}
}

// CalculateAlarmStatus calculates alarm status based on value and threshold configuration
// Returns alarm status according to priority rules:
// 1. value >= critical_high → critical_high
// 2. value <= critical_low → critical_low
// 3. value >= warning_high → warning_high
// 4. value <= warning_low → warning_low
// 5. value < min_value OR value > max_value → abnormal
// 6. else → normal
func (s *ThresholdService) CalculateAlarmStatus(parameterID uint, value float64) string {
	// Get threshold configuration for this parameter
	threshold, err := s.thresholdRepo.GetByParameterID(parameterID)
	if err != nil {
		// No threshold configured, return normal
		return domain.AlarmStatusNormal
	}

	// Apply priority rules
	if value >= threshold.CriticalHigh {
		return domain.AlarmStatusCriticalHigh
	}
	if value <= threshold.CriticalLow {
		return domain.AlarmStatusCriticalLow
	}
	if value >= threshold.WarningHigh {
		return domain.AlarmStatusWarningHigh
	}
	if value <= threshold.WarningLow {
		return domain.AlarmStatusWarningLow
	}
	if value < threshold.MinValue || value > threshold.MaxValue {
		return domain.AlarmStatusAbnormal
	}

	return domain.AlarmStatusNormal
}

// ApplyThreshold applies threshold to RealtimeData and sets alarm status
// This should be called AFTER clean value is calculated
func (s *ThresholdService) ApplyThreshold(data *domain.RealtimeData) {
	if data.ProcessedValue == nil {
		// No value to check, set to normal
		data.AlarmStatus = domain.AlarmStatusNormal
		return
	}

	// Convert ProcessedValue to float64
	var value float64
	switch v := data.ProcessedValue.(type) {
	case float64:
		value = v
	case float32:
		value = float64(v)
	case int:
		value = float64(v)
	case int32:
		value = float64(v)
	case int64:
		value = float64(v)
	case string:
		// Try to parse string to float64
		parsed, err := strconv.ParseFloat(v, 64)
		if err != nil {
			log := logger.GetLogger()
			log.Warnf("ThresholdService: Cannot parse ProcessedValue string to float64 for parameter %d, value: %s, error: %v", data.ParameterID, v, err)
			data.AlarmStatus = domain.AlarmStatusNormal
			return
		}
		value = parsed
	default:
		log := logger.GetLogger()
		log.Warnf("ThresholdService: Cannot convert ProcessedValue to float64 for parameter %d, type: %T, value: %v", data.ParameterID, v, v)
		data.AlarmStatus = domain.AlarmStatusNormal
		return
	}

	// Calculate alarm status
	data.AlarmStatus = s.CalculateAlarmStatus(data.ParameterID, value)
}

