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
// Supports different threshold types:
// - "max": Standard max threshold (critical_high, warning_high, etc.)
// - "min": Standard min threshold (critical_low, warning_low, etc.)
// - "min-max": Combined min and max threshold (check both min_value and max_value)
// - "abnormal": Binary threshold (1 = normal, 0 = abnormal)
func (s *ThresholdService) CalculateAlarmStatus(parameterID uint, value float64) string {
	// Get threshold configuration for this parameter
	threshold, err := s.thresholdRepo.GetByParameterID(parameterID)
	if err != nil {
		// No threshold configured, return normal
		return domain.AlarmStatusNormal
	}

	log := logger.GetLogger()

	// Handle different threshold types
	switch threshold.ThresholdType {
	case "abnormal":
		// Abnormal type: hanya mengawasi 0 dan 1
		// Jika nilai tidak 1, maka alarmstatus = abnormal
		// Hanya nilai 1 yang normal, semua nilai lainnya (termasuk 0) adalah abnormal
		if value == 1 {
			return domain.AlarmStatusNormal
		}
		// Semua nilai selain 1 adalah abnormal (termasuk 0, 0.5, 2, dll)
		return domain.AlarmStatusAbnormal

	case "min-max":
		// Min-max type: combined min and max threshold
		// Priority rules (same as max/min but with min-max range check):
		// 1. value >= critical_high → critical_high
		// 2. value <= critical_low → critical_low
		// 3. value >= warning_high → warning_high
		// 4. value <= warning_low → warning_low
		// 5. value < min_value OR value > max_value → abnormal
		// 6. else → normal
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
		// Check if value is outside min-max range
		if value < threshold.MinValue || value > threshold.MaxValue {
			return domain.AlarmStatusAbnormal
		}
		// Value is within min-max range and within normal thresholds
		return domain.AlarmStatusNormal

	case "max":
		// Max type: Standard max threshold (original logic)
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

	case "min":
		// Min type: Focus on low values (critical_low, warning_low)
		// Priority rules for min threshold:
		// 1. value <= critical_low → critical_low (most important - trigger alarm when too low)
		// 2. value <= warning_low → warning_low
		// 3. value < min_value → abnormal
		// 4. else → normal
		// Note: For "min" type, we don't check critical_high or warning_high
		// because the focus is on monitoring minimum values
		if value <= threshold.CriticalLow {
			return domain.AlarmStatusCriticalLow
		}
		if value <= threshold.WarningLow {
			return domain.AlarmStatusWarningLow
		}
		if value < threshold.MinValue {
			return domain.AlarmStatusAbnormal
		}
		// Value is above min_value and above warning_low, so it's normal
		return domain.AlarmStatusNormal

	default:
		// Unknown type, use default max logic
		log.Warnf("ThresholdService: Unknown threshold_type '%s' for parameter %d, using default max logic", threshold.ThresholdType, parameterID)
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
