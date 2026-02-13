package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"aoi-backend/internal/core/cache"
	"aoi-backend/internal/domain"
	"aoi-backend/internal/repository"
	"aoi-backend/internal/service"
	"aoi-backend/internal/snmp"
)

// Helper function to check if float is NaN or Inf
func isNaNOrInf(f float64) bool {
	return math.IsNaN(f) || math.IsInf(f, 0)
}

// getMapKeys returns all keys from a map[string]float64 as a slice
func getMapKeys(m map[string]float64) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

type SNMPWorker struct {
	repo             repository.SNMPRepository
	paramRepo        repository.ParameterRepository
	deviceRepo       repository.DeviceRepository
	cache            cache.Cache
	formulaEval      *service.FormulaEvaluator
	formulaEngine    *service.FormulaEngine
	thresholdService *service.ThresholdService
	wsHub            *WebSocketHub
}

func NewSNMPWorker(
	repo repository.SNMPRepository,
	paramRepo repository.ParameterRepository,
	deviceRepo repository.DeviceRepository,
	cache cache.Cache,
	formulaEval *service.FormulaEvaluator,
	formulaEngine *service.FormulaEngine,
	thresholdService *service.ThresholdService,
	wsHub *WebSocketHub,
) *SNMPWorker {
	return &SNMPWorker{
		repo:             repo,
		paramRepo:        paramRepo,
		deviceRepo:       deviceRepo,
		cache:            cache,
		formulaEval:      formulaEval,
		formulaEngine:    formulaEngine,
		thresholdService: thresholdService,
		wsHub:            wsHub,
	}
}

func (w *SNMPWorker) Execute(ctx context.Context, configID uint) (err error) {
	// Panic recovery to prevent crash on concurrent SNMP requests
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic recovered: %v", r)
		}
	}()

	config, err := w.repo.GetByID(configID)
	if err != nil {
		return fmt.Errorf("failed to get SNMP config: %w", err)
	}


	param, err := w.paramRepo.GetByID(config.IDParameter)
	if err != nil {
		return fmt.Errorf("failed to get parameter: %w", err)
	}

	// Create SNMP client with panic recovery
	var client *snmp.SNMPClient
	func() {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("panic in NewSNMPClient: %v", r)
			}
		}()
		var createErr error
		client, createErr = snmp.NewSNMPClient(config)
		if createErr != nil {
			w.handleError(config.IDParameter, param.Nama, param.Type, createErr)
			err = createErr
		}
	}()

	if err != nil {
		return err
	}

	if client == nil {
		return fmt.Errorf("failed to create SNMP client (panic or error)")
	}

	defer func() {
		if client != nil {
			defer func() {
				if r := recover(); r != nil {
					// Silent panic recovery
				}
			}()
			client.Close()
		}
	}()

	// Get SNMP value with panic recovery
	var rawValue interface{}
	func() {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("panic in client.Get: %v", r)
			}
		}()
		var getErr error
		rawValue, getErr = client.Get(ctx, config.OID)
		if getErr != nil {
			w.handleError(config.IDParameter, param.Nama, param.Type, getErr)
			err = getErr
		}
	}()

	if err != nil {
		return err
	}

	if rawValue == nil {
		return fmt.Errorf("failed to get SNMP value (panic or error)")
	}

	// Handle string values: map to numeric using string mapping
	var processedRawValue interface{} = rawValue
	var rawFloat float64

	// Check if raw value is string
	if strValue, ok := rawValue.(string); ok {
		// CRITICAL: Trim whitespace first (SNMP sometimes returns values with trailing spaces)
		trimmedValue := strings.TrimSpace(strValue)

		// Try to parse as number first (SNMP sometimes returns numeric as string like "231.30", "0.00", "15", "-5.5")
		// Priority: Try float64 first (handles both int and float), then int64 as fallback
		parsedFloat, errFloat := strconv.ParseFloat(trimmedValue, 64)
		if errFloat == nil && !isNaNOrInf(parsedFloat) {
			// String is actually a numeric value - use it directly (no mapping needed)
			// Check if it's a whole number (int) or has decimal (float)
			if parsedFloat == float64(int64(parsedFloat)) {
				// It's a whole number, use int64 for cleaner representation
				processedRawValue = int64(parsedFloat)
				rawFloat = parsedFloat
			} else {
				// It has decimal places, use float64
				processedRawValue = parsedFloat
				rawFloat = parsedFloat
			}
		} else {
			// String is not numeric (e.g., "up", "down", "hello world") - must have mapping
			if config.StringMapping == nil || *config.StringMapping == "" {
				err := fmt.Errorf("SNMP string value '%s' requires string mapping configuration", strValue)
				w.handleError(config.IDParameter, param.Nama, param.Type, err)
				return err
			}

			// Parse string mapping JSON (e.g., {"hello world": 1, "up": 1, "down": 0})
			var stringMap map[string]float64
			if err := json.Unmarshal([]byte(*config.StringMapping), &stringMap); err != nil {
				err := fmt.Errorf("invalid string mapping JSON: %w", err)
				w.handleError(config.IDParameter, param.Nama, param.Type, err)
				return err
			}

			// Map string to numeric using saved mapping
			// CRITICAL: Use trimmedValue for mapping comparison (consistent with numeric parsing)
			// Also try original strValue in case mapping was saved with whitespace
			mappedValue, found := stringMap[trimmedValue]
			if !found {
				// Try original strValue (in case mapping was saved with whitespace)
				mappedValue, found = stringMap[strValue]
			}

			if !found {
				err := fmt.Errorf("string value '%s' (trimmed: '%s') not found in mapping. Available: %v", strValue, trimmedValue, stringMap)
				w.handleError(config.IDParameter, param.Nama, param.Type, err)
				return err
			}

			// Use mapped numeric value (from saved mapping configuration)
			// CRITICAL: Ensure this is always numeric (float64), never string
			processedRawValue = mappedValue
			rawFloat = mappedValue
		}
	} else {
		// Numeric value - convert to float64 for formula evaluation
		switch v := rawValue.(type) {
		case int:
			rawFloat = float64(v)
			processedRawValue = float64(v)
		case uint:
			rawFloat = float64(v)
			processedRawValue = float64(v)
		case uint32:
			rawFloat = float64(v)
			processedRawValue = float64(v)
		case uint64:
			rawFloat = float64(v)
			processedRawValue = float64(v)
		case float64:
			rawFloat = v
			processedRawValue = v
		case float32:
			rawFloat = float64(v)
			processedRawValue = float64(v)
		default:
			rawFloat = 0
			processedRawValue = float64(0)
		}
	}

	// Evaluate formula if exists (use processedRawValue which is already numeric)
	// CRITICAL: processedValue must ALWAYS be numeric (never string) for broadcast
	var processedValue interface{} = processedRawValue
	if param.CustomRumus != nil && *param.CustomRumus != "" {
		processed, err := w.formulaEval.Evaluate(*param.CustomRumus, rawFloat)
		if err == nil {
			processedValue = processed
		}
	}

	// CRITICAL: Ensure processedValue is always numeric (never string)
	// Convert to float64 if it's still a string (should not happen, but safety check)
	if strVal, ok := processedValue.(string); ok {
		// Try to parse as number
		if parsedNum, err := strconv.ParseFloat(strVal, 64); err == nil && !isNaNOrInf(parsedNum) {
			processedValue = parsedNum
			rawFloat = parsedNum
		} else if config.StringMapping != nil && *config.StringMapping != "" {
			// Try to use mapping
			var stringMap map[string]float64
			if err := json.Unmarshal([]byte(*config.StringMapping), &stringMap); err == nil {
				if mappedValue, found := stringMap[strVal]; found {
					processedValue = mappedValue
					rawFloat = mappedValue
				}
			}
		}
		// If still string after all attempts, default to 0
		if _, stillString := processedValue.(string); stillString {
			processedValue = float64(0)
			rawFloat = 0
		}
	}

	// Get device name from parameter
	var deviceNama string
	if param.DeviceID > 0 {
		device, err := w.deviceRepo.GetByID(param.DeviceID)
		if err == nil {
			deviceNama = device.Nama
		}
	}

	// Determine parameter mode (default to DIRECT for backward compatibility)
	// DIRECT = broadcast langsung, SOURCE = internal only
	// Untuk backward compatibility: parameter yang sudah ada tanpa mode eksplisit
	// akan muncul di dashboard (DIRECT), bukan hidden (SOURCE)
	paramMode := param.ParameterMode
	if paramMode == "" {
		paramMode = domain.ParameterModeDIRECT
	}

	// CRITICAL: SOURCE parameters are NEVER broadcast, even if not used by formula
	// SOURCE is explicitly set by user/system and should remain SOURCE
	// Only DIRECT, CLEAN, and FORMULA should appear in dashboard

	// GUARD: Only process SOURCE & DIRECT parameters here
	// CLEAN & FORMULA are handled by Formula Engine, not workers
	if paramMode != domain.ParameterModeSOURCE && paramMode != domain.ParameterModeDIRECT {
		return nil
	}

	// Create realtime data for RAW parameter
	// For string values: store original string in RawValue, mapped numeric in ProcessedValue
	realtimeData := domain.RealtimeData{
		ParameterID:    config.IDParameter,
		ParameterNama:  param.Nama,
		ParameterType:  param.Type,
		DeviceNama:     deviceNama,
		RawValue:       rawValue,       // Original value (string or numeric)
		ProcessedValue: processedValue, // Mapped numeric value (for strings) or original numeric
		CustomRumus:    param.CustomRumus,
		Timestamp:      time.Now(),
		Status:         "online",
		Error:          nil,
	}

	// ============================================
	// STEP 1: Save RAW data to internal cache (for SOURCE & DIRECT)
	// ============================================
	rawCacheKey := fmt.Sprintf("parameter:raw:%d", config.IDParameter)
	if err := w.cache.Set(rawCacheKey, realtimeData, 24*time.Hour); err != nil {
		// Silent fail
	}

	// ============================================
	// STEP 2: Handle SOURCE vs DIRECT differently
	// ============================================
	if paramMode == domain.ParameterModeSOURCE {
		// SOURCE: Internal only, tidak dibroadcast
		// Only notify Formula Engine (for CLEAN derivation)
		if w.formulaEngine != nil {
			w.formulaEngine.OnRawParameterUpdated(config.IDParameter, &realtimeData)
		}
		return nil
	}

	// DIRECT: Nilai sudah clean, dibroadcast langsung
	if paramMode == domain.ParameterModeDIRECT {
		// CRITICAL: Ensure ProcessedValue is always numeric (never string) before broadcast
		finalProcessedValue := processedValue
		if strVal, ok := processedValue.(string); ok {
			// This should not happen if mapping is correct, but safety check
			if parsedNum, err := strconv.ParseFloat(strVal, 64); err == nil && !isNaNOrInf(parsedNum) {
				finalProcessedValue = parsedNum
			} else {
				finalProcessedValue = float64(0)
			}
		}

		// Create CLEAN data (no raw value exposed)
		cleanData := domain.RealtimeData{
			ParameterID:    config.IDParameter,
			ParameterNama:  param.Nama,
			ParameterType:  param.Type,
			DeviceNama:     deviceNama,
			RawValue:       nil,                 // CLEAN data does NOT expose raw value
			ProcessedValue: finalProcessedValue, // This is the CLEAN value (ALWAYS numeric, never string)
			CustomRumus:    param.Unit,          // Use CustomRumus field to pass unit (temporary)
			Timestamp:      time.Now(),
			Status:         "online",
			Error:          nil,
			AlarmStatus:    domain.AlarmStatusNormal, // Default to normal
		}

		// Apply threshold to calculate alarm status
		// SPECIAL RULE: For SNMP string mapping, if value == 0, force abnormal
		// If value == 1, continue with normal threshold evaluation
		if w.thresholdService != nil {
			// Check if this is from string mapping (value is 0 or 1, and we have string mapping config)
			if config.StringMapping != nil && *config.StringMapping != "" {
				// Check if processed value is 0 or 1 (typical string mapping values)
				if numericValue, ok := cleanData.ProcessedValue.(float64); ok {
					if numericValue == 0 {
						// Force abnormal for value 0 (from string mapping)
						cleanData.AlarmStatus = domain.AlarmStatusAbnormal
					} else if numericValue == 1 {
						// Value 1: continue with normal threshold evaluation
						w.thresholdService.ApplyThreshold(&cleanData)
					} else {
						// Other numeric values: normal threshold evaluation
						w.thresholdService.ApplyThreshold(&cleanData)
					}
				} else {
					// Not numeric, use normal threshold evaluation
					w.thresholdService.ApplyThreshold(&cleanData)
				}
			} else {
				// No string mapping, use normal threshold evaluation
				w.thresholdService.ApplyThreshold(&cleanData)
			}
		}

		// Save CLEAN data to public cache (with alarmstatus)
		cleanCacheKey := fmt.Sprintf("parameter:clean:%d", config.IDParameter)
		if err := w.cache.Set(cleanCacheKey, cleanData, 24*time.Hour); err != nil {
			// Silent fail
		}

		// Broadcast CLEAN data via WebSocket (with alarmstatus)
		if w.wsHub != nil {
			w.wsHub.BroadcastDataUpdate(cleanData)
		}

		// Also notify Formula Engine (for CLEAN derivation if needed)
		if w.formulaEngine != nil {
			w.formulaEngine.OnRawParameterUpdated(config.IDParameter, &realtimeData)
		}

		return nil
	}

	return nil
}

func (w *SNMPWorker) handleError(paramID uint, paramNama, paramType string, err error) {
	errorMsg := err.Error()

	// Get parameter and device for error message
	param, paramErr := w.paramRepo.GetByID(paramID)
	var deviceNama string
	var paramMode string
	if paramErr == nil {
		device, deviceErr := w.deviceRepo.GetByID(param.DeviceID)
		if deviceErr == nil {
			deviceNama = device.Nama
		}
		paramMode = param.ParameterMode
		if paramMode == "" {
			paramMode = domain.ParameterModeDIRECT // Default for backward compatibility
		}

		// GUARD: Skip error handling for formula parameters (handled by Formula Engine)
		if param.IsFormula {
			return
		}

		// GUARD: Skip error handling for SOURCE parameters (internal only)
		if paramMode == domain.ParameterModeSOURCE {
			return
		}
	} else {
		// If we can't get parameter, default to DIRECT mode
		paramMode = domain.ParameterModeDIRECT
	}

	// CRITICAL: When device is offline, update clean cache with value=null and status=OFFLINE
	// This ensures parameter remains visible in dashboard
	cleanData := domain.RealtimeData{
		ParameterID:    paramID,
		ParameterNama:  paramNama,
		ParameterType:  paramType,
		DeviceNama:     deviceNama,
		RawValue:       nil, // CLEAN data does NOT expose raw value
		ProcessedValue: nil, // Value is null when offline
		CustomRumus: func() *string {
			if paramErr == nil && param.Unit != nil {
				return param.Unit
			}
			return nil
		}(),
		Timestamp:   time.Now(),
		Status:      "offline",                // Status = OFFLINE (not "error")
		Error:       nil,                      // Don't expose error message in clean data
		AlarmStatus: domain.AlarmStatusNormal, // Normal when offline (no value to check)
	}

	// Save RAW error state to internal cache
	rawCacheKey := fmt.Sprintf("parameter:raw:%d", paramID)
	rawErrorData := domain.RealtimeData{
		ParameterID:   paramID,
		ParameterNama: paramNama,
		ParameterType: paramType,
		DeviceNama:    deviceNama,
		Timestamp:     time.Now(),
		Status:        "error",
		Error:         &errorMsg,
	}
	if err := w.cache.Set(rawCacheKey, rawErrorData, 24*time.Hour); err != nil {
		// Silent fail
	}

	// CRITICAL: Save CLEAN data with value=0 and status=OFFLINE to public cache
	// This ensures parameter remains visible in dashboard
	cleanCacheKey := fmt.Sprintf("parameter:clean:%d", paramID)
	if err := w.cache.Set(cleanCacheKey, cleanData, 24*time.Hour); err != nil {
		// Silent fail
	}

	// CRITICAL: Broadcast CLEAN data with value=0 and status=OFFLINE
	// This ensures frontend receives update and parameter remains visible
	if w.wsHub != nil {
		w.wsHub.BroadcastDataUpdate(cleanData)
	}
}
