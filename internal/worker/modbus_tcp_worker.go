package worker

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"time"

	"aoi-backend/internal/core/cache"
	"aoi-backend/internal/core/logger"
	"aoi-backend/internal/domain"
	"aoi-backend/internal/modbus"
	"aoi-backend/internal/repository"
	"aoi-backend/internal/service"
)

type ModbusTCPWorker struct {
	repo          repository.ModbusTCPRepository
	paramRepo     repository.ParameterRepository
	deviceRepo    repository.DeviceRepository
	cache         cache.Cache
	formulaEval   *service.FormulaEvaluator
	formulaEngine *service.FormulaEngine
	wsHub         *WebSocketHub
}

func NewModbusTCPWorker(
	repo repository.ModbusTCPRepository,
	paramRepo repository.ParameterRepository,
	deviceRepo repository.DeviceRepository,
	cache cache.Cache,
	formulaEval *service.FormulaEvaluator,
	formulaEngine *service.FormulaEngine,
	wsHub *WebSocketHub,
) *ModbusTCPWorker {
	return &ModbusTCPWorker{
		repo:          repo,
		paramRepo:     paramRepo,
		deviceRepo:    deviceRepo,
		cache:         cache,
		formulaEval:   formulaEval,
		formulaEngine: formulaEngine,
		wsHub:         wsHub,
	}
}

func (w *ModbusTCPWorker) Execute(ctx context.Context, configID uint) error {
	config, err := w.repo.GetByID(configID)
	if err != nil {
		return fmt.Errorf("failed to get Modbus TCP config: %w", err)
	}

	param, err := w.paramRepo.GetByID(config.IDParameter)
	if err != nil {
		return fmt.Errorf("failed to get parameter: %w", err)
	}

	// Get device from parameter
	device, err := w.deviceRepo.GetByID(param.DeviceID)
	if err != nil {
		return fmt.Errorf("failed to get device: %w", err)
	}

	// Create Modbus client using device config and parameter-level slave_id
	slaveID := &config.SlaveID
	client, err := modbus.NewTCPClient(device, slaveID)
	if err != nil {
		return fmt.Errorf("failed to create Modbus TCP client: %w", err)
	}
	defer client.Close()

	// Validate function code
	if config.FunctionCode == 0 {
		err := fmt.Errorf("function_code is not set (0 is invalid)")
		w.handleError(config.IDParameter, param.Nama, param.Type, err)
		return err
	}

	// Read registers
	data, err := client.Read(ctx, config.FunctionCode, uint16(config.RegisterAddress), uint16(config.Quantity))
	if err != nil {
		w.handleError(config.IDParameter, param.Nama, param.Type, err)
		return err
	}

	// Convert bytes to value based on data type
	var rawValueFloat float64

	// Handle Function Code 1 (Read Coils) and Function Code 2 (Read Discrete Inputs)
	// These return bit-packed data (1 byte can contain 8 bits), not register format (2 bytes per register)
	if config.FunctionCode == 1 || config.FunctionCode == 2 {
		// Convert bit-packed data to array of bits
		bits := modbus.ConvertBitsToArray(data, config.Quantity)
		if len(bits) == 0 {
			err := fmt.Errorf("no bits extracted from FC%d response: data=%v, quantity=%d", config.FunctionCode, data, config.Quantity)
			w.handleError(config.IDParameter, param.Nama, param.Type, err)
			return err
		}
		// Use the first bit value (or sum if quantity > 1)
		if config.Quantity == 1 {
			rawValueFloat = float64(bits[0])
		} else {
			// For multiple coils/discrete inputs, sum them or use first value
			// Most common use case: quantity=1, so use first value
			rawValueFloat = float64(bits[0])
			logger.GetLogger().Debugf("Parameter %d (%s): FC%d read %d bits, using first bit value: %d",
				config.IDParameter, param.Nama, config.FunctionCode, len(bits), bits[0])
		}
		logger.GetLogger().Debugf("Parameter %d (%s): Converted FC%d bits to value: %.0f (bits: %v)",
			config.IDParameter, param.Nama, config.FunctionCode, rawValueFloat, bits)
	} else {
		// Handle register-based function codes (FC3, FC4, etc.)
		// Determine data type (default to uint16 if not specified)
		dataType := config.DataType
		if dataType == "" {
			// Auto-detect based on quantity
			if config.Quantity >= 4 {
				dataType = "float64" // Default to float64 for 4+ registers
			} else if config.Quantity >= 2 {
				dataType = "uint32" // Default to uint32 for 2 registers (safer than float32)
			} else {
				dataType = "uint16" // Default to uint16 for single register
			}
		}

		// Validate data_type and quantity compatibility
		if (dataType == "float32" || dataType == "float") && config.Quantity < 2 {
			logger.GetLogger().Warnf("Parameter %d: data_type is %s but quantity is %d, adjusting quantity to 2", config.IDParameter, dataType, config.Quantity)
		} else if dataType == "float64" && config.Quantity < 4 {
			logger.GetLogger().Warnf("Parameter %d: data_type is float64 but quantity is %d, adjusting quantity to 4", config.IDParameter, config.Quantity)
		} else if dataType == "int32" && config.Quantity < 2 {
			logger.GetLogger().Warnf("Parameter %d: data_type is int32 but quantity is %d, adjusting quantity to 2", config.IDParameter, config.Quantity)
		}

		switch dataType {
		case "float32", "float": // Support both "float32" and "float" (alias)
			rawFloat32, err := modbus.ConvertBytesToFloat32(data, config.ByteOrder, config.Quantity)
			if err != nil {
				w.handleError(config.IDParameter, param.Nama, param.Type, err)
				return err
			}
			rawValueFloat = float64(rawFloat32)
		case "float64":
			rawFloat64, err := modbus.ConvertBytesToFloat64(data, config.ByteOrder, config.Quantity)
			if err != nil {
				w.handleError(config.IDParameter, param.Nama, param.Type, err)
				return err
			}
			rawValueFloat = rawFloat64
		case "int32":
			rawInt32, err := modbus.ConvertBytesToSignedInt32(data, config.ByteOrder, config.Quantity)
			if err != nil {
				w.handleError(config.IDParameter, param.Nama, param.Type, err)
				return err
			}
			rawValueFloat = float64(rawInt32)
		default: // uint16, uint32, or default
			rawValue, err := modbus.ConvertBytesToValue(data, config.ByteOrder, config.Quantity)
			if err != nil {
				w.handleError(config.IDParameter, param.Nama, param.Type, err)
				return err
			}
			rawValueFloat = float64(rawValue)
		}
	}

	// Apply scaling factor
	scaledValue := rawValueFloat * config.Scaling

	// Evaluate formula if exists
	var processedValue interface{} = scaledValue
	if param.CustomRumus != nil && *param.CustomRumus != "" {
		// Check if formula contains field names (like "Slave 6 Addr 4")
		formula := *param.CustomRumus
		if w.containsFieldNames(formula) {
			// Get all parameter values from the same device for field name evaluation
			fieldValues := w.getFieldValues(param.DeviceID, config.SlaveID, config.RegisterAddress, scaledValue)
			processed, err := w.formulaEval.EvaluateWithFields(formula, fieldValues)
			if err != nil {
				logger.GetLogger().Warnf("Failed to evaluate formula with fields for parameter %d: %v", config.IDParameter, err)
			} else {
				processedValue = processed
			}
		} else {
			// Simple formula evaluation with single value
			processed, err := w.formulaEval.Evaluate(formula, scaledValue)
			if err != nil {
				logger.GetLogger().Warnf("Failed to evaluate formula for parameter %d: %v", config.IDParameter, err)
			} else {
				processedValue = processed
			}
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
		logger.GetLogger().Debugf("Skipping non-pollable parameter %d (Mode=%s) - handled by Formula Engine", config.IDParameter, paramMode)
		return nil
	}

	// Create realtime data for RAW parameter
	realtimeData := domain.RealtimeData{
		ParameterID:    config.IDParameter,
		ParameterNama:  param.Nama,
		ParameterType:  param.Type,
		DeviceNama:     device.Nama,
		RawValue:       rawValueFloat,
		ProcessedValue: processedValue,
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
		logger.GetLogger().Warnf("Failed to save raw data to cache: %v", err)
	} else {
		logger.GetLogger().Infof("[RAW READ] parameter_id=%d, parameter_nama=%s, mode=%s, raw_value=%v, processed_value=%v",
			config.IDParameter, param.Nama, paramMode, realtimeData.RawValue, realtimeData.ProcessedValue)
	}

	// ============================================
	// STEP 2: Handle SOURCE vs DIRECT differently
	// ============================================
	if paramMode == domain.ParameterModeSOURCE {
		// SOURCE: Internal only, tidak dibroadcast
		// Only notify Formula Engine (for CLEAN derivation)
		if w.formulaEngine != nil {
			logger.GetLogger().Debugf("[SOURCE] Notifying Formula Engine: parameter_id=%d", config.IDParameter)
			w.formulaEngine.OnRawParameterUpdated(config.IDParameter, &realtimeData)
		}
		logger.GetLogger().Debugf("[SOURCE] Parameter %d: RAW cached, NOT broadcast (internal only)", config.IDParameter)
		return nil
	}

	// DIRECT: Nilai sudah clean, dibroadcast langsung
	if paramMode == domain.ParameterModeDIRECT {
		// Get device name (device already loaded earlier in Execute)
		deviceNama := device.Nama

		// Create CLEAN data (no raw value exposed)
		cleanData := domain.RealtimeData{
			ParameterID:    config.IDParameter,
			ParameterNama:  param.Nama,
			ParameterType:  param.Type,
			DeviceNama:     deviceNama,
			RawValue:       nil,            // CLEAN data does NOT expose raw value
			ProcessedValue: processedValue, // This is the CLEAN value
			CustomRumus:    param.Unit,     // Use CustomRumus field to pass unit (temporary)
			Timestamp:      time.Now(),
			Status:         "online",
			Error:          nil,
		}

		// Save CLEAN data to public cache
		cleanCacheKey := fmt.Sprintf("parameter:clean:%d", config.IDParameter)
		if err := w.cache.Set(cleanCacheKey, cleanData, 24*time.Hour); err != nil {
			logger.GetLogger().Warnf("Failed to save clean data to cache: %v", err)
		} else {
			logger.GetLogger().Infof("[CLEAN EMIT] parameter_id=%d, parameter_nama=%s, clean_value=%v",
				config.IDParameter, param.Nama, cleanData.ProcessedValue)
		}

		// Broadcast CLEAN data via WebSocket
		if w.wsHub != nil {
			logger.GetLogger().Debugf("[BROADCAST] Broadcasting DIRECT parameter clean data: parameter_id=%d", config.IDParameter)
			w.wsHub.BroadcastDataUpdate(cleanData)
		}

		// Also notify Formula Engine (for CLEAN derivation if needed)
		if w.formulaEngine != nil {
			logger.GetLogger().Debugf("[DIRECT] Notifying Formula Engine: parameter_id=%d", config.IDParameter)
			w.formulaEngine.OnRawParameterUpdated(config.IDParameter, &realtimeData)
		}

		return nil
	}

	return nil
}

func (w *ModbusTCPWorker) handleError(paramID uint, paramNama, paramType string, err error) {
	errorMsg := err.Error()

	// Get parameter and device for error message
	param, paramErr := w.paramRepo.GetByID(paramID)
	var deviceNama string
	if paramErr == nil {
		device, deviceErr := w.deviceRepo.GetByID(param.DeviceID)
		if deviceErr == nil {
			deviceNama = device.Nama
		}

		// GUARD: Skip error broadcast for formula parameters
		if param.IsFormula {
			logger.GetLogger().Debugf("Skipping error broadcast for formula parameter %d", paramID)
			return
		}
	}

	realtimeData := domain.RealtimeData{
		ParameterID:   paramID,
		ParameterNama: paramNama,
		ParameterType: paramType,
		DeviceNama:    deviceNama,
		Timestamp:     time.Now(),
		Status:        "error",
		Error:         &errorMsg,
	}

	// Cache error state (raw parameter only)
	cacheKey := fmt.Sprintf("parameter:raw:%d", paramID)
	w.cache.Set(cacheKey, realtimeData, 24*time.Hour)

	// DO NOT broadcast error - errors are handled by Formula Engine for formula parameters
	// Raw parameter errors are internal only
}

// containsFieldNames checks if formula contains field names like "Slave X Addr Y"
func (w *ModbusTCPWorker) containsFieldNames(formula string) bool {
	// Check for pattern like "Slave" followed by number and "Addr" followed by number
	matched, _ := regexp.MatchString(`Slave\s+\d+\s+Addr\s+\d+`, formula)
	return matched
}

// getFieldValues collects field values from all parameters in the same device
// Returns a map of field names (e.g., "Slave 6 Addr 4") to their processed values
func (w *ModbusTCPWorker) getFieldValues(deviceID uint, currentSlaveID int, currentAddr int, currentValue float64) map[string]float64 {
	fieldValues := make(map[string]float64)

	// Add current parameter value
	fieldName := fmt.Sprintf("Slave %d Addr %d", currentSlaveID, currentAddr)
	fieldValues[fieldName] = currentValue

	// Get all parameters from the same device
	allParams, err := w.paramRepo.GetByDeviceID(deviceID)
	if err != nil {
		logger.GetLogger().Warnf("Failed to get parameters for device %d: %v", deviceID, err)
		return fieldValues
	}

	// Get values from other parameters
	for _, p := range allParams {
		if p.Type != "modbus_tcp" {
			continue
		}

		// Get Modbus TCP config for this parameter
		tcpConfig, err := w.repo.GetByParameterID(p.ID)
		if err != nil {
			continue
		}

		// Create field name
		paramFieldName := fmt.Sprintf("Slave %d Addr %d", tcpConfig.SlaveID, tcpConfig.RegisterAddress)

		// Skip if it's the current parameter (already added)
		if tcpConfig.SlaveID == currentSlaveID && tcpConfig.RegisterAddress == currentAddr {
			continue
		}

		// Try to get value from cache
		cacheKey := fmt.Sprintf("parameter:%d", p.ID)
		var cachedData domain.RealtimeData
		if err := w.cache.Get(cacheKey, &cachedData); err == nil {
			// Use processed value if available, otherwise raw value
			var val float64
			switch v := cachedData.ProcessedValue.(type) {
			case float64:
				val = v
			case int:
				val = float64(v)
			case int64:
				val = float64(v)
			default:
				// Try raw value
				switch v := cachedData.RawValue.(type) {
				case float64:
					val = v
				case int:
					val = float64(v)
				case int64:
					val = float64(v)
				default:
					val = 0
				}
			}
			// Round to 3 decimal places (using math.Round to avoid overflow for large numbers)
			val = math.Round(val*1000) / 1000
			fieldValues[paramFieldName] = val
		}
	}

	return fieldValues
}
