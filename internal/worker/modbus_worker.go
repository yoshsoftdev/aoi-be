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

type ModbusRTUWorker struct {
	repo            repository.ModbusRTURepository
	paramRepo       repository.ParameterRepository
	deviceRepo      repository.DeviceRepository
	cache           cache.Cache
	formulaEval     *service.FormulaEvaluator
	formulaEngine   *service.FormulaEngine
	thresholdService *service.ThresholdService
	wsHub           *WebSocketHub
}

func NewModbusRTUWorker(
	repo repository.ModbusRTURepository,
	paramRepo repository.ParameterRepository,
	deviceRepo repository.DeviceRepository,
	cache cache.Cache,
	formulaEval *service.FormulaEvaluator,
	formulaEngine *service.FormulaEngine,
	thresholdService *service.ThresholdService,
	wsHub *WebSocketHub,
) *ModbusRTUWorker {
	return &ModbusRTUWorker{
		repo:            repo,
		paramRepo:       paramRepo,
		deviceRepo:      deviceRepo,
		cache:           cache,
		formulaEval:     formulaEval,
		formulaEngine:   formulaEngine,
		thresholdService: thresholdService,
		wsHub:           wsHub,
	}
}

func (w *ModbusRTUWorker) Execute(ctx context.Context, configID uint) error {
	config, err := w.repo.GetByID(configID)
	if err != nil {
		return fmt.Errorf("failed to get Modbus RTU config: %w", err)
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

	client, err := modbus.NewRTUClient(device, slaveID)
	if err != nil {
		w.handleError(config.IDParameter, param.Nama, param.Type, err)
		return fmt.Errorf("failed to create Modbus RTU client: %w", err)
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
		}
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
		// Silent validation

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
			if err == nil {
				processedValue = processed
			}
		} else {
			// Simple formula evaluation with single value
			processed, err := w.formulaEval.Evaluate(formula, scaledValue)
			if err == nil {
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
		// Create CLEAN data (no raw value exposed)
		cleanData := domain.RealtimeData{
			ParameterID:    config.IDParameter,
			ParameterNama:  param.Nama,
			ParameterType:  param.Type,
			DeviceNama:     device.Nama,
			RawValue:       nil,            // CLEAN data does NOT expose raw value
			ProcessedValue: processedValue, // This is the CLEAN value
			CustomRumus:    param.Unit,     // Use CustomRumus field to pass unit (temporary)
			Timestamp:      time.Now(),
			Status:         "online",
			Error:          nil,
			AlarmStatus:    domain.AlarmStatusNormal, // Default to normal
			Direction:      param.Direction,           // Include parameter direction
		}

		// Apply threshold to calculate alarm status
		if w.thresholdService != nil {
			w.thresholdService.ApplyThreshold(&cleanData)
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

func (w *ModbusRTUWorker) handleError(paramID uint, paramNama, paramType string, err error) {
	errorMsg := err.Error()
	log := logger.GetLogger()

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
			log.Debugf("Skipping error broadcast for formula parameter %d", paramID)
			return
		}

		// GUARD: Skip error handling for SOURCE parameters (internal only)
		if paramMode == domain.ParameterModeSOURCE {
			log.Debugf("Skipping error broadcast for SOURCE parameter %d", paramID)
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
		RawValue:       nil,        // CLEAN data does NOT expose raw value
		ProcessedValue: nil,        // Value is null when offline
		CustomRumus: func() *string {
			if paramErr == nil && param.Unit != nil {
				return param.Unit
			}
			return nil
		}(),
		Timestamp:   time.Now(),
		Status:      "offline", // Status = OFFLINE (not "error")
		Error:       nil,       // Don't expose error message in clean data
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

// containsFieldNames checks if formula contains field names like "Slave X Addr Y"
func (w *ModbusRTUWorker) containsFieldNames(formula string) bool {
	// Check for pattern like "Slave" followed by number and "Addr" followed by number
	matched, _ := regexp.MatchString(`Slave\s+\d+\s+Addr\s+\d+`, formula)
	return matched
}

// getFieldValues collects field values from all parameters in the same device
// Returns a map of field names (e.g., "Slave 6 Addr 4") to their processed values
func (w *ModbusRTUWorker) getFieldValues(deviceID uint, currentSlaveID int, currentAddr int, currentValue float64) map[string]float64 {
	fieldValues := make(map[string]float64)

	// Add current parameter value
	fieldName := fmt.Sprintf("Slave %d Addr %d", currentSlaveID, currentAddr)
	fieldValues[fieldName] = currentValue

	// Get all parameters from the same device
	allParams, err := w.paramRepo.GetByDeviceID(deviceID)
	if err != nil {
		return fieldValues
	}

	// Get values from other parameters
	for _, p := range allParams {
		if p.Type != "modbus_rtu" {
			continue
		}

		// Get Modbus RTU config for this parameter
		rtuConfig, err := w.repo.GetByParameterID(p.ID)
		if err != nil {
			continue
		}

		// Create field name
		paramFieldName := fmt.Sprintf("Slave %d Addr %d", rtuConfig.SlaveID, rtuConfig.RegisterAddress)

		// Skip if it's the current parameter (already added)
		if rtuConfig.SlaveID == currentSlaveID && rtuConfig.RegisterAddress == currentAddr {
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
