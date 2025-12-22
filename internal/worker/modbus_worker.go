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
	repo          repository.ModbusRTURepository
	paramRepo     repository.ParameterRepository
	deviceRepo    repository.DeviceRepository
	cache         cache.Cache
	formulaEval   *service.FormulaEvaluator
	formulaEngine *service.FormulaEngine
	wsHub         *WebSocketHub
}

func NewModbusRTUWorker(
	repo repository.ModbusRTURepository,
	paramRepo repository.ParameterRepository,
	deviceRepo repository.DeviceRepository,
	cache cache.Cache,
	formulaEval *service.FormulaEvaluator,
	formulaEngine *service.FormulaEngine,
	wsHub *WebSocketHub,
) *ModbusRTUWorker {
	return &ModbusRTUWorker{
		repo:          repo,
		paramRepo:     paramRepo,
		deviceRepo:    deviceRepo,
		cache:         cache,
		formulaEval:   formulaEval,
		formulaEngine: formulaEngine,
		wsHub:         wsHub,
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
	log := logger.GetLogger()
	log.Infof("[READ] Creating Modbus RTU client: parameter_id=%d, device_id=%d, device_nama=%s, port=%s, baud=%d, slave_id=%d, timeout_ms=%d",
		config.IDParameter, device.ID, device.Nama,
		func() string {
			if device.Port != nil {
				return *device.Port
			} else {
				return "nil"
			}
		}(),
		func() int {
			if device.Baud != nil {
				return *device.Baud
			} else {
				return 0
			}
		}(),
		*slaveID,
		device.TimeoutMs)

	client, err := modbus.NewRTUClient(device, slaveID)
	if err != nil {
		log.Errorf("[READ] Failed to create Modbus RTU client: parameter_id=%d, error=%v", config.IDParameter, err)
		return fmt.Errorf("failed to create Modbus RTU client: %w", err)
	}
	defer client.Close()
	log.Debugf("[READ] Modbus RTU client created successfully: parameter_id=%d", config.IDParameter)

	// Validate function code
	if config.FunctionCode == 0 {
		err := fmt.Errorf("function_code is not set (0 is invalid)")
		w.handleError(config.IDParameter, param.Nama, param.Type, err)
		return err
	}

	// Read registers
	log.Debugf("[READ] Reading Modbus: parameter_id=%d, function_code=%d, address=%d, quantity=%d",
		config.IDParameter, config.FunctionCode, config.RegisterAddress, config.Quantity)
	data, err := client.Read(ctx, config.FunctionCode, uint16(config.RegisterAddress), uint16(config.Quantity))
	if err != nil {
		log.Errorf("[READ] Failed to read Modbus: parameter_id=%d, function_code=%d, address=%d, quantity=%d, error=%v, error_type=%T",
			config.IDParameter, config.FunctionCode, config.RegisterAddress, config.Quantity, err, err)
		w.handleError(config.IDParameter, param.Nama, param.Type, err)
		return err
	}
	log.Debugf("[READ] Successfully read Modbus: parameter_id=%d, bytes_received=%d", config.IDParameter, len(data))

	// Log raw data received
	logger.GetLogger().Debugf("Parameter %d (%s): Raw data received: %v (hex: %X), length: %d bytes",
		config.IDParameter, param.Nama, data, data, len(data))

	// Convert bytes to value based on data type
	var rawValueFloat float64

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

	logger.GetLogger().Debugf("Parameter %d (%s): Using data_type=%s, byte_order=%s, quantity=%d, scaling=%.6f",
		config.IDParameter, param.Nama, dataType, config.ByteOrder, config.Quantity, config.Scaling)

	// Validate data_type and quantity compatibility
	if (dataType == "float32" || dataType == "float") && config.Quantity < 2 {
		logger.GetLogger().Warnf("Parameter %d: data_type is %s but quantity is %d, adjusting quantity to 2", config.IDParameter, dataType, config.Quantity)
		// Note: We can't change config.Quantity here, but we'll use what we have
		// The converter will return an error which will be handled
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
		logger.GetLogger().Debugf("Parameter %d (%s): Converted to float32: %.6f", config.IDParameter, param.Nama, rawFloat32)
	case "float64":
		rawFloat64, err := modbus.ConvertBytesToFloat64(data, config.ByteOrder, config.Quantity)
		if err != nil {
			w.handleError(config.IDParameter, param.Nama, param.Type, err)
			return err
		}
		rawValueFloat = rawFloat64
		logger.GetLogger().Debugf("Parameter %d (%s): Converted to float64: %.6f", config.IDParameter, param.Nama, rawFloat64)
	case "int32":
		rawInt32, err := modbus.ConvertBytesToSignedInt32(data, config.ByteOrder, config.Quantity)
		if err != nil {
			w.handleError(config.IDParameter, param.Nama, param.Type, err)
			return err
		}
		rawValueFloat = float64(rawInt32)
		logger.GetLogger().Debugf("Parameter %d (%s): Converted to int32: %d", config.IDParameter, param.Nama, rawInt32)
	default: // uint16, uint32, or default
		rawValue, err := modbus.ConvertBytesToValue(data, config.ByteOrder, config.Quantity)
		if err != nil {
			w.handleError(config.IDParameter, param.Nama, param.Type, err)
			return err
		}
		rawValueFloat = float64(rawValue)
		logger.GetLogger().Debugf("Parameter %d (%s): Converted to uint%d: %d (0x%X)",
			config.IDParameter, param.Nama,
			config.Quantity*16, rawValue, rawValue)
	}

	// Apply scaling factor
	scaledValue := rawValueFloat * config.Scaling
	logger.GetLogger().Debugf("Parameter %d (%s): Raw value: %.6f, Scaling: %.6f, Scaled value: %.6f",
		config.IDParameter, param.Nama, rawValueFloat, config.Scaling, scaledValue)

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

	// GUARD: Skip if this is a formula parameter (not raw)
	// Formula parameters are handled by Formula Engine, not workers
	if param.IsFormula {
		logger.GetLogger().Debugf("Skipping formula parameter %d in worker - handled by Formula Engine", config.IDParameter)
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

	// Save RAW data to cache with raw key (internal use only)
	// This will NOT be broadcast to frontend
	cacheKey := fmt.Sprintf("parameter:raw:%d", config.IDParameter)
	if err := w.cache.Set(cacheKey, realtimeData, 24*time.Hour); err != nil {
		logger.GetLogger().Warnf("Failed to save raw data to cache: %v", err)
	} else {
		logger.GetLogger().Infof("[READ] Raw parameter cached: parameter_id=%d, parameter_nama=%s, raw_value=%v, processed_value=%v, cache_key=%s",
			config.IDParameter, param.Nama, realtimeData.RawValue, realtimeData.ProcessedValue, cacheKey)
	}

	// NOTIFY Formula Engine (do not broadcast)
	// Formula Engine will evaluate formulas and broadcast clean data
	if w.formulaEngine != nil {
		logger.GetLogger().Debugf("[READ] Notifying Formula Engine: parameter_id=%d", config.IDParameter)
		w.formulaEngine.OnRawParameterUpdated(config.IDParameter, &realtimeData)
	} else {
		logger.GetLogger().Warnf("[READ] Formula Engine is nil, cannot notify: parameter_id=%d", config.IDParameter)
	}

	return nil
}

func (w *ModbusRTUWorker) handleError(paramID uint, paramNama, paramType string, err error) {
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
		logger.GetLogger().Warnf("Failed to get parameters for device %d: %v", deviceID, err)
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
