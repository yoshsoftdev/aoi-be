package worker

import (
	"context"
	"fmt"
	"time"

	"aoi-backend/internal/core/cache"
	"aoi-backend/internal/core/logger"
	"aoi-backend/internal/domain"
	"aoi-backend/internal/repository"
	"aoi-backend/internal/service"
	"aoi-backend/internal/snmp"
)

type SNMPWorker struct {
	repo          repository.SNMPRepository
	paramRepo     repository.ParameterRepository
	deviceRepo    repository.DeviceRepository
	cache         cache.Cache
	formulaEval   *service.FormulaEvaluator
	formulaEngine *service.FormulaEngine
	wsHub         *WebSocketHub
}

func NewSNMPWorker(
	repo repository.SNMPRepository,
	paramRepo repository.ParameterRepository,
	deviceRepo repository.DeviceRepository,
	cache cache.Cache,
	formulaEval *service.FormulaEvaluator,
	formulaEngine *service.FormulaEngine,
	wsHub *WebSocketHub,
) *SNMPWorker {
	return &SNMPWorker{
		repo:          repo,
		paramRepo:     paramRepo,
		deviceRepo:    deviceRepo,
		cache:         cache,
		formulaEval:   formulaEval,
		formulaEngine: formulaEngine,
		wsHub:         wsHub,
	}
}

func (w *SNMPWorker) Execute(ctx context.Context, configID uint) error {
	config, err := w.repo.GetByID(configID)
	if err != nil {
		return fmt.Errorf("failed to get SNMP config: %w", err)
	}

	param, err := w.paramRepo.GetByID(config.IDParameter)
	if err != nil {
		return fmt.Errorf("failed to get parameter: %w", err)
	}

	// Create SNMP client
	client, err := snmp.NewSNMPClient(config)
	if err != nil {
		w.handleError(config.IDParameter, param.Nama, param.Type, err)
		return err
	}
	defer client.Close()

	// Get SNMP value
	rawValue, err := client.Get(ctx, config.OID)
	if err != nil {
		w.handleError(config.IDParameter, param.Nama, param.Type, err)
		return err
	}

	// Convert to float64 for formula evaluation
	var rawFloat float64
	switch v := rawValue.(type) {
	case int:
		rawFloat = float64(v)
	case uint:
		rawFloat = float64(v)
	case uint32:
		rawFloat = float64(v)
	case uint64:
		rawFloat = float64(v)
	case float64:
		rawFloat = v
	default:
		rawFloat = 0
	}

	// Evaluate formula if exists
	var processedValue interface{} = rawValue
	if param.CustomRumus != nil && *param.CustomRumus != "" {
		processed, err := w.formulaEval.Evaluate(*param.CustomRumus, rawFloat)
		if err != nil {
			logger.GetLogger().Warnf("Failed to evaluate formula for parameter %d: %v", config.IDParameter, err)
		} else {
			processedValue = processed
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
		logger.GetLogger().Debugf("Skipping non-pollable parameter %d (Mode=%s) - handled by Formula Engine", config.IDParameter, paramMode)
		return nil
	}

	// Create realtime data for RAW parameter
	realtimeData := domain.RealtimeData{
		ParameterID:    config.IDParameter,
		ParameterNama:  param.Nama,
		ParameterType:  param.Type,
		DeviceNama:     deviceNama,
		RawValue:       rawValue,
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

func (w *SNMPWorker) handleError(paramID uint, paramNama, paramType string, err error) {
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

	// CRITICAL: When device is offline, update clean cache with value=0 and status=OFFLINE
	// This ensures parameter remains visible in dashboard
	cleanData := domain.RealtimeData{
		ParameterID:    paramID,
		ParameterNama:  paramNama,
		ParameterType:  paramType,
		DeviceNama:     deviceNama,
		RawValue:       nil,        // CLEAN data does NOT expose raw value
		ProcessedValue: float64(0), // Force value to 0 when offline
		CustomRumus: func() *string {
			if paramErr == nil && param.Unit != nil {
				return param.Unit
			}
			return nil
		}(),
		Timestamp: time.Now(),
		Status:    "offline", // Status = OFFLINE (not "error")
		Error:     nil,       // Don't expose error message in clean data
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
		log.Warnf("Failed to save raw error data to cache: %v", err)
	}

	// CRITICAL: Save CLEAN data with value=0 and status=OFFLINE to public cache
	// This ensures parameter remains visible in dashboard
	cleanCacheKey := fmt.Sprintf("parameter:clean:%d", paramID)
	if err := w.cache.Set(cleanCacheKey, cleanData, 24*time.Hour); err != nil {
		log.Warnf("Failed to save clean offline data to cache: %v", err)
	} else {
		log.Infof("[OFFLINE] parameter_id=%d, parameter_nama=%s, value=0, status=offline",
			paramID, paramNama)
	}

	// CRITICAL: Broadcast CLEAN data with value=0 and status=OFFLINE
	// This ensures frontend receives update and parameter remains visible
	if w.wsHub != nil {
		log.Debugf("[BROADCAST] Broadcasting OFFLINE parameter: parameter_id=%d, value=0", paramID)
		w.wsHub.BroadcastDataUpdate(cleanData)
	}
}
