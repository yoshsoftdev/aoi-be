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
		DeviceNama:     deviceNama,
		RawValue:       rawValue,
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

func (w *SNMPWorker) handleError(paramID uint, paramNama, paramType string, err error) {
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
