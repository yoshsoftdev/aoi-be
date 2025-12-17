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
	repo        repository.SNMPRepository
	paramRepo   repository.ParameterRepository
	deviceRepo  repository.DeviceRepository
	cache       cache.Cache
	formulaEval *service.FormulaEvaluator
	wsHub       *WebSocketHub
}

func NewSNMPWorker(
	repo repository.SNMPRepository,
	paramRepo repository.ParameterRepository,
	deviceRepo repository.DeviceRepository,
	cache cache.Cache,
	formulaEval *service.FormulaEvaluator,
	wsHub *WebSocketHub,
) *SNMPWorker {
	return &SNMPWorker{
		repo:        repo,
		paramRepo:   paramRepo,
		deviceRepo:  deviceRepo,
		cache:       cache,
		formulaEval: formulaEval,
		wsHub:       wsHub,
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

	// Create realtime data
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

	// Save to cache
	cacheKey := fmt.Sprintf("parameter:%d", config.IDParameter)
	if err := w.cache.Set(cacheKey, realtimeData, 24*time.Hour); err != nil {
		logger.GetLogger().Warnf("Failed to save to cache: %v", err)
	}

	// Broadcast via WebSocket
	w.wsHub.BroadcastDataUpdate(realtimeData)

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

	cacheKey := fmt.Sprintf("parameter:%d", paramID)
	w.cache.Set(cacheKey, realtimeData, 24*time.Hour)
	w.wsHub.BroadcastError(realtimeData)
}
