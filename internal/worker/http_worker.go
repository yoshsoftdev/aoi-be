package worker

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"aoi-backend/internal/core/cache"
	"aoi-backend/internal/core/logger"
	"aoi-backend/internal/domain"
	"aoi-backend/internal/repository"
	"aoi-backend/internal/service"
)

type HTTPWorker struct {
	repo             repository.HTTPRepository
	paramRepo        repository.ParameterRepository
	cache            cache.Cache
	formulaEval      *service.FormulaEvaluator
	thresholdService *service.ThresholdService
	wsHub            *WebSocketHub
}

func NewHTTPWorker(
	repo repository.HTTPRepository,
	paramRepo repository.ParameterRepository,
	cache cache.Cache,
	formulaEval *service.FormulaEvaluator,
	thresholdService *service.ThresholdService,
	wsHub *WebSocketHub,
) *HTTPWorker {
	return &HTTPWorker{
		repo:             repo,
		paramRepo:        paramRepo,
		cache:            cache,
		formulaEval:      formulaEval,
		thresholdService: thresholdService,
		wsHub:            wsHub,
	}
}

// Execute performs HTTP GET request and updates parameter cache
func (w *HTTPWorker) Execute(ctx context.Context, configID uint) error {
	config, err := w.repo.GetByID(configID)
	if err != nil {
		return fmt.Errorf("failed to get HTTP config: %w", err)
	}

	param, err := w.paramRepo.GetByID(config.IDParameter)
	if err != nil {
		return fmt.Errorf("failed to get parameter: %w", err)
	}

	// Create HTTP client with timeout
	client := &http.Client{
		Timeout: time.Duration(config.TimeoutMs) * time.Millisecond,
	}

	// Create request
	req, err := http.NewRequestWithContext(ctx, config.Method, config.URL, nil)
	if err != nil {
		w.handleError(config.IDParameter, param.Nama, param.Type, err)
		return fmt.Errorf("failed to create HTTP request: %w", err)
	}

	// Set Basic Auth if provided
	if config.Username != nil && config.Password != nil && *config.Username != "" {
		auth := base64.StdEncoding.EncodeToString([]byte(*config.Username + ":" + *config.Password))
		req.Header.Set("Authorization", "Basic "+auth)
	}

	// Set custom headers if provided
	if config.Headers != nil && *config.Headers != "" {
		var headers map[string]string
		if err := json.Unmarshal([]byte(*config.Headers), &headers); err == nil {
			for key, value := range headers {
				req.Header.Set(key, value)
			}
		}
	}

	// Set default Content-Type if not provided
	if req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}

	// Perform HTTP request
	resp, err := client.Do(req)
	if err != nil {
		w.handleError(config.IDParameter, param.Nama, param.Type, err)
		return fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	// Check status code
	if resp.StatusCode != http.StatusOK {
		err := fmt.Errorf("HTTP request returned status %d: %s", resp.StatusCode, resp.Status)
		w.handleError(config.IDParameter, param.Nama, param.Type, err)
		return err
	}

	// Parse JSON response
	var jsonData map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&jsonData); err != nil {
		w.handleError(config.IDParameter, param.Nama, param.Type, err)
		return fmt.Errorf("failed to decode JSON response: %w", err)
	}

	// Parse field mappings
	var fieldMappings map[string]string
	if err := json.Unmarshal([]byte(config.FieldMappings), &fieldMappings); err != nil {
		w.handleError(config.IDParameter, param.Nama, param.Type, err)
		return fmt.Errorf("failed to parse field mappings: %w", err)
	}

	// Get value path from field mappings
	valuePath, ok := fieldMappings["value"]
	if !ok || valuePath == "" {
		err := fmt.Errorf("field mapping 'value' is missing or empty")
		w.handleError(config.IDParameter, param.Nama, param.Type, err)
		return err
	}

	// Extract value from JSON using field mappings
	rawValue := w.extractFieldFloat(jsonData, valuePath)
	processedValue := rawValue

	// Evaluate formula if exists
	if param.CustomRumus != nil && *param.CustomRumus != "" {
		processed, err := w.formulaEval.Evaluate(*param.CustomRumus, rawValue)
		if err == nil {
			processedValue = processed
		}
	}

	// Get device name from parameter (if device is preloaded)
	var deviceNama string
	if param.Device.ID > 0 {
		deviceNama = param.Device.Nama
	}

	// Determine parameter mode (default to DIRECT)
	paramMode := param.ParameterMode
	if paramMode == "" {
		paramMode = domain.ParameterModeDIRECT
	}

	// Only process SOURCE & DIRECT parameters
	// CLEAN & FORMULA are handled by Formula Engine, not workers
	if paramMode != domain.ParameterModeSOURCE && paramMode != domain.ParameterModeDIRECT {
		return nil
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
		AlarmStatus:    domain.AlarmStatusNormal,
	}

	// Save RAW data to internal cache (for SOURCE & DIRECT)
	rawCacheKey := fmt.Sprintf("parameter:raw:%d", config.IDParameter)
	if err := w.cache.Set(rawCacheKey, realtimeData, 24*time.Hour); err != nil {
		// Silent fail
	}

	// Handle SOURCE vs DIRECT differently
	if paramMode == domain.ParameterModeSOURCE {
		// SOURCE: Internal only, tidak dibroadcast
		// Only notify Formula Engine (for CLEAN derivation)
		// Note: Formula Engine integration would go here if needed
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
			ProcessedValue: processedValue, // This is the CLEAN value (ALWAYS numeric)
			CustomRumus:    param.Unit,     // Use CustomRumus field to pass unit (temporary)
			Timestamp:      time.Now(),
			Status:         "online",
			Error:          nil,
			AlarmStatus:    domain.AlarmStatusNormal,
		}

		// Apply threshold to calculate alarm status
		if w.thresholdService != nil {
			w.thresholdService.ApplyThreshold(&cleanData)
		}

		// Save CLEAN data to public cache
		cleanCacheKey := fmt.Sprintf("parameter:clean:%d", config.IDParameter)
		if err := w.cache.Set(cleanCacheKey, cleanData, 24*time.Hour); err != nil {
			// Silent fail
		}

		// Broadcast to WebSocket
		if w.wsHub != nil {
			w.wsHub.BroadcastDataUpdate(cleanData)
		}
	}

	logger.GetLogger().Infof("HTTP parameter %s (%d) updated: value=%.2f", param.Nama, config.IDParameter, processedValue)

	return nil
}

// extractField extracts a field from JSON using dot notation path (e.g., "data.temperature")
func (w *HTTPWorker) extractField(jsonData map[string]interface{}, path string) int {
	if path == "" {
		return 0
	}

	value := w.getNestedValue(jsonData, path)
	if value == nil {
		return 0
	}

	switch v := value.(type) {
	case float64:
		return int(v)
	case int:
		return v
	case string:
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
		return 0
	default:
		return 0
	}
}

// extractFieldString extracts a string field from JSON
func (w *HTTPWorker) extractFieldString(jsonData map[string]interface{}, path string) string {
	if path == "" {
		return ""
	}

	value := w.getNestedValue(jsonData, path)
	if value == nil {
		return ""
	}

	switch v := value.(type) {
	case string:
		return v
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case int:
		return strconv.Itoa(v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// extractFieldFloat extracts a float field from JSON
func (w *HTTPWorker) extractFieldFloat(jsonData map[string]interface{}, path string) float64 {
	if path == "" {
		return 0
	}

	value := w.getNestedValue(jsonData, path)
	if value == nil {
		return 0
	}

	switch v := value.(type) {
	case float64:
		return v
	case float32:
		return float64(v)
	case int:
		return float64(v)
	case int64:
		return float64(v)
	case string:
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
		return 0
	default:
		return 0
	}
}

// getNestedValue gets a value from nested JSON using dot notation (e.g., "data.temperature" or "data.0.temperature")
// Supports array indices in path (e.g., "data.0.value" or "items.1.temp")
func (w *HTTPWorker) getNestedValue(data map[string]interface{}, path string) interface{} {
	if path == "" {
		return nil
	}

	parts := strings.Split(path, ".")
	current := interface{}(data)

	for _, part := range parts {
		if part == "" {
			continue
		}

		// Check if current is a map
		if m, ok := current.(map[string]interface{}); ok {
			current = m[part]
			if current == nil {
				return nil
			}
		} else if arr, ok := current.([]interface{}); ok {
			// Current is an array, try to parse part as index
			idx, err := strconv.Atoi(part)
			if err != nil || idx < 0 || idx >= len(arr) {
				return nil
			}
			current = arr[idx]
		} else {
			return nil
		}

		if current == nil {
			return nil
		}
	}

	return current
}

// handleError logs error and updates cache with error state
func (w *HTTPWorker) handleError(paramID uint, paramName, paramType string, err error) {
	logger.GetLogger().Errorf("HTTP worker error for parameter %s (%d): %v", paramName, paramID, err)

	// Update cache with error state
	errorData := domain.RealtimeData{
		ParameterID:    paramID,
		ParameterNama:  paramName,
		ParameterType:  paramType,
		DeviceNama:     "",
		RawValue:       nil,
		ProcessedValue: float64(0),
		CustomRumus:    nil,
		Timestamp:      time.Now(),
		Status:         "error",
		Error:          &[]string{err.Error()}[0],
		AlarmStatus:    domain.AlarmStatusAbnormal,
	}

	cleanCacheKey := fmt.Sprintf("parameter:clean:%d", paramID)
	w.cache.Set(cleanCacheKey, errorData, 24*time.Hour)
}
