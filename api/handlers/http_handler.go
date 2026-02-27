package handlers

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"aoi-backend/internal/core/logger"
	"aoi-backend/internal/domain"
	"aoi-backend/internal/repository"
	"aoi-backend/internal/scheduler"
	"aoi-backend/internal/service"

	"github.com/gin-gonic/gin"
)

type HTTPHandler struct {
	repo             repository.HTTPRepository
	koneksiRepo      repository.KoneksiRepository
	paramService     service.ParameterService
	schedulerManager *scheduler.SchedulerManager
}

func NewHTTPHandler(repo repository.HTTPRepository, koneksiRepo repository.KoneksiRepository, paramService service.ParameterService, schedulerManager *scheduler.SchedulerManager) *HTTPHandler {
	return &HTTPHandler{
		repo:             repo,
		koneksiRepo:      koneksiRepo,
		paramService:     paramService,
		schedulerManager: schedulerManager,
	}
}

// getOrCreateHTTPKoneksi finds or creates the "HTTP GET" koneksi
func (h *HTTPHandler) getOrCreateHTTPKoneksi() (*domain.Koneksi, error) {
	// Try to find existing HTTP GET koneksi
	koneksis, err := h.koneksiRepo.GetAll()
	if err != nil {
		return nil, fmt.Errorf("failed to get koneksis: %w", err)
	}

	for _, k := range koneksis {
		if k.Nama == "HTTP GET" {
			return &k, nil
		}
	}

	// Create new HTTP GET koneksi if not found
	httpKoneksi := &domain.Koneksi{
		Nama:       "HTTP GET",
		Type:       "snmp", // Use snmp type as it's the closest match (no http type yet)
		Enabled:    true,
		PollingMs:  60000, // Default 60 seconds
		TimeoutMs:  10000, // Default 10 seconds
		RetryCount: 3,
		Description: func() *string {
			s := "Koneksi untuk HTTP GET parameters"
			return &s
		}(),
	}

	if err := h.koneksiRepo.Create(httpKoneksi); err != nil {
		return nil, fmt.Errorf("failed to create HTTP GET koneksi: %w", err)
	}

	logger.GetLogger().Info("Created HTTP GET koneksi automatically")
	return httpKoneksi, nil
}

func (h *HTTPHandler) GetAll(c *gin.Context) {
	configs, err := h.repo.GetAll()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data":    configs,
	})
}

func (h *HTTPHandler) GetByID(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "Invalid ID",
		})
		return
	}

	config, err := h.repo.GetByID(uint(id))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{
			"success": false,
			"error":   "HTTP config not found",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data":    config,
	})
}

func (h *HTTPHandler) Create(c *gin.Context) {
	var req struct {
		IDParameter   *uint   `json:"id_parameter,omitempty"`   // Optional: if not provided, will create parameter
		ParameterName *string `json:"parameter_name,omitempty"` // Required if id_parameter is not provided
		URL           string  `json:"url" binding:"required"`
		Method        string  `json:"method,omitempty"`
		Username      *string `json:"username,omitempty"`
		Password      *string `json:"password,omitempty"`
		Headers       *string `json:"headers,omitempty"`
		TimeoutMs     int     `json:"timeout_ms,omitempty"`
		PollingMs     int     `json:"polling_ms" binding:"required"`
		FieldMappings string  `json:"field_mappings" binding:"required"`
		Description   *string `json:"description,omitempty"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		logger.GetLogger().Errorf("Failed to bind JSON in HTTP Create: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	// Get or create HTTP GET koneksi first
	httpKoneksi, err := h.getOrCreateHTTPKoneksi()
	if err != nil {
		logger.GetLogger().Errorf("Failed to get or create HTTP GET koneksi: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   fmt.Sprintf("Failed to get or create HTTP GET koneksi: %v", err),
		})
		return
	}

	var param *domain.Parameter
	var paramID uint

	// If id_parameter is provided, use existing parameter
	if req.IDParameter != nil && *req.IDParameter > 0 {
		paramID = *req.IDParameter
		param, err = h.paramService.GetByID(paramID)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"success": false,
				"error":   fmt.Sprintf("Parameter not found: %v", err),
			})
			return
		}
	} else if req.ParameterName != nil && *req.ParameterName != "" {
		// Create new parameter if parameter_name is provided
		newParam := &domain.Parameter{
			DeviceID:      httpKoneksi.ID,
			Nama:          *req.ParameterName,
			Type:          "snmp", // Use 'snmp' as type for now
			Direction:     "READ",
			ReadWrite:     0, // READ only
			ParameterMode: domain.ParameterModeDIRECT,
			IsFormula:     false,
		}

		if err := h.paramService.Create(newParam); err != nil {
			logger.GetLogger().Errorf("Failed to create parameter: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{
				"success": false,
				"error":   fmt.Sprintf("Failed to create parameter: %v", err),
			})
			return
		}

		param = newParam
		paramID = newParam.ID
		logger.GetLogger().Infof("Created new parameter %d (%s) for HTTP config", paramID, *req.ParameterName)
	} else {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "Either id_parameter or parameter_name must be provided",
		})
		return
	}

	// Update parameter's device_id to HTTP GET koneksi if not already set
	if param.DeviceID != httpKoneksi.ID {
		param.DeviceID = httpKoneksi.ID
		if err := h.paramService.Update(paramID, param); err != nil {
			logger.GetLogger().Warnf("Failed to update parameter device_id: %v", err)
			// Continue anyway, not critical
		} else {
			logger.GetLogger().Infof("Updated parameter %d device_id to HTTP GET koneksi %d", paramID, httpKoneksi.ID)
		}
	}

	logger.GetLogger().Infof("Creating HTTP config: IDParameter=%d, URL=%s, PollingMs=%d", paramID, req.URL, req.PollingMs)

	// Build HTTPConfig from request
	config := domain.HTTPConfig{
		IDParameter:   paramID,
		URL:           req.URL,
		Method:        req.Method,
		Username:      req.Username,
		Password:      req.Password,
		Headers:       req.Headers,
		TimeoutMs:     req.TimeoutMs,
		PollingMs:     req.PollingMs,
		FieldMappings: req.FieldMappings,
		Description:   req.Description,
	}

	// Validate required fields
	if config.URL == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "URL is required",
		})
		return
	}

	if config.FieldMappings == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "Field mappings are required",
		})
		return
	}

	if config.PollingMs <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "Polling interval must be greater than 0",
		})
		return
	}

	// Set default method if not provided
	if config.Method == "" {
		config.Method = "GET"
	}

	// Set default timeout if not provided
	if config.TimeoutMs == 0 {
		config.TimeoutMs = 5000
	}

	if err := h.repo.Create(&config); err != nil {
		logger.GetLogger().Errorf("Failed to create HTTP config: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	// Reload scheduler to pick up new HTTP config
	if h.schedulerManager != nil {
		logger.GetLogger().Info("Reloading scheduler after HTTP config creation")
		h.schedulerManager.Reload()
	}

	c.JSON(http.StatusCreated, gin.H{
		"success": true,
		"data":    config,
	})
}

func (h *HTTPHandler) Update(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "Invalid ID",
		})
		return
	}

	var config domain.HTTPConfig
	if err := c.ShouldBindJSON(&config); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	config.ID = uint(id)

	// Validate required fields
	if config.URL == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "URL is required",
		})
		return
	}

	if config.FieldMappings == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "Field mappings are required",
		})
		return
	}

	if config.PollingMs <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "Polling interval must be greater than 0",
		})
		return
	}

	if err := h.repo.Update(&config); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	// Reload scheduler to pick up updated HTTP config
	if h.schedulerManager != nil {
		logger.GetLogger().Info("Reloading scheduler after HTTP config update")
		h.schedulerManager.Reload()
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data":    config,
	})
}

func (h *HTTPHandler) Delete(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "Invalid ID",
		})
		return
	}

	if err := h.repo.Delete(uint(id)); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	// Reload scheduler after deletion
	if h.schedulerManager != nil {
		logger.GetLogger().Info("Reloading scheduler after HTTP config deletion")
		h.schedulerManager.Reload()
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"message": "HTTP config deleted successfully",
	})
}

// TestFetch tests an HTTP GET request with the provided configuration
func (h *HTTPHandler) TestFetch(c *gin.Context) {
	var req struct {
		URL       string            `json:"url" binding:"required"`
		Method    string            `json:"method,omitempty"`
		Username  *string           `json:"username,omitempty"`
		Password  *string           `json:"password,omitempty"`
		Headers   map[string]string `json:"headers,omitempty"`
		TimeoutMs int               `json:"timeout_ms,omitempty"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	// Set defaults
	method := req.Method
	if method == "" {
		method = "GET"
	}
	timeoutMs := req.TimeoutMs
	if timeoutMs <= 0 {
		timeoutMs = 5000 // Default 5 seconds
	}

	// Create HTTP client
	client := &http.Client{
		Timeout: time.Duration(timeoutMs) * time.Millisecond,
	}

	// Create request
	httpReq, err := http.NewRequest(method, req.URL, nil)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   fmt.Sprintf("Failed to create request: %v", err),
		})
		return
	}

	// Set Basic Auth if provided
	if req.Username != nil && req.Password != nil && *req.Username != "" {
		httpReq.SetBasicAuth(*req.Username, *req.Password)
	}

	// Set custom headers
	if req.Headers != nil {
		for key, value := range req.Headers {
			httpReq.Header.Set(key, value)
		}
	}

	// Set default Content-Type if not provided
	if httpReq.Header.Get("Content-Type") == "" {
		httpReq.Header.Set("Content-Type", "application/json")
	}

	// Perform HTTP request
	resp, err := client.Do(httpReq)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   fmt.Sprintf("HTTP request failed: %v", err),
		})
		return
	}
	defer resp.Body.Close()

	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   fmt.Sprintf("Failed to read response: %v", err),
		})
		return
	}

	// Try to parse as JSON
	var jsonData interface{}
	if err := json.Unmarshal(body, &jsonData); err != nil {
		// If not JSON, return as string
		c.JSON(http.StatusOK, gin.H{
			"success": true,
			"data":    string(body),
			"status":  resp.StatusCode,
			"is_json": false,
		})
		return
	}

	// Return JSON data
	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data":    jsonData,
		"status":  resp.StatusCode,
		"is_json": true,
	})
}
