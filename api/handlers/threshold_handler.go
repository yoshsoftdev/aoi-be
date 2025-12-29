package handlers

import (
	"net/http"
	"strconv"

	"aoi-backend/internal/core/logger"
	"aoi-backend/internal/domain"
	"aoi-backend/internal/repository"

	"github.com/gin-gonic/gin"
)

type ThresholdHandler struct {
	repo repository.ThresholdRepository
}

func NewThresholdHandler(repo repository.ThresholdRepository) *ThresholdHandler {
	return &ThresholdHandler{
		repo: repo,
	}
}

// CreateOrUpdateThresholdRequest represents the request payload for creating/updating threshold
type CreateOrUpdateThresholdRequest struct {
	ID            *uint    `json:"id"` // Optional: Threshold ID (for update)
	IDParameter   uint     `json:"id_param" binding:"required"`
	Unit          string   `json:"unit"`           // Optional: Unit for display
	ThresholdType string   `json:"threshold_type"` // Optional: "max" or "min" (default: "max")
	MinValue      *float64 `json:"min_value"`      // Optional: Minimum normal value
	MaxValue      *float64 `json:"max_value"`      // Optional: Maximum normal value
	CriticalLow   *float64 `json:"critical_low"`   // Optional: Critical low threshold
	CriticalHigh  *float64 `json:"critical_high"`  // Optional: Critical high threshold
	WarningLow    *float64 `json:"warning_low"`    // Optional: Warning low threshold
	WarningHigh   *float64 `json:"warning_high"`   // Optional: Warning high threshold
}

// CreateOrUpdate handles POST /api/tracehold
// Creates or updates threshold configuration for a parameter
// If "id" is provided, updates the threshold with that ID
// If "id" is not provided, creates or updates based on id_param (upsert)
func (h *ThresholdHandler) CreateOrUpdate(c *gin.Context) {
	var req CreateOrUpdateThresholdRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	// Validate threshold_type
	validTypes := map[string]bool{
		"max":      true,
		"min":      true,
		"min-max":  true,
		"abnormal": true,
	}
	if req.ThresholdType != "" && !validTypes[req.ThresholdType] {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "threshold_type must be 'max', 'min', 'min-max', or 'abnormal'",
		})
		return
	}

	// Set default threshold_type if empty
	thresholdType := req.ThresholdType
	if thresholdType == "" {
		thresholdType = "max"
	}

	// Helper function to get float64 value or default
	getFloatValue := func(ptr *float64, defaultValue float64) float64 {
		if ptr != nil {
			return *ptr
		}
		return defaultValue
	}

	// If ID is provided, try to update existing threshold
	if req.ID != nil && *req.ID > 0 {
		// Get existing threshold
		existing, err := h.repo.GetByID(*req.ID)
		if err != nil {
			// If threshold with ID not found, fallback to create/update by id_param
			// This allows flexibility: if ID is wrong, still create/update based on parameter
			logger.GetLogger().Warnf("Threshold with ID %d not found, falling back to create/update by parameter_id %d", *req.ID, req.IDParameter)
			// Continue to create/update logic below (don't return here)
		} else {
			// Threshold found, update it

			// Update threshold domain object (only update fields that are provided)
			existing.IDParameter = req.IDParameter
			if req.Unit != "" {
				existing.Unit = req.Unit
			}
			existing.ThresholdType = thresholdType
			if req.MinValue != nil {
				existing.MinValue = *req.MinValue
			}
			if req.MaxValue != nil {
				existing.MaxValue = *req.MaxValue
			}
			if req.CriticalLow != nil {
				existing.CriticalLow = *req.CriticalLow
			}
			if req.CriticalHigh != nil {
				existing.CriticalHigh = *req.CriticalHigh
			}
			if req.WarningLow != nil {
				existing.WarningLow = *req.WarningLow
			}
			if req.WarningHigh != nil {
				existing.WarningHigh = *req.WarningHigh
			}

			if err := h.repo.Update(existing); err != nil {
				logger.GetLogger().Errorf("Failed to update threshold: %v", err)
				c.JSON(http.StatusInternalServerError, gin.H{
					"success": false,
					"error":   "Failed to update threshold configuration",
				})
				return
			}

			c.JSON(http.StatusOK, gin.H{
				"success": true,
				"data":    existing,
				"message": "Threshold configuration updated successfully",
			})
			return
		}
		// If threshold with ID not found, fall through to create/update by id_param
	}

	// If ID is not provided, use CreateOrUpdate (upsert by id_param)
	// Check if threshold already exists for this parameter
	existing, err := h.repo.GetByParameterID(req.IDParameter)
	if err == nil && existing != nil {
		// Update existing threshold (only update fields that are provided)
		if req.Unit != "" {
			existing.Unit = req.Unit
		}
		existing.ThresholdType = thresholdType
		if req.MinValue != nil {
			existing.MinValue = *req.MinValue
		}
		if req.MaxValue != nil {
			existing.MaxValue = *req.MaxValue
		}
		if req.CriticalLow != nil {
			existing.CriticalLow = *req.CriticalLow
		}
		if req.CriticalHigh != nil {
			existing.CriticalHigh = *req.CriticalHigh
		}
		if req.WarningLow != nil {
			existing.WarningLow = *req.WarningLow
		}
		if req.WarningHigh != nil {
			existing.WarningHigh = *req.WarningHigh
		}

		if err := h.repo.Update(existing); err != nil {
			logger.GetLogger().Errorf("Failed to update threshold: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{
				"success": false,
				"error":   "Failed to update threshold configuration",
			})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"success": true,
			"data":    existing,
			"message": "Threshold configuration updated successfully",
		})
		return
	}

	// Create new threshold (all fields are optional, use defaults if not provided)
	threshold := &domain.Threshold{
		IDParameter:   req.IDParameter,
		Unit:          req.Unit,
		ThresholdType: thresholdType,
		MinValue:      getFloatValue(req.MinValue, 0),
		MaxValue:      getFloatValue(req.MaxValue, 100),
		CriticalLow:   getFloatValue(req.CriticalLow, 0),
		CriticalHigh:  getFloatValue(req.CriticalHigh, 100),
		WarningLow:    getFloatValue(req.WarningLow, 0),
		WarningHigh:   getFloatValue(req.WarningHigh, 100),
	}

	// Use Create to create new threshold
	if err := h.repo.Create(threshold); err != nil {
		logger.GetLogger().Errorf("Failed to create threshold: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   "Failed to save threshold configuration",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data":    threshold,
		"message": "Threshold configuration saved successfully",
	})
}

// GetByParameterID handles GET /api/tracehold/:parameter_id
func (h *ThresholdHandler) GetByParameterID(c *gin.Context) {
	paramIDStr := c.Param("parameter_id")
	paramID, err := strconv.ParseUint(paramIDStr, 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "Invalid parameter ID",
		})
		return
	}

	threshold, err := h.repo.GetByParameterID(uint(paramID))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{
			"success": false,
			"error":   "Threshold not found",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data":    threshold,
	})
}

// GetAll handles GET /api/tracehold
func (h *ThresholdHandler) GetAll(c *gin.Context) {
	thresholds, err := h.repo.GetAll()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data":    thresholds,
	})
}

// Update handles PUT /api/tracehold/:id
// Updates threshold configuration by ID
func (h *ThresholdHandler) Update(c *gin.Context) {
	idStr := c.Param("id")
	id, err := strconv.ParseUint(idStr, 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "Invalid ID",
		})
		return
	}

	// Get existing threshold
	existing, err := h.repo.GetByID(uint(id))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{
			"success": false,
			"error":   "Threshold not found",
		})
		return
	}

	var req CreateOrUpdateThresholdRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	// Validate threshold_type
	validTypes := map[string]bool{
		"max":      true,
		"min":      true,
		"min-max":  true,
		"abnormal": true,
	}
	if req.ThresholdType != "" && !validTypes[req.ThresholdType] {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "threshold_type must be 'max', 'min', 'min-max', or 'abnormal'",
		})
		return
	}

	// Set default threshold_type if empty
	thresholdType := req.ThresholdType
	if thresholdType == "" {
		thresholdType = "max"
	}

	// Update threshold domain object (only update fields that are provided)
	existing.IDParameter = req.IDParameter
	if req.Unit != "" {
		existing.Unit = req.Unit
	}
	existing.ThresholdType = thresholdType
	if req.MinValue != nil {
		existing.MinValue = *req.MinValue
	}
	if req.MaxValue != nil {
		existing.MaxValue = *req.MaxValue
	}
	if req.CriticalLow != nil {
		existing.CriticalLow = *req.CriticalLow
	}
	if req.CriticalHigh != nil {
		existing.CriticalHigh = *req.CriticalHigh
	}
	if req.WarningLow != nil {
		existing.WarningLow = *req.WarningLow
	}
	if req.WarningHigh != nil {
		existing.WarningHigh = *req.WarningHigh
	}

	if err := h.repo.Update(existing); err != nil {
		logger.GetLogger().Errorf("Failed to update threshold: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   "Failed to update threshold configuration",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data":    existing,
		"message": "Threshold configuration updated successfully",
	})
}

// Delete handles DELETE /api/tracehold/:parameter_id
// Deletes threshold configuration by parameter ID
func (h *ThresholdHandler) Delete(c *gin.Context) {
	paramIDStr := c.Param("parameter_id")
	paramID, err := strconv.ParseUint(paramIDStr, 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "Invalid parameter ID",
		})
		return
	}

	// Check if threshold exists for this parameter
	_, err = h.repo.GetByParameterID(uint(paramID))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{
			"success": false,
			"error":   "Threshold not found for this parameter",
		})
		return
	}

	if err := h.repo.DeleteByParameterID(uint(paramID)); err != nil {
		logger.GetLogger().Errorf("Failed to delete threshold: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   "Failed to delete threshold configuration",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"message": "Threshold deleted successfully",
	})
}
