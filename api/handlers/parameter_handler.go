package handlers

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"aoi-backend/internal/core/errors"
	"aoi-backend/internal/core/logger"
	"aoi-backend/internal/domain"
	"aoi-backend/internal/scheduler"
	"aoi-backend/internal/service"

	"github.com/gin-gonic/gin"
)

type ParameterHandler struct {
	service          service.ParameterService
	writeService     *service.WriteService
	schedulerManager *scheduler.SchedulerManager
	formulaEngine    *service.FormulaEngine
}

func NewParameterHandler(service service.ParameterService, writeService *service.WriteService, schedulerManager *scheduler.SchedulerManager, formulaEngine *service.FormulaEngine) *ParameterHandler {
	return &ParameterHandler{
		service:          service,
		writeService:     writeService,
		schedulerManager: schedulerManager,
		formulaEngine:    formulaEngine,
	}
}

func (h *ParameterHandler) GetAll(c *gin.Context) {
	params, err := h.service.GetAll()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	// Debug: log the number of parameters retrieved
	fmt.Printf("GetAll: Retrieved %d parameters\n", len(params))

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data":    params,
		"meta": gin.H{
			"total": len(params),
		},
	})
}

func (h *ParameterHandler) GetByID(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "Invalid parameter ID",
		})
		return
	}

	param, err := h.service.GetByID(uint(id))
	if err != nil {
		if appErr, ok := err.(*errors.AppError); ok {
			c.JSON(appErr.Code, gin.H{
				"success": false,
				"error":   appErr.Message,
			})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data":    param,
	})
}

func (h *ParameterHandler) Create(c *gin.Context) {
	var param domain.Parameter
	if err := c.ShouldBindJSON(&param); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	// Validate write_mode for WRITE parameters
	if param.Direction == "WRITE" {
		if param.WriteMode == nil || (*param.WriteMode != "PRESET" && *param.WriteMode != "DYNAMIC") {
			c.JSON(http.StatusBadRequest, gin.H{
				"success": false,
				"error":   "WRITE parameters must have write_mode set to 'PRESET' or 'DYNAMIC'",
			})
			return
		}

		// PRESET mode must have preset_value
		if *param.WriteMode == "PRESET" {
			if param.PresetValue == nil || *param.PresetValue == "" {
				c.JSON(http.StatusBadRequest, gin.H{
					"success": false,
					"error":   "PRESET write_mode requires preset_value to be set",
				})
				return
			}
		}

		// DYNAMIC mode must NOT have preset_value
		if *param.WriteMode == "DYNAMIC" {
			if param.PresetValue != nil && *param.PresetValue != "" {
				c.JSON(http.StatusBadRequest, gin.H{
					"success": false,
					"error":   "DYNAMIC write_mode cannot have preset_value",
				})
				return
			}
		}
	} else {
		// READ parameters should not have write_mode or preset_value
		if param.WriteMode != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"success": false,
				"error":   "READ parameters cannot have write_mode",
			})
			return
		}
		if param.PresetValue != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"success": false,
				"error":   "READ parameters cannot have preset_value",
			})
			return
		}
	}

	if err := h.service.Create(&param); err != nil {
		if appErr, ok := err.(*errors.AppError); ok {
			c.JSON(appErr.Code, gin.H{
				"success": false,
				"error":   appErr.Message,
			})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	// Reload scheduler ONLY for READ parameters
	// WRITE parameters are command/on-demand and should NOT be scheduled
	if h.schedulerManager != nil && param.Direction == "READ" {
		logger.GetLogger().Info("Reloading scheduler after READ parameter creation")
		h.schedulerManager.Reload()
	} else if param.Direction == "WRITE" {
		logger.GetLogger().Info("WRITE parameter created - skipping scheduler reload (WRITE parameters are not scheduled)")
	}

	// Reload formula engine if this is a formula parameter
	if param.IsFormula && h.formulaEngine != nil {
		logger.GetLogger().Info("Reloading formula engine after formula parameter creation")
		if err := h.formulaEngine.LoadAll(); err != nil {
			logger.GetLogger().Warnf("Failed to reload formula engine: %v", err)
		} else {
			logger.GetLogger().Info("Formula engine reloaded successfully")
			// Evaluate all formulas after reload to ensure all appear in dashboard
			go func() {
				// Wait a bit for cache to be ready
				time.Sleep(200 * time.Millisecond)
				h.formulaEngine.EvaluateAll()
			}()
		}
	}

	c.JSON(http.StatusCreated, gin.H{
		"success": true,
		"data":    param,
	})
}

func (h *ParameterHandler) Update(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "Invalid parameter ID",
		})
		return
	}

	var param domain.Parameter
	if err := c.ShouldBindJSON(&param); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	// Validate write_mode for WRITE parameters
	if param.Direction == "WRITE" {
		if param.WriteMode == nil || (*param.WriteMode != "PRESET" && *param.WriteMode != "DYNAMIC") {
			c.JSON(http.StatusBadRequest, gin.H{
				"success": false,
				"error":   "WRITE parameters must have write_mode set to 'PRESET' or 'DYNAMIC'",
			})
			return
		}

		// PRESET mode must have preset_value
		if *param.WriteMode == "PRESET" {
			if param.PresetValue == nil || *param.PresetValue == "" {
				c.JSON(http.StatusBadRequest, gin.H{
					"success": false,
					"error":   "PRESET write_mode requires preset_value to be set",
				})
				return
			}
		}

		// DYNAMIC mode must NOT have preset_value
		if *param.WriteMode == "DYNAMIC" {
			if param.PresetValue != nil && *param.PresetValue != "" {
				c.JSON(http.StatusBadRequest, gin.H{
					"success": false,
					"error":   "DYNAMIC write_mode cannot have preset_value",
				})
				return
			}
		}
	} else {
		// READ parameters should not have write_mode or preset_value
		if param.WriteMode != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"success": false,
				"error":   "READ parameters cannot have write_mode",
			})
			return
		}
		if param.PresetValue != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"success": false,
				"error":   "READ parameters cannot have preset_value",
			})
			return
		}
	}

	if err := h.service.Update(uint(id), &param); err != nil {
		if appErr, ok := err.(*errors.AppError); ok {
			c.JSON(appErr.Code, gin.H{
				"success": false,
				"error":   appErr.Message,
			})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	// Reload scheduler ONLY for READ parameters
	// WRITE parameters are command/on-demand and should NOT be scheduled
	if h.schedulerManager != nil && param.Direction == "READ" {
		logger.GetLogger().Info("Reloading scheduler after READ parameter update")
		h.schedulerManager.Reload()
	} else if param.Direction == "WRITE" {
		logger.GetLogger().Info("WRITE parameter updated - skipping scheduler reload (WRITE parameters are not scheduled)")
	}

	// Reload formula engine if this is a formula parameter or if any formula might be affected
	if h.formulaEngine != nil {
		logger.GetLogger().Info("Reloading formula engine after parameter update")
		if err := h.formulaEngine.LoadAll(); err != nil {
			logger.GetLogger().Warnf("Failed to reload formula engine: %v", err)
		} else {
			// Evaluate all formulas after reload
			go func() {
				time.Sleep(200 * time.Millisecond)
				h.formulaEngine.EvaluateAll()
			}()
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data":    param,
	})
}

func (h *ParameterHandler) Delete(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "Invalid parameter ID",
		})
		return
	}

	if err := h.service.Delete(uint(id)); err != nil {
		if appErr, ok := err.(*errors.AppError); ok {
			c.JSON(appErr.Code, gin.H{
				"success": false,
				"error":   appErr.Message,
			})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	// Reload scheduler to remove deleted parameter
	if h.schedulerManager != nil {
		logger.GetLogger().Info("Reloading scheduler after parameter deletion")
		h.schedulerManager.Reload()
	}

	// Reload formula engine to remove deleted parameter from registry
	if h.formulaEngine != nil {
		logger.GetLogger().Info("Reloading formula engine after parameter deletion")
		if err := h.formulaEngine.LoadAll(); err != nil {
			logger.GetLogger().Warnf("Failed to reload formula engine: %v", err)
		} else {
			// Evaluate all remaining formulas after reload
			go func() {
				time.Sleep(200 * time.Millisecond)
				h.formulaEngine.EvaluateAll()
			}()
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"message": "Parameter deleted successfully",
	})
}

// Write handles write operations for a parameter
func (h *ParameterHandler) Write(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "Invalid parameter ID",
		})
		return
	}

	var req service.WriteRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   fmt.Sprintf("Invalid request: %v", err),
		})
		return
	}

	// Validate mode
	if req.Mode != "TEST" && req.Mode != "MANUAL" && req.Mode != "PRESET" {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "Invalid mode. Must be TEST, MANUAL, or PRESET",
		})
		return
	}

	// Execute write
	response, err := h.writeService.Write(c.Request.Context(), uint(id), req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	statusCode := http.StatusOK
	if !response.Success {
		statusCode = http.StatusBadRequest
	}

	c.JSON(statusCode, gin.H{
		"success": response.Success,
		"message": response.Message,
		"data": gin.H{
			"read_back_value": response.ReadBackValue,
		},
		"error": response.Error,
	})
}
