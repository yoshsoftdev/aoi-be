package handlers

import (
	"fmt"
	"net/http"
	"strconv"

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
}

func NewParameterHandler(service service.ParameterService, writeService *service.WriteService, schedulerManager *scheduler.SchedulerManager) *ParameterHandler {
	return &ParameterHandler{
		service:          service,
		writeService:     writeService,
		schedulerManager: schedulerManager,
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
