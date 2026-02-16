package handlers

import (
	"net/http"
	"strconv"

	"aoi-backend/internal/core/logger"
	"aoi-backend/internal/service"

	"github.com/gin-gonic/gin"
)

type InternalParameterHandler struct {
	internalParameterService *service.InternalParameterService
	deviceRepo               interface {
		GetByID(id uint) (interface{}, error)
	}
}

func NewInternalParameterHandler(internalParameterService *service.InternalParameterService) *InternalParameterHandler {
	return &InternalParameterHandler{
		internalParameterService: internalParameterService,
	}
}

// CreateRequest represents an internal parameter creation request
type CreateInternalParameterRequest struct {
	Name     string `json:"name" binding:"required"`
	DeviceID uint   `json:"device_id" binding:"required"`
}

// Create creates a new internal parameter
func (h *InternalParameterHandler) Create(c *gin.Context) {
	var req CreateInternalParameterRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	ctx := c.Request.Context()
	param, err := h.internalParameterService.CreateInternalParameter(ctx, req.Name, req.DeviceID)
	if err != nil {
		logger.GetLogger().Errorf("Failed to create internal parameter: %v", err)
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

// SetValueRequest represents a request to set internal parameter value
type SetValueRequest struct {
	Value float64 `json:"value" binding:"required"`
}

// SetValue sets the value of an internal parameter
func (h *InternalParameterHandler) SetValue(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "Invalid parameter ID",
		})
		return
	}

	var req SetValueRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	ctx := c.Request.Context()
	if err := h.internalParameterService.SetValue(ctx, uint(id), req.Value); err != nil {
		logger.GetLogger().Errorf("Failed to set internal parameter value: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
	})
}

// ResetValue resets an internal parameter to 0
func (h *InternalParameterHandler) ResetValue(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "Invalid parameter ID",
		})
		return
	}

	ctx := c.Request.Context()
	if err := h.internalParameterService.ResetValue(ctx, uint(id)); err != nil {
		logger.GetLogger().Errorf("Failed to reset internal parameter: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
	})
}

// GetValue gets the current value of an internal parameter
func (h *InternalParameterHandler) GetValue(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "Invalid parameter ID",
		})
		return
	}

	ctx := c.Request.Context()
	value, err := h.internalParameterService.GetValue(ctx, uint(id))
	if err != nil {
		logger.GetLogger().Errorf("Failed to get internal parameter value: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data": gin.H{
			"value": value,
		},
	})
}
