package handlers

import (
	"aoi-backend/internal/core/logger"
	"aoi-backend/internal/domain"
	"aoi-backend/internal/service"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
)

type SetPointHandler struct {
	svc            *service.SetPointService
	workflowEngine *service.WorkflowEngine
}

func NewSetPointHandler(svc *service.SetPointService, engine *service.WorkflowEngine) *SetPointHandler {
	return &SetPointHandler{
		svc:            svc,
		workflowEngine: engine,
	}
}

// GetAll returns all set-points.
func (h *SetPointHandler) GetAll(c *gin.Context) {
	items, err := h.svc.GetAll()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data":    items,
		"meta": gin.H{
			"total": len(items),
		},
	})
}

// GetByID returns a single set-point.
func (h *SetPointHandler) GetByID(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "invalid set-point id",
		})
		return
	}
	item, err := h.svc.GetByID(uint(id))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{
			"success": false,
			"error":   "set-point not found",
		})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data":    item,
	})
}

type createSetPointRequest struct {
	Name        string   `json:"name" binding:"required"`
	Unit        *string  `json:"unit"`
	Value       float64  `json:"value"`
	DataType    string   `json:"data_type"`
	MinValue    *float64 `json:"min_value"`
	MaxValue    *float64 `json:"max_value"`
	Description *string  `json:"description"`
}

func (h *SetPointHandler) Create(c *gin.Context) {
	var req createSetPointRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	if req.DataType == "" {
		req.DataType = "float"
	}

	item := &domain.SetPoint{
		Name:        req.Name,
		Unit:        req.Unit,
		Value:       req.Value,
		DataType:    req.DataType,
		MinValue:    req.MinValue,
		MaxValue:    req.MaxValue,
		Description: req.Description,
	}
	if err := h.svc.Create(item); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}
	c.JSON(http.StatusCreated, gin.H{
		"success": true,
		"data":    item,
	})
}

type updateSetPointRequest struct {
	Name        *string  `json:"name"`
	Unit        *string  `json:"unit"`
	Value       *float64 `json:"value"`
	DataType    *string  `json:"data_type"`
	MinValue    *float64 `json:"min_value"`
	MaxValue    *float64 `json:"max_value"`
	Description *string  `json:"description"`
}

func (h *SetPointHandler) Update(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "invalid set-point id",
		})
		return
	}

	// Get existing set-point first
	existing, err := h.svc.GetByID(uint(id))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{
			"success": false,
			"error":   "set-point not found",
		})
		return
	}

	var req updateSetPointRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	// Log incoming update for debugging
	logger.GetLogger().Infof("[SETPOINT] Update request: id=%d, old_value=%f, new_value_ptr=%v", id, existing.Value, req.Value)

	// Update only fields that are provided
	item := &domain.SetPoint{
		ID:          uint(id),
		Name:        existing.Name,
		Unit:        existing.Unit,
		Value:       existing.Value,
		DataType:    existing.DataType,
		MinValue:    existing.MinValue,
		MaxValue:    existing.MaxValue,
		Description: existing.Description,
	}

	if req.Name != nil {
		item.Name = *req.Name
	}
	if req.Unit != nil {
		item.Unit = req.Unit
	}
	valueChanged := false
	if req.Value != nil {
		item.Value = *req.Value
		if *req.Value != existing.Value {
			valueChanged = true
		}
	}
	if req.DataType != nil {
		item.DataType = *req.DataType
	}
	if req.MinValue != nil {
		item.MinValue = req.MinValue
	}
	if req.MaxValue != nil {
		item.MaxValue = req.MaxValue
	}
	if req.Description != nil {
		item.Description = req.Description
	}

	if err := h.svc.Update(item); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	// Explicitly trigger workflow engine for SETPOINT_CHANGED workflows
	if valueChanged && h.workflowEngine != nil {
		logger.GetLogger().Infof("[SETPOINT] Value changed for id=%d, triggering workflows", item.ID)
		h.workflowEngine.OnSetPointChanged(item.ID)
	}
	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data":    item,
	})
}

func (h *SetPointHandler) Delete(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "invalid set-point id",
		})
		return
	}
	if err := h.svc.Delete(uint(id)); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"message": "set-point deleted",
	})
}
