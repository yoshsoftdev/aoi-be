package handlers

import (
	"aoi-backend/internal/domain"
	"aoi-backend/internal/service"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
)

type BypassHandler struct {
	svc          *service.BypassService
	bypassEngine *service.BypassEngine
}

func NewBypassHandler(svc *service.BypassService, bypassEngine *service.BypassEngine) *BypassHandler {
	return &BypassHandler{
		svc:          svc,
		bypassEngine: bypassEngine,
	}
}

func (h *BypassHandler) GetAll(c *gin.Context) {
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

func (h *BypassHandler) GetByID(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "invalid bypass id",
		})
		return
	}
	item, err := h.svc.GetByID(uint(id))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{
			"success": false,
			"error":   "bypass not found",
		})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data":    item,
	})
}

type BypassRuleRequest struct {
	ConditionType  string   `json:"condition_type" binding:"required"`   // always | compare
	LeftSourceType string   `json:"left_source_type" binding:"required"` // set_point | parameter
	LeftSourceID   uint     `json:"left_source_id" binding:"required"`
	Operator       string   `json:"operator"`                            // >, <, =, >=, <= (hanya untuk compare)
	RightValueType string   `json:"right_value_type" binding:"required"` // static | set_point
	RightValue     *float64 `json:"right_value"`
	RightSourceID  *uint    `json:"right_source_id"`
}

type BypassActionRequest struct {
	WriteParameterID uint     `json:"write_parameter_id"`                   // Optional: if writing to parameter
	WriteSetPointID  *uint    `json:"write_set_point_id,omitempty"`         // Optional: if writing to set-point
	ValueSourceType  string   `json:"value_source_type" binding:"required"` // static | set_point
	Value            *float64 `json:"value"`
	SourceSetPointID *uint    `json:"source_set_point_id"`
}

type CreateBypassRequest struct {
	Name        string                `json:"name" binding:"required"`
	Enabled     bool                  `json:"enabled"`
	Description *string               `json:"description"`
	Rules       []BypassRuleRequest   `json:"rules" binding:"required,min=1"`
	Actions     []BypassActionRequest `json:"actions" binding:"required,min=1"`
}

func (h *BypassHandler) Create(c *gin.Context) {
	var req CreateBypassRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	b := domain.Bypass{
		Name:        req.Name,
		Enabled:     req.Enabled,
		Description: req.Description,
	}

	// Map rules
	for _, r := range req.Rules {
		rule := domain.BypassRule{
			ConditionType:  r.ConditionType,
			LeftSourceType: r.LeftSourceType,
			LeftSourceID:   r.LeftSourceID,
			Operator:       r.Operator,
			RightValueType: r.RightValueType,
			RightValue:     r.RightValue,
			RightSourceID:  r.RightSourceID,
		}
		b.Rules = append(b.Rules, rule)
	}

	// Map actions
	for _, a := range req.Actions {
		// Validate: either write_parameter_id or write_set_point_id must be set
		if a.WriteParameterID == 0 && a.WriteSetPointID == nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"success": false,
				"error":   "either write_parameter_id or write_set_point_id must be set",
			})
			return
		}

		action := domain.BypassAction{
			WriteParameterID: a.WriteParameterID,
			WriteSetPointID:  a.WriteSetPointID,
			ValueSourceType:  a.ValueSourceType,
			Value:            a.Value,
			SourceSetPointID: a.SourceSetPointID,
		}
		b.Actions = append(b.Actions, action)
	}

	if err := h.svc.Create(&b); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	// Reload bypass engine to include the new bypass
	if h.bypassEngine != nil {
		if err := h.bypassEngine.LoadAll(); err != nil {
			// Log warning but don't fail the request
			_ = err
		}
	}

	c.JSON(http.StatusCreated, gin.H{
		"success": true,
		"data":    b,
	})
}

type UpdateBypassRequest = CreateBypassRequest

func (h *BypassHandler) Update(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "invalid bypass id",
		})
		return
	}

	var req UpdateBypassRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	b := domain.Bypass{
		ID:          uint(id),
		Name:        req.Name,
		Enabled:     req.Enabled,
		Description: req.Description,
	}

	for _, r := range req.Rules {
		rule := domain.BypassRule{
			ConditionType:  r.ConditionType,
			LeftSourceType: r.LeftSourceType,
			LeftSourceID:   r.LeftSourceID,
			Operator:       r.Operator,
			RightValueType: r.RightValueType,
			RightValue:     r.RightValue,
			RightSourceID:  r.RightSourceID,
		}
		b.Rules = append(b.Rules, rule)
	}

	for _, a := range req.Actions {
		// Validate: either write_parameter_id or write_set_point_id must be set
		if a.WriteParameterID == 0 && a.WriteSetPointID == nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"success": false,
				"error":   "either write_parameter_id or write_set_point_id must be set",
			})
			return
		}

		action := domain.BypassAction{
			WriteParameterID: a.WriteParameterID,
			WriteSetPointID:  a.WriteSetPointID,
			ValueSourceType:  a.ValueSourceType,
			Value:            a.Value,
			SourceSetPointID: a.SourceSetPointID,
		}
		b.Actions = append(b.Actions, action)
	}

	if err := h.svc.Update(&b); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	// Reload bypass engine to reflect the updated bypass
	if h.bypassEngine != nil {
		if err := h.bypassEngine.LoadAll(); err != nil {
			// Log warning but don't fail the request
			_ = err
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data":    b,
	})
}

func (h *BypassHandler) Delete(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "invalid bypass id",
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

	// Reload bypass engine to remove the deleted bypass
	if h.bypassEngine != nil {
		if err := h.bypassEngine.LoadAll(); err != nil {
			// Log warning but don't fail the request
			_ = err
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"message": "bypass deleted",
	})
}
