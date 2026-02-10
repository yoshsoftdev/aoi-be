package handlers

import (
	"aoi-backend/internal/service"
	"net/http"

	"github.com/gin-gonic/gin"
)

type IMCConfigHandler struct {
	svc *service.IMCConfigService
}

func NewIMCConfigHandler(svc *service.IMCConfigService) *IMCConfigHandler {
	return &IMCConfigHandler{svc: svc}
}

func (h *IMCConfigHandler) GetAll(c *gin.Context) {
	configs, err := h.svc.GetAll()
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

func (h *IMCConfigHandler) GetByCardKey(c *gin.Context) {
	cardKey := c.Param("cardKey")
	config, err := h.svc.GetByCardKey(cardKey)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{
			"success": false,
			"error":   "config not found",
		})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data":    config,
	})
}

type CreateOrUpdateIMCConfigRequest struct {
	CardKey     string  `json:"card_key" binding:"required"`
	ParameterID *uint   `json:"parameter_id"`
}

func (h *IMCConfigHandler) CreateOrUpdate(c *gin.Context) {
	var req CreateOrUpdateIMCConfigRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	if err := h.svc.CreateOrUpdate(req.CardKey, req.ParameterID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	// Return the updated config
	config, err := h.svc.GetByCardKey(req.CardKey)
	if err != nil {
		c.JSON(http.StatusOK, gin.H{
			"success": true,
			"message": "config saved",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data":    config,
	})
}

func (h *IMCConfigHandler) Delete(c *gin.Context) {
	cardKey := c.Param("cardKey")
	if err := h.svc.Delete(cardKey); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"message": "config deleted",
	})
}
