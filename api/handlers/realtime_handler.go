package handlers

import (
	"fmt"
	"net/http"
	"strconv"

	"aoi-backend/internal/core/cache"
	"aoi-backend/internal/domain"
	"github.com/gin-gonic/gin"
)

type RealtimeHandler struct {
	cache cache.Cache
}

func NewRealtimeHandler(cache cache.Cache) *RealtimeHandler {
	return &RealtimeHandler{
		cache: cache,
	}
}

func (h *RealtimeHandler) GetAll(c *gin.Context) {
	// Get all parameters from cache
	// This is a simplified version - in production, you'd want to get parameter list from DB
	// and then fetch each from cache
	
	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data":    []interface{}{},
		"message": "Use WebSocket for real-time data",
	})
}

func (h *RealtimeHandler) GetByParameterID(c *gin.Context) {
	paramID, err := strconv.ParseUint(c.Param("parameter_id"), 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "Invalid parameter ID",
		})
		return
	}

	cacheKey := fmt.Sprintf("parameter:%d", paramID)
	var data domain.RealtimeData
	
	if err := h.cache.Get(cacheKey, &data); err != nil {
		c.JSON(http.StatusNotFound, gin.H{
			"success": false,
			"error":   "Parameter data not found in cache",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data":    data,
	})
}

