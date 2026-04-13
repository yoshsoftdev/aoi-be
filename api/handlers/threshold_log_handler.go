package handlers

import (
	"net/http"
	"strconv"

	"aoi-backend/internal/repository"

	"github.com/gin-gonic/gin"
)

type ThresholdLogHandler struct {
	repo repository.ThresholdLogRepository
}

func NewThresholdLogHandler(repo repository.ThresholdLogRepository) *ThresholdLogHandler {
	return &ThresholdLogHandler{repo: repo}
}

// GetByParameterID handles GET /api/v1/threshold-logs/parameter/:parameter_id?limit=20
func (h *ThresholdLogHandler) GetByParameterID(c *gin.Context) {
	paramIDStr := c.Param("parameter_id")
	paramID, err := strconv.ParseUint(paramIDStr, 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "Invalid parameter ID",
		})
		return
	}

	limit := 20
	if limitStr := c.Query("limit"); limitStr != "" {
		parsedLimit, err := strconv.Atoi(limitStr)
		if err != nil || parsedLimit <= 0 {
			c.JSON(http.StatusBadRequest, gin.H{
				"success": false,
				"error":   "Invalid limit",
			})
			return
		}
		limit = parsedLimit
	}

	logs, err := h.repo.GetByParameterID(uint(paramID), limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   "Failed to get threshold logs",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data":    logs,
	})
}
