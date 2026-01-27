package handlers

import (
	"net/http"
	"strconv"
	"time"

	"aoi-backend/internal/core/logger"
	"aoi-backend/internal/repository"

	"github.com/gin-gonic/gin"
)

type WriteLogHandler struct {
	writeLogRepo repository.WriteLogRepository
}

func NewWriteLogHandler(writeLogRepo repository.WriteLogRepository) *WriteLogHandler {
	return &WriteLogHandler{
		writeLogRepo: writeLogRepo,
	}
}

// GetAll returns all write logs with optional limit
func (h *WriteLogHandler) GetAll(c *gin.Context) {
	limitStr := c.DefaultQuery("limit", "100")
	limit, err := strconv.Atoi(limitStr)
	if err != nil || limit < 1 {
		limit = 100
	}

	logs, err := h.writeLogRepo.GetRecent(limit)
	if err != nil {
		logger.GetLogger().Errorf("Failed to get write logs: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   "Failed to get write logs",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data":    logs,
		"meta": gin.H{
			"total": len(logs),
		},
	})
}

// GetByID returns a write log by ID
func (h *WriteLogHandler) GetByID(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "Invalid write log ID",
		})
		return
	}

	log, err := h.writeLogRepo.GetByID(uint(id))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{
			"success": false,
			"error":   "Write log not found",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data":    log,
	})
}

// GetByParameterID returns write logs for a specific parameter
func (h *WriteLogHandler) GetByParameterID(c *gin.Context) {
	paramID, err := strconv.ParseUint(c.Param("parameter_id"), 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "Invalid parameter ID",
		})
		return
	}

	limitStr := c.DefaultQuery("limit", "50")
	limit, err := strconv.Atoi(limitStr)
	if err != nil || limit < 1 {
		limit = 50
	}

	logs, err := h.writeLogRepo.GetByParameterID(uint(paramID), limit)
	if err != nil {
		logger.GetLogger().Errorf("Failed to get write logs for parameter %d: %v", paramID, err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   "Failed to get write logs",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data":    logs,
		"meta": gin.H{
			"total": len(logs),
		},
	})
}

// GetByWorkflowID returns write logs for a specific workflow
func (h *WriteLogHandler) GetByWorkflowID(c *gin.Context) {
	workflowID, err := strconv.ParseUint(c.Param("workflow_id"), 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "Invalid workflow ID",
		})
		return
	}

	limitStr := c.DefaultQuery("limit", "50")
	limit, err := strconv.Atoi(limitStr)
	if err != nil || limit < 1 {
		limit = 50
	}

	logs, err := h.writeLogRepo.GetByWorkflowID(uint(workflowID), limit)
	if err != nil {
		logger.GetLogger().Errorf("Failed to get write logs for workflow %d: %v", workflowID, err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   "Failed to get write logs",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data":    logs,
		"meta": gin.H{
			"total": len(logs),
		},
	})
}

// GetByDateRange returns write logs within a date range
func (h *WriteLogHandler) GetByDateRange(c *gin.Context) {
	startDateStr := c.Query("start_date")
	endDateStr := c.Query("end_date")

	if startDateStr == "" || endDateStr == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "start_date and end_date are required",
		})
		return
	}

	startDate, err := time.Parse("2006-01-02", startDateStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "Invalid start_date format. Use YYYY-MM-DD",
		})
		return
	}

	endDate, err := time.Parse("2006-01-02", endDateStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "Invalid end_date format. Use YYYY-MM-DD",
		})
		return
	}

	// Set end date to end of day
	endDate = endDate.Add(23*time.Hour + 59*time.Minute + 59*time.Second)

	limitStr := c.DefaultQuery("limit", "100")
	limit, err := strconv.Atoi(limitStr)
	if err != nil || limit < 1 {
		limit = 100
	}

	logs, err := h.writeLogRepo.GetByDateRange(startDate, endDate, limit)
	if err != nil {
		logger.GetLogger().Errorf("Failed to get write logs by date range: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   "Failed to get write logs",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data":    logs,
		"meta": gin.H{
			"total": len(logs),
		},
	})
}

