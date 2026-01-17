package handlers

import (
	"net/http"
	"strconv"

	"aoi-backend/internal/core/logger"
	"aoi-backend/internal/repository"

	"github.com/gin-gonic/gin"
)

type AlarmHandler struct {
	alarmRepo repository.AlarmRepository
}

func NewAlarmHandler(alarmRepo repository.AlarmRepository) *AlarmHandler {
	return &AlarmHandler{
		alarmRepo: alarmRepo,
	}
}

// GetAll returns all current alarms
func (h *AlarmHandler) GetAll(c *gin.Context) {
	alarms, err := h.alarmRepo.GetAll()
	if err != nil {
		logger.GetLogger().Errorf("Failed to get alarms: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   "Failed to get alarms",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data":    alarms,
	})
}

// DeleteByID deletes an alarm by ID
func (h *AlarmHandler) DeleteByID(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "Invalid alarm ID",
		})
		return
	}

	if err := h.alarmRepo.DeleteByID(uint(id)); err != nil {
		logger.GetLogger().Errorf("Failed to delete alarm %d: %v", id, err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   "Failed to delete alarm",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"message": "Alarm deleted successfully",
	})
}

// DeleteByWorkflowID deletes an alarm by workflow ID
func (h *AlarmHandler) DeleteByWorkflowID(c *gin.Context) {
	workflowID, err := strconv.ParseUint(c.Param("workflow_id"), 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "Invalid workflow ID",
		})
		return
	}

	if err := h.alarmRepo.DeleteByWorkflowID(uint(workflowID)); err != nil {
		logger.GetLogger().Errorf("Failed to delete alarm for workflow %d: %v", workflowID, err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   "Failed to delete alarm",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"message": "Alarm deleted successfully",
	})
}

