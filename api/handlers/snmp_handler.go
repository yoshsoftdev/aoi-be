package handlers

import (
	"net/http"
	"strconv"

	"aoi-backend/internal/core/logger"
	"aoi-backend/internal/domain"
	"aoi-backend/internal/repository"
	"aoi-backend/internal/scheduler"

	"github.com/gin-gonic/gin"
)

type SNMPHandler struct {
	repo             repository.SNMPRepository
	schedulerManager *scheduler.SchedulerManager
}

func NewSNMPHandler(repo repository.SNMPRepository, schedulerManager *scheduler.SchedulerManager) *SNMPHandler {
	return &SNMPHandler{
		repo:             repo,
		schedulerManager: schedulerManager,
	}
}

func (h *SNMPHandler) GetAll(c *gin.Context) {
	configs, err := h.repo.GetAll()
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

func (h *SNMPHandler) GetByID(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "Invalid ID",
		})
		return
	}

	config, err := h.repo.GetByID(uint(id))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{
			"success": false,
			"error":   "SNMP config not found",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data":    config,
	})
}

func (h *SNMPHandler) Create(c *gin.Context) {
	var config domain.SNMPConfig
	if err := c.ShouldBindJSON(&config); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	if err := h.repo.Create(&config); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	// Reload scheduler to pick up new SNMP config
	if h.schedulerManager != nil {
		logger.GetLogger().Info("Reloading scheduler after SNMP config creation")
		h.schedulerManager.Reload()
	}

	c.JSON(http.StatusCreated, gin.H{
		"success": true,
		"data":    config,
	})
}

func (h *SNMPHandler) Update(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "Invalid ID",
		})
		return
	}

	var config domain.SNMPConfig
	if err := c.ShouldBindJSON(&config); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	config.ID = uint(id)
	if err := h.repo.Update(&config); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	// Reload scheduler to pick up updated SNMP config
	if h.schedulerManager != nil {
		logger.GetLogger().Info("Reloading scheduler after SNMP config update")
		h.schedulerManager.Reload()
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data":    config,
	})
}

func (h *SNMPHandler) Delete(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "Invalid ID",
		})
		return
	}

	if err := h.repo.Delete(uint(id)); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	// Reload scheduler to remove deleted SNMP config
	if h.schedulerManager != nil {
		logger.GetLogger().Info("Reloading scheduler after SNMP config deletion")
		h.schedulerManager.Reload()
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"message": "SNMP config deleted successfully",
	})
}
