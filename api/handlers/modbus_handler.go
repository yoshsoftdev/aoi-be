package handlers

import (
	"net/http"
	"strconv"

	"aoi-backend/internal/domain"
	"aoi-backend/internal/repository"
	"github.com/gin-gonic/gin"
)

type ModbusHandler struct {
	rtuRepo repository.ModbusRTURepository
	tcpRepo repository.ModbusTCPRepository
}

func NewModbusHandler(
	rtuRepo repository.ModbusRTURepository,
	tcpRepo repository.ModbusTCPRepository,
) *ModbusHandler {
	return &ModbusHandler{
		rtuRepo: rtuRepo,
		tcpRepo: tcpRepo,
	}
}

// Modbus RTU Handlers
func (h *ModbusHandler) GetAllRTU(c *gin.Context) {
	configs, err := h.rtuRepo.GetAll()
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

func (h *ModbusHandler) GetRTUByID(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "Invalid ID",
		})
		return
	}

	config, err := h.rtuRepo.GetByID(uint(id))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{
			"success": false,
			"error":   "Modbus RTU config not found",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data":    config,
	})
}

func (h *ModbusHandler) CreateRTU(c *gin.Context) {
	var config domain.ModbusRTU
	if err := c.ShouldBindJSON(&config); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	// Validate required fields
	if config.FunctionCode == 0 {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "function_code is required and cannot be 0",
		})
		return
	}

	if err := h.rtuRepo.Create(&config); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"success": true,
		"data":    config,
	})
}

func (h *ModbusHandler) UpdateRTU(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "Invalid ID",
		})
		return
	}

	var config domain.ModbusRTU
	if err := c.ShouldBindJSON(&config); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	config.ID = uint(id)
	if err := h.rtuRepo.Update(&config); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data":    config,
	})
}

func (h *ModbusHandler) DeleteRTU(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "Invalid ID",
		})
		return
	}

	if err := h.rtuRepo.Delete(uint(id)); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"message": "Modbus RTU config deleted successfully",
	})
}

// Modbus TCP Handlers
func (h *ModbusHandler) GetAllTCP(c *gin.Context) {
	configs, err := h.tcpRepo.GetAll()
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

func (h *ModbusHandler) GetTCPByID(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "Invalid ID",
		})
		return
	}

	config, err := h.tcpRepo.GetByID(uint(id))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{
			"success": false,
			"error":   "Modbus TCP config not found",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data":    config,
	})
}

func (h *ModbusHandler) CreateTCP(c *gin.Context) {
	var config domain.ModbusTCP
	if err := c.ShouldBindJSON(&config); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	// Validate required fields
	if config.FunctionCode == 0 {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "function_code is required and cannot be 0",
		})
		return
	}

	// Set default data_type if not provided
	if config.DataType == "" {
		config.DataType = "uint16"
	}

	if err := h.tcpRepo.Create(&config); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"success": true,
		"data":    config,
	})
}

func (h *ModbusHandler) UpdateTCP(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "Invalid ID",
		})
		return
	}

	var config domain.ModbusTCP
	if err := c.ShouldBindJSON(&config); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	config.ID = uint(id)
	if err := h.tcpRepo.Update(&config); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data":    config,
	})
}

func (h *ModbusHandler) DeleteTCP(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "Invalid ID",
		})
		return
	}

	if err := h.tcpRepo.Delete(uint(id)); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"message": "Modbus TCP config deleted successfully",
	})
}

