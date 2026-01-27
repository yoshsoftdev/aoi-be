package handlers

import (
	"fmt"
	"net/http"

	"aoi-backend/internal/core/logger"
	"aoi-backend/internal/repository"
	"aoi-backend/internal/scheduler"
	"aoi-backend/internal/service"

	"github.com/gin-gonic/gin"
)

type WriteHandler struct {
	writeService     *service.WriteService
	schedulerManager *scheduler.SchedulerManager
	paramRepo        repository.ParameterRepository
	deviceRepo       repository.DeviceRepository
}

func NewWriteHandler(writeService *service.WriteService, schedulerManager *scheduler.SchedulerManager) *WriteHandler {
	return &WriteHandler{
		writeService:     writeService,
		schedulerManager: schedulerManager,
		paramRepo:        repository.NewParameterRepository(),
		deviceRepo:       repository.NewDeviceRepository(),
	}
}

type ExecuteWriteRequest struct {
	ParameterID    uint     `json:"parameter_id" binding:"required"`
	Mode           string   `json:"mode" binding:"required,oneof=preset manual test"`
	PresetID       *uint    `json:"preset_id,omitempty"`
	RegisterValues []uint16 `json:"register_values,omitempty"`
}

type ExecuteWriteResponse struct {
	Success       bool     `json:"success"`
	Message       string   `json:"message"`
	ReadBackValue *float64 `json:"read_back_value,omitempty"`
	Error         string   `json:"error,omitempty"`
}

// Execute handles POST /api/write/execute
func (h *WriteHandler) Execute(c *gin.Context) {
	var req ExecuteWriteRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, ExecuteWriteResponse{
			Success: false,
			Message: "Invalid request",
			Error:   err.Error(),
		})
		return
	}

	// Convert mode to uppercase for consistency
	mode := "MANUAL"
	if req.Mode == "preset" {
		mode = "PRESET"
	} else if req.Mode == "test" {
		mode = "TEST"
	}

	// Build write request
	var writeReq service.WriteRequest
	if mode == "PRESET" {
		if req.PresetID == nil {
			c.JSON(http.StatusBadRequest, ExecuteWriteResponse{
				Success: false,
				Message: "preset_id is required for preset mode",
				Error:   "Missing preset_id",
			})
			return
		}
		writeReq = service.WriteRequest{
			Mode:     mode,
			PresetID: req.PresetID,
		}
	} else if mode == "TEST" {
		// TEST mode: use register_values if provided, otherwise use default test value
		if len(req.RegisterValues) > 0 {
			writeReq = service.WriteRequest{
				Mode:           mode,
				RegisterValues: req.RegisterValues,
			}
		} else {
			// Default test value: single register with value 0
			writeReq = service.WriteRequest{
				Mode:           mode,
				RegisterValues: []uint16{0},
			}
		}
	} else {
		// MANUAL mode: can use either register_values (raw) or value (scalar)
		if len(req.RegisterValues) > 0 {
			// Use raw register values directly
			writeReq = service.WriteRequest{
				Mode:           mode,
				RegisterValues: req.RegisterValues,
			}
		} else {
			c.JSON(http.StatusBadRequest, ExecuteWriteResponse{
				Success: false,
				Message: "Either register_values or value is required for manual mode",
				Error:   "Missing register_values or value",
			})
			return
		}
	}

	// Get parameter to get device_id for locking
	param, err := h.paramRepo.GetByID(req.ParameterID)
	if err != nil {
		c.JSON(http.StatusBadRequest, ExecuteWriteResponse{
			Success: false,
			Message: "Parameter not found",
			Error:   err.Error(),
		})
		return
	}

	// Get device for connection name
	device, err := h.deviceRepo.GetByID(param.DeviceID)
	connectionName := "Unknown"
	if err == nil {
		connectionName = device.Nama
	}

	// Lock connection during write
	if h.schedulerManager != nil {
		h.schedulerManager.LockConnection(param.DeviceID, fmt.Sprintf("write_operation: parameter_id=%d", req.ParameterID))
		defer h.schedulerManager.UnlockConnection(param.DeviceID, fmt.Sprintf("write_operation: parameter_id=%d", req.ParameterID))
		logger.GetLogger().Infof("[WRITE] Connection locked: connection_id=%d, connection_name=%s, parameter_id=%d",
			param.DeviceID, connectionName, req.ParameterID)
	}

	// Execute write
	result, err := h.writeService.Write(c.Request.Context(), req.ParameterID, writeReq)
	if err != nil {
		c.JSON(http.StatusInternalServerError, ExecuteWriteResponse{
			Success: false,
			Message: "Write operation failed",
			Error:   err.Error(),
		})
		return
	}

	// Build response
	response := ExecuteWriteResponse{
		Success: result.Success,
		Message: result.Message,
	}
	if result.ReadBackValue != nil {
		// Type assertion for ReadBackValue
		if val, ok := result.ReadBackValue.(float64); ok {
			response.ReadBackValue = &val
		}
	}
	if result.Error != "" {
		response.Error = result.Error
	}

	c.JSON(http.StatusOK, response)
}

// TestWriteRequest represents a test write request without parameter_id
type TestWriteRequest struct {
	ParameterID     *uint    `json:"parameter_id,omitempty"` // Optional: if provided, log will be saved to DB
	DeviceID        uint     `json:"device_id" binding:"required"`
	Protocol        string   `json:"protocol" binding:"required,oneof=modbus_rtu modbus_tcp"`
	SlaveID         int      `json:"slave_id" binding:"required,min=1,max=247"`
	FunctionCode    int      `json:"function_code" binding:"required,oneof=5 6 15 16"`
	RegisterAddress *int     `json:"register_address" binding:"required"` // Use pointer to distinguish between 0 and not provided
	Quantity        int      `json:"quantity" binding:"required,min=1,max=123"`
	DataType        string   `json:"data_type,omitempty"`
	ByteOrder       string   `json:"byte_order,omitempty"`
	RegisterValues  []uint16 `json:"register_values" binding:"required"`
}

// TestWrite handles POST /api/write/test
// This endpoint allows testing write operations without creating a parameter in database
func (h *WriteHandler) TestWrite(c *gin.Context) {
	var req TestWriteRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, ExecuteWriteResponse{
			Success: false,
			Message: "Invalid request",
			Error:   err.Error(),
		})
		return
	}

	// Validate RegisterAddress (using pointer to allow 0 as valid value)
	if req.RegisterAddress == nil {
		c.JSON(http.StatusBadRequest, ExecuteWriteResponse{
			Success: false,
			Message: "Invalid request",
			Error:   "register_address is required",
		})
		return
	}

	if *req.RegisterAddress < 0 || *req.RegisterAddress > 65535 {
		c.JSON(http.StatusBadRequest, ExecuteWriteResponse{
			Success: false,
			Message: "Invalid request",
			Error:   fmt.Sprintf("register_address must be between 0 and 65535, got %d", *req.RegisterAddress),
		})
		return
	}

	// Validate register values length matches quantity
	if len(req.RegisterValues) != req.Quantity {
		c.JSON(http.StatusBadRequest, ExecuteWriteResponse{
			Success: false,
			Message: "Invalid request",
			Error:   fmt.Sprintf("Register values length (%d) must match quantity (%d)", len(req.RegisterValues), req.Quantity),
		})
		return
	}

	// Set defaults
	if req.DataType == "" {
		req.DataType = "uint16"
	}
	if req.ByteOrder == "" {
		req.ByteOrder = "ABCD"
	}

	// Get device for connection name
	device, err := h.deviceRepo.GetByID(req.DeviceID)
	connectionName := "Unknown"
	if err == nil {
		connectionName = device.Nama
	}

	// Lock connection during test write
	if h.schedulerManager != nil {
		h.schedulerManager.LockConnection(req.DeviceID, fmt.Sprintf("test_write: device_id=%d", req.DeviceID))
		defer h.schedulerManager.UnlockConnection(req.DeviceID, fmt.Sprintf("test_write: device_id=%d", req.DeviceID))
		logger.GetLogger().Infof("[WRITE] Connection locked: connection_id=%d, connection_name=%s, test_write",
			req.DeviceID, connectionName)
	}

	// Build test write config
	config := service.TestWriteConfig{
		DeviceID:        req.DeviceID,
		Protocol:        req.Protocol,
		SlaveID:         req.SlaveID,
		FunctionCode:    req.FunctionCode,
		RegisterAddress: *req.RegisterAddress, // Dereference pointer
		Quantity:        req.Quantity,
		DataType:        req.DataType,
		ByteOrder:       req.ByteOrder,
		RegisterValues:  req.RegisterValues,
	}

	// Execute test write (pass parameterID if provided for logging)
	result, err := h.writeService.TestWrite(c.Request.Context(), config, req.ParameterID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, ExecuteWriteResponse{
			Success: false,
			Message: "Test write operation failed",
			Error:   err.Error(),
		})
		return
	}

	// Build response
	response := ExecuteWriteResponse{
		Success: result.Success,
		Message: result.Message,
	}
	if result.Error != "" {
		response.Error = result.Error
	}

	c.JSON(http.StatusOK, response)
}
