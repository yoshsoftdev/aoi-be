package handlers

import (
	"net/http"

	"aoi-backend/internal/service"

	"github.com/gin-gonic/gin"
)

type WriteHandler struct {
	writeService *service.WriteService
}

func NewWriteHandler(writeService *service.WriteService) *WriteHandler {
	return &WriteHandler{
		writeService: writeService,
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
