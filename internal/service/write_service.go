package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"aoi-backend/internal/core/cache"
	"aoi-backend/internal/core/logger"
	"aoi-backend/internal/domain"
	"aoi-backend/internal/modbus"
	"aoi-backend/internal/repository"
)

// WriteService handles write operations for parameters
type WriteService struct {
	paramRepo     repository.ParameterRepository
	presetRepo    repository.PresetRepository
	modbusRTURepo repository.ModbusRTURepository
	modbusTCPRepo repository.ModbusTCPRepository
	deviceRepo    repository.DeviceRepository
	cache         cache.Cache
	formulaEngine *FormulaEngine
}

// NewWriteService creates a new WriteService instance
func NewWriteService(
	paramRepo repository.ParameterRepository,
	presetRepo repository.PresetRepository,
	modbusRTURepo repository.ModbusRTURepository,
	modbusTCPRepo repository.ModbusTCPRepository,
	deviceRepo repository.DeviceRepository,
	cache cache.Cache,
	formulaEngine *FormulaEngine,
) *WriteService {
	return &WriteService{
		paramRepo:     paramRepo,
		presetRepo:    presetRepo,
		modbusRTURepo: modbusRTURepo,
		modbusTCPRepo: modbusTCPRepo,
		deviceRepo:    deviceRepo,
		cache:         cache,
		formulaEngine: formulaEngine,
	}
}

// WriteRequest represents a write request
type WriteRequest struct {
	Mode           string   `json:"mode"`                      // TEST, MANUAL, PRESET
	Value          *float64 `json:"value,omitempty"`           // For MANUAL mode (scalar value)
	RegisterValues []uint16 `json:"register_values,omitempty"` // For MANUAL mode (raw register values)
	PresetID       *uint    `json:"preset_id,omitempty"`       // For PRESET mode
}

// WriteResponse represents the result of a write operation
type WriteResponse struct {
	Success       bool        `json:"success"`
	Message       string      `json:"message"`
	ReadBackValue interface{} `json:"read_back_value,omitempty"`
	Error         string      `json:"error,omitempty"`
}

// Write executes a write operation for a parameter
func (s *WriteService) Write(ctx context.Context, parameterID uint, req WriteRequest) (*WriteResponse, error) {
	log := logger.GetLogger()

	// 1. Get parameter
	param, err := s.paramRepo.GetByID(parameterID)
	if err != nil {
		return &WriteResponse{
			Success: false,
			Error:   fmt.Sprintf("Parameter not found: %v", err),
		}, nil
	}

	// 2. GUARD: Reject formula parameters
	if param.IsFormula {
		return &WriteResponse{
			Success: false,
			Error:   "Cannot write to formula parameter",
		}, nil
	}

	// 3. GUARD: Only WRITE parameters can be written to
	// Direction must be 'WRITE' (new field) or read_write must be true (legacy)
	if param.Direction != "WRITE" && !param.ReadWrite {
		return &WriteResponse{
			Success: false,
			Error:   "Parameter is not writable. Only parameters with direction=WRITE can be written to",
		}, nil
	}

	// 4. Get device
	device, err := s.deviceRepo.GetByID(param.DeviceID)
	if err != nil {
		return &WriteResponse{
			Success: false,
			Error:   fmt.Sprintf("Device not found: %v", err),
		}, nil
	}

	// 5. Get Modbus config based on parameter type
	var modbusConfig *domain.ModbusRTU
	var modbusTCPConfig *domain.ModbusTCP

	switch param.Type {
	case "modbus_rtu":
		rtuConfig, err := s.modbusRTURepo.GetByParameterID(parameterID)
		if err != nil {
			return &WriteResponse{
				Success: false,
				Error:   "Modbus RTU config not found",
			}, nil
		}
		modbusConfig = rtuConfig

	case "modbus_tcp":
		tcpConfig, err := s.modbusTCPRepo.GetByParameterID(parameterID)
		if err != nil {
			return &WriteResponse{
				Success: false,
				Error:   "Modbus TCP config not found",
			}, nil
		}
		modbusTCPConfig = tcpConfig

	default:
		return &WriteResponse{
			Success: false,
			Error:   fmt.Sprintf("Write not supported for parameter type: %s", param.Type),
		}, nil
	}

	// Helper to get config values
	getSlaveID := func() int {
		if modbusConfig != nil {
			return modbusConfig.SlaveID
		}
		return modbusTCPConfig.SlaveID
	}
	getFunctionCode := func() int {
		if modbusConfig != nil {
			return modbusConfig.FunctionCode
		}
		return modbusTCPConfig.FunctionCode
	}
	getRegisterAddress := func() int {
		if modbusConfig != nil {
			return modbusConfig.RegisterAddress
		}
		return modbusTCPConfig.RegisterAddress
	}
	getQuantity := func() int {
		if modbusConfig != nil {
			return modbusConfig.Quantity
		}
		return modbusTCPConfig.Quantity
	}
	getByteOrder := func() string {
		if modbusConfig != nil {
			return modbusConfig.ByteOrder
		}
		return modbusTCPConfig.ByteOrder
	}
	getDataType := func() string {
		if modbusConfig != nil {
			return modbusConfig.DataType
		}
		return modbusTCPConfig.DataType
	}
	getScaling := func() float64 {
		if modbusConfig != nil {
			return modbusConfig.Scaling
		}
		return modbusTCPConfig.Scaling
	}

	// 6. Resolve write value based on mode
	var writeValue float64
	var registerValues []uint16

	switch req.Mode {
	case "TEST":
		// Test mode: use a test value (0 or 1)
		writeValue = 0
		log.Infof("Write TEST mode for parameter %d", parameterID)

	case "MANUAL":
		// MANUAL mode can use either Value (scalar) or RegisterValues (raw registers)
		if len(req.RegisterValues) > 0 {
			// Use raw register values directly
			registerValues = req.RegisterValues
			log.Infof("Write MANUAL mode for parameter %d: register_values=%v", parameterID, registerValues)
		} else if req.Value != nil {
			// Use scalar value (will be encoded to registers)
			writeValue = *req.Value
			log.Infof("Write MANUAL mode for parameter %d: value=%f", parameterID, writeValue)
		} else {
			return &WriteResponse{
				Success: false,
				Error:   "Either value or register_values is required for MANUAL mode",
			}, nil
		}

	case "PRESET":
		if req.PresetID == nil {
			return &WriteResponse{
				Success: false,
				Error:   "preset_id is required for PRESET mode",
			}, nil
		}

		preset, err := s.presetRepo.GetByID(*req.PresetID)
		if err != nil {
			return &WriteResponse{
				Success: false,
				Error:   fmt.Sprintf("Preset not found: %v", err),
			}, nil
		}

		// Validate preset belongs to this parameter
		if preset.ParameterID != parameterID {
			return &WriteResponse{
				Success: false,
				Error:   "Preset does not belong to this parameter",
			}, nil
		}

		// Parse preset value based on value_type
		switch preset.ValueType {
		case "scalar":
			var scalarValue domain.PresetValueScalar
			if err := json.Unmarshal([]byte(preset.Value), &scalarValue); err != nil {
				return &WriteResponse{
					Success: false,
					Error:   fmt.Sprintf("Invalid scalar preset value: %v", err),
				}, nil
			}
			writeValue = scalarValue.Value

		case "bitmask", "raw_register":
			var rawValue domain.PresetValueRawRegister
			if err := json.Unmarshal([]byte(preset.Value), &rawValue); err != nil {
				return &WriteResponse{
					Success: false,
					Error:   fmt.Sprintf("Invalid raw register preset value: %v", err),
				}, nil
			}

			// Validate length matches quantity
			if len(rawValue.RegisterValues) != getQuantity() {
				return &WriteResponse{
					Success: false,
					Error:   fmt.Sprintf("Preset register_values length (%d) must match quantity (%d)", len(rawValue.RegisterValues), getQuantity()),
				}, nil
			}

			// Encode raw registers
			registerValues, err = modbus.EncodeRawRegisters(rawValue.RegisterValues, getQuantity())
			if err != nil {
				return &WriteResponse{
					Success: false,
					Error:   fmt.Sprintf("Failed to encode raw registers: %v", err),
				}, nil
			}

			log.Infof("Write PRESET mode for parameter %d: preset=%s, registers=%v", parameterID, preset.Name, registerValues)
		}

	default:
		return &WriteResponse{
			Success: false,
			Error:   fmt.Sprintf("Invalid write mode: %s", req.Mode),
		}, nil
	}

	// 7. Encode value to registers (if not already encoded from raw_register preset or manual register_values)
	if len(registerValues) == 0 {
		dataType := getDataType()
		if dataType == "" {
			dataType = "uint16" // Default
		}

		var err error
		registerValues, err = modbus.EncodeValueToRegisters(
			writeValue,
			dataType,
			getByteOrder(),
			getQuantity(),
		)
		if err != nil {
			return &WriteResponse{
				Success: false,
				Error:   fmt.Sprintf("Failed to encode value to registers: %v", err),
			}, nil
		}
	}

	// Validate register values length matches quantity
	if len(registerValues) != getQuantity() {
		return &WriteResponse{
			Success: false,
			Error:   fmt.Sprintf("Register values length (%d) must match quantity (%d)", len(registerValues), getQuantity()),
		}, nil
	}

	// 8. Create Modbus client and write
	var client Client
	switch param.Type {
	case "modbus_rtu":
		slaveID := getSlaveID()
		rtuClient, err := modbus.NewRTUClient(device, &slaveID)
		if err != nil {
			return &WriteResponse{
				Success: false,
				Error:   fmt.Sprintf("Failed to create Modbus RTU client: %v", err),
			}, nil
		}
		defer rtuClient.Close()
		client = rtuClient

	case "modbus_tcp":
		slaveID := getSlaveID()
		tcpClient, err := modbus.NewTCPClient(device, &slaveID)
		if err != nil {
			return &WriteResponse{
				Success: false,
				Error:   fmt.Sprintf("Failed to create Modbus TCP client: %v", err),
			}, nil
		}
		defer tcpClient.Close()
		client = tcpClient
	}

	// 9. Write multiple registers (atomic write)
	address := uint16(getRegisterAddress())
	err = client.WriteMultipleRegisters(ctx, address, registerValues)
	if err != nil {
		return &WriteResponse{
			Success: false,
			Error:   fmt.Sprintf("Failed to write registers: %v", err),
		}, nil
	}

	log.Infof("Successfully wrote %d registers to parameter %d", len(registerValues), parameterID)

	// 10. TEST mode: Don't update cache or trigger formula
	if req.Mode == "TEST" {
		return &WriteResponse{
			Success: true,
			Message: "Test write successful (no cache update)",
		}, nil
	}

	// 11. Read-back (optional) - read the value we just wrote
	var readBackValue interface{}
	readData, err := client.Read(ctx, getFunctionCode(), address, uint16(getQuantity()))
	if err == nil {
		// Decode read-back value (similar to worker logic)
		dataType := getDataType()
		if dataType == "" {
			dataType = "uint16"
		}

		var rawValueFloat float64
		switch dataType {
		case "float32", "float":
			rawFloat32, err := modbus.ConvertBytesToFloat32(readData, getByteOrder(), getQuantity())
			if err == nil {
				rawValueFloat = float64(rawFloat32)
			}
		case "float64":
			rawFloat64, err := modbus.ConvertBytesToFloat64(readData, getByteOrder(), getQuantity())
			if err == nil {
				rawValueFloat = rawFloat64
			}
		case "int32":
			rawInt32, err := modbus.ConvertBytesToSignedInt32(readData, getByteOrder(), getQuantity())
			if err == nil {
				rawValueFloat = float64(rawInt32)
			}
		default:
			rawValue, err := modbus.ConvertBytesToValue(readData, getByteOrder(), getQuantity())
			if err == nil {
				rawValueFloat = float64(rawValue)
			}
		}

		// Apply scaling
		readBackValue = rawValueFloat * getScaling()
	}

	// 12. Update cache RAW (for MANUAL and PRESET modes)
	realtimeData := domain.RealtimeData{
		ParameterID:    parameterID,
		ParameterNama:  param.Nama,
		ParameterType:  param.Type,
		DeviceNama:     device.Nama,
		RawValue:       readBackValue,
		ProcessedValue: readBackValue,
		CustomRumus:    param.CustomRumus,
		Timestamp:      time.Now(),
		Status:         "online",
		Error:          nil,
	}

	cacheKey := fmt.Sprintf("parameter:raw:%d", parameterID)
	if err := s.cache.Set(cacheKey, realtimeData, 24*time.Hour); err != nil {
		log.Warnf("Failed to update cache after write: %v", err)
	}

	// 13. Trigger formula engine (reactive evaluation)
	if s.formulaEngine != nil {
		s.formulaEngine.OnRawParameterUpdated(parameterID, &realtimeData)
	}

	return &WriteResponse{
		Success:       true,
		Message:       "Write successful",
		ReadBackValue: readBackValue,
	}, nil
}

// Client interface for Modbus clients
type Client interface {
	Read(ctx context.Context, functionCode int, address uint16, quantity uint16) ([]byte, error)
	WriteMultipleRegisters(ctx context.Context, address uint16, values []uint16) error
	Close() error
}
