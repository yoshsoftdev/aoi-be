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
		// Format: register_values is array of uint16 values, e.g., [1, 1, 1, 1, 1, 1, 1, 1]
		// This matches modpoll format: modpoll ... 1 1 1 1 1 1 1 1
		if len(req.RegisterValues) > 0 {
			// Use raw register values directly (same format as modpoll)
			registerValues = req.RegisterValues
			log.Infof("Write MANUAL mode for parameter %d: register_values=%v (count=%d)",
				parameterID, registerValues, len(registerValues))
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

	// 9. Write based on function code
	address := uint16(getRegisterAddress())
	fc := getFunctionCode()

	switch fc {
	case 5: // Write Single Coil
		if len(registerValues) != 1 {
			return &WriteResponse{
				Success: false,
				Error:   fmt.Sprintf("Write Single Coil requires exactly 1 value, got %d", len(registerValues)),
			}, nil
		}
		err = client.WriteSingleCoil(ctx, address, registerValues[0])
		if err != nil {
			return &WriteResponse{
				Success: false,
				Error:   fmt.Sprintf("Failed to write single coil: %v", err),
			}, nil
		}

	case 15: // Write Multiple Coils
		err = client.WriteMultipleCoils(ctx, address, registerValues)
		if err != nil {
			return &WriteResponse{
				Success: false,
				Error:   fmt.Sprintf("Failed to write multiple coils: %v", err),
			}, nil
		}

	case 6: // Write Single Register
		if len(registerValues) != 1 {
			return &WriteResponse{
				Success: false,
				Error:   fmt.Sprintf("Write Single Register requires exactly 1 value, got %d", len(registerValues)),
			}, nil
		}
		// Use WriteMultipleRegisters with quantity=1 for single register
		err = client.WriteMultipleRegisters(ctx, address, registerValues)
		if err != nil {
			return &WriteResponse{
				Success: false,
				Error:   fmt.Sprintf("Failed to write single register: %v", err),
			}, nil
		}

	case 16: // Write Multiple Registers
		err = client.WriteMultipleRegisters(ctx, address, registerValues)
		if err != nil {
			return &WriteResponse{
				Success: false,
				Error:   fmt.Sprintf("Failed to write multiple registers: %v", err),
			}, nil
		}

	default:
		return &WriteResponse{
			Success: false,
			Error:   fmt.Sprintf("Unsupported function code for write: %d", fc),
		}, nil
	}

	// Log success message based on function code
	switch fc {
	case 5:
		log.Infof("Successfully wrote single coil to parameter %d", parameterID)
	case 15:
		log.Infof("Successfully wrote %d coils to parameter %d", len(registerValues), parameterID)
	case 6:
		log.Infof("Successfully wrote single register to parameter %d", parameterID)
	case 16:
		log.Infof("Successfully wrote %d registers to parameter %d", len(registerValues), parameterID)
	}

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

// TestWriteConfig represents configuration for test write without parameter_id
type TestWriteConfig struct {
	DeviceID        uint
	Protocol        string // "modbus_rtu" or "modbus_tcp"
	SlaveID         int
	FunctionCode    int
	RegisterAddress int
	Quantity        int
	DataType        string
	ByteOrder       string
	RegisterValues  []uint16
}

// TestWrite executes a test write operation without creating a parameter in database
// This is used for testing write capability before saving parameter configuration
func (s *WriteService) TestWrite(ctx context.Context, config TestWriteConfig) (*WriteResponse, error) {
	log := logger.GetLogger()

	// 1. Get device
	device, err := s.deviceRepo.GetByID(config.DeviceID)
	if err != nil {
		return &WriteResponse{
			Success: false,
			Error:   fmt.Sprintf("Device not found: %v", err),
		}, nil
	}

	// 2. Validate register values length matches quantity
	if len(config.RegisterValues) != config.Quantity {
		return &WriteResponse{
			Success: false,
			Error:   fmt.Sprintf("Register values length (%d) must match quantity (%d)", len(config.RegisterValues), config.Quantity),
		}, nil
	}

	// 3. Create Modbus client
	var client Client
	switch config.Protocol {
	case "modbus_rtu":
		rtuClient, err := modbus.NewRTUClient(device, &config.SlaveID)
		if err != nil {
			return &WriteResponse{
				Success: false,
				Error:   fmt.Sprintf("Failed to create Modbus RTU client: %v", err),
			}, nil
		}
		defer rtuClient.Close()
		client = rtuClient

	case "modbus_tcp":
		tcpClient, err := modbus.NewTCPClient(device, &config.SlaveID)
		if err != nil {
			return &WriteResponse{
				Success: false,
				Error:   fmt.Sprintf("Failed to create Modbus TCP client: %v", err),
			}, nil
		}
		defer tcpClient.Close()
		client = tcpClient

	default:
		return &WriteResponse{
			Success: false,
			Error:   fmt.Sprintf("Unsupported protocol: %s", config.Protocol),
		}, nil
	}

	// 4. Write based on function code
	address := uint16(config.RegisterAddress)
	fc := config.FunctionCode

	switch fc {
	case 5: // Write Single Coil
		if len(config.RegisterValues) != 1 {
			return &WriteResponse{
				Success: false,
				Error:   fmt.Sprintf("Write Single Coil requires exactly 1 value, got %d", len(config.RegisterValues)),
			}, nil
		}
		err = client.WriteSingleCoil(ctx, address, config.RegisterValues[0])
		if err != nil {
			return &WriteResponse{
				Success: false,
				Error:   fmt.Sprintf("Failed to write single coil: %v", err),
			}, nil
		}

	case 15: // Write Multiple Coils
		err = client.WriteMultipleCoils(ctx, address, config.RegisterValues)
		if err != nil {
			return &WriteResponse{
				Success: false,
				Error:   fmt.Sprintf("Failed to write multiple coils: %v", err),
			}, nil
		}

	case 6: // Write Single Register
		if len(config.RegisterValues) != 1 {
			return &WriteResponse{
				Success: false,
				Error:   fmt.Sprintf("Write Single Register requires exactly 1 value, got %d", len(config.RegisterValues)),
			}, nil
		}
		// Use WriteMultipleRegisters with quantity=1 for single register
		err = client.WriteMultipleRegisters(ctx, address, config.RegisterValues)
		if err != nil {
			return &WriteResponse{
				Success: false,
				Error:   fmt.Sprintf("Failed to write single register: %v", err),
			}, nil
		}

	case 16: // Write Multiple Registers
		err = client.WriteMultipleRegisters(ctx, address, config.RegisterValues)
		if err != nil {
			return &WriteResponse{
				Success: false,
				Error:   fmt.Sprintf("Failed to write multiple registers: %v", err),
			}, nil
		}

	default:
		return &WriteResponse{
			Success: false,
			Error:   fmt.Sprintf("Unsupported function code for write: %d", fc),
		}, nil
	}

	// Log success message based on function code
	switch fc {
	case 5:
		log.Infof("Test write successful (single coil): device=%d, slave=%d, address=%d",
			config.DeviceID, config.SlaveID, config.RegisterAddress)
	case 15:
		log.Infof("Test write successful (multiple coils): device=%d, slave=%d, address=%d, quantity=%d",
			config.DeviceID, config.SlaveID, config.RegisterAddress, config.Quantity)
	case 6:
		log.Infof("Test write successful (single register): device=%d, slave=%d, address=%d",
			config.DeviceID, config.SlaveID, config.RegisterAddress)
	case 16:
		log.Infof("Test write successful (multiple registers): device=%d, slave=%d, address=%d, quantity=%d",
			config.DeviceID, config.SlaveID, config.RegisterAddress, config.Quantity)
	}

	return &WriteResponse{
		Success: true,
		Message: "Test write successful (no parameter saved)",
	}, nil
}

// Client interface for Modbus clients
type Client interface {
	Read(ctx context.Context, functionCode int, address uint16, quantity uint16) ([]byte, error)
	WriteMultipleRegisters(ctx context.Context, address uint16, values []uint16) error
	WriteMultipleCoils(ctx context.Context, address uint16, values []uint16) error
	WriteSingleCoil(ctx context.Context, address uint16, value uint16) error
	Close() error
}
