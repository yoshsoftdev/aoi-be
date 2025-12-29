package handlers

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"aoi-backend/internal/core/logger"
	"aoi-backend/internal/domain"
	"aoi-backend/internal/modbus"
	"aoi-backend/internal/snmp"

	"github.com/gin-gonic/gin"
)

type TestConnectionRequest struct {
	Type   string      `json:"type" binding:"required"`
	Config interface{} `json:"config" binding:"required"`
}

type TestConnectionResponse struct {
	Connected bool        `json:"connected"`
	RawValue  interface{} `json:"raw_value,omitempty"`
	RawHex    string      `json:"raw_hex,omitempty"`   // Raw bytes in hex format
	RawBytes  []int       `json:"raw_bytes,omitempty"` // Raw bytes as array of integers (0-255)
	Message   string      `json:"message"`
	Details   string      `json:"details,omitempty"`
}

func (h *ModbusHandler) TestConnection(c *gin.Context) {
	log := logger.GetLogger()

	// Add panic recovery
	defer func() {
		if r := recover(); r != nil {
			log.Errorf("Panic in TestConnection: %v", r)
			c.JSON(http.StatusInternalServerError, gin.H{
				"success": false,
				"error":   "Internal server error",
				"details": fmt.Sprintf("%v", r),
			})
		}
	}()

	var req TestConnectionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var response TestConnectionResponse

	switch req.Type {
	case "modbus_rtu":
		log.Debugf("TestConnection: modbus_rtu, quantity=%d", getInt(req.Config.(map[string]interface{}), "quantity"))
		// Parse config to Device (for connection) and ModbusRTU (for register reading)
		configMap := req.Config.(map[string]interface{})
		device := &domain.Device{
			Port:     stringPtr(getString(configMap, "port")),
			Baud:     intPtr(getInt(configMap, "baud")),
			DataBits: intPtr(getInt(configMap, "data_bits")),
			Parity:   stringPtr(getString(configMap, "parity")),
			StopBits: intPtr(getInt(configMap, "stop_bits")),
			SlaveID:  intPtr(getInt(configMap, "slave_id")),
		}
		config := &domain.ModbusRTU{
			SlaveID:         getInt(configMap, "slave_id"), // Parameter-level slave_id
			FunctionCode:    getInt(configMap, "function_code"),
			RegisterAddress: getInt(configMap, "register_address"),
			Quantity:        getInt(configMap, "quantity"),
			ByteOrder:       getString(configMap, "byte_order"),
			DataType:        getString(configMap, "data_type"), // Data type (uint16, int32, float32, float64)
		}

		// Set default data_type if not provided
		if config.DataType == "" {
			config.DataType = "uint16"
		}

		// Use slave_id from parameter config if provided, otherwise from device
		slaveID := device.SlaveID
		if config.SlaveID > 0 {
			slaveID = &config.SlaveID
		}
		client, err := modbus.NewRTUClient(device, slaveID)
		if err != nil {
			c.JSON(http.StatusOK, gin.H{
				"success": true,
				"data": TestConnectionResponse{
					Connected: false,
					Message:   "Connection failed",
					Details:   err.Error(),
				},
			})
			return
		}
		defer client.Close()

		// Coba baca sekali untuk sanity check.
		// Kalau baca gagal, tetap dianggap koneksi berhasil selama client bisa dibuat
		// (mirip modpoll yang fokus cek koneksi fisik).
		log.Debugf("TestConnection: Reading FC=%d, addr=%d, qty=%d", config.FunctionCode, config.RegisterAddress, config.Quantity)
		data, err := client.Read(ctx, config.FunctionCode, uint16(config.RegisterAddress), uint16(config.Quantity))
		if err != nil {
			log.Warnf("TestConnection: Read failed: %v", err)
			response = TestConnectionResponse{
				Connected: true,
				Message:   "Connection OK, read failed",
				Details:   err.Error(),
			}
			break
		}
		log.Debugf("TestConnection: Read succeeded, got %d bytes", len(data))

		// Always return raw bytes in hex format and as array
		rawHex := modbus.FormatHex(data)
		rawBytes := make([]int, len(data))
		for i, b := range data {
			rawBytes[i] = int(b)
		}

		// Function code 1 (Read Coils) and 2 (Read Discrete Inputs) read BITS, not registers
		// They return packed bits, not register values
		if config.FunctionCode == 1 || config.FunctionCode == 2 {
			// Unpack bits from response
			bits := modbus.ConvertBitsToArray(data, config.Quantity)
			// Convert to float64 array for JSON response
			bitsArray := make([]float64, len(bits))
			for i, bit := range bits {
				bitsArray[i] = float64(bit)
			}

			response = TestConnectionResponse{
				Connected: true,
				RawValue:  bitsArray,
				RawHex:    rawHex,
				RawBytes:  rawBytes,
				Message:   "Connection successful",
			}
			break
		}

		// Convert bytes to value berdasarkan data_type.
		// Kalau konversi gagal, tetap Connected=true tapi dengan detail error.
		dataType := config.DataType
		if dataType == "" {
			dataType = "uint16"
		}

		var rawValue interface{}

		// For uint16 with quantity > 1, return array of individual register values
		if dataType == "uint16" && config.Quantity > 1 {
			registerArray, err := modbus.ConvertBytesToRegisterArray(data, config.Quantity)
			if err != nil {
				response = TestConnectionResponse{
					Connected: true,
					RawHex:    rawHex,
					RawBytes:  rawBytes,
					Message:   "Connection OK, conversion failed",
					Details:   err.Error(),
				}
				break
			}
			// Convert []uint16 to []float64 for JSON response
			rawValueArray := make([]float64, len(registerArray))
			for i, v := range registerArray {
				rawValueArray[i] = float64(v)
			}
			rawValue = rawValueArray
		} else {
			// For other data types or single uint16, convert to single value
			var rawValueFloat float64
			var conversionErr error
			switch dataType {
			case "float32", "float": // Support both "float32" and "float" (alias)
				rawFloat32, err := modbus.ConvertBytesToFloat32(data, config.ByteOrder, config.Quantity)
				if err != nil {
					conversionErr = err
				} else {
					rawValueFloat = float64(rawFloat32)
				}
			case "float64":
				rawFloat64, err := modbus.ConvertBytesToFloat64(data, config.ByteOrder, config.Quantity)
				if err != nil {
					conversionErr = err
				} else {
					rawValueFloat = rawFloat64
				}
			case "int32":
				rawInt32, err := modbus.ConvertBytesToSignedInt32(data, config.ByteOrder, config.Quantity)
				if err != nil {
					conversionErr = err
				} else {
					rawValueFloat = float64(rawInt32)
				}
			default: // uint16 (single), uint32, or default
				rawValueUint, err := modbus.ConvertBytesToValue(data, config.ByteOrder, config.Quantity)
				if err != nil {
					conversionErr = err
				} else {
					rawValueFloat = float64(rawValueUint)
				}
			}
			if conversionErr != nil {
				response = TestConnectionResponse{
					Connected: true,
					RawHex:    rawHex,
					RawBytes:  rawBytes,
					Message:   "Connection OK, conversion failed",
					Details:   conversionErr.Error(),
				}
				break
			}
			rawValue = rawValueFloat
		}

		response = TestConnectionResponse{
			Connected: true,
			RawValue:  rawValue,
			Message:   "Connection successful",
		}

	case "modbus_tcp":
		configMap := req.Config.(map[string]interface{})
		device := &domain.Device{
			IPAddress: stringPtr(getString(configMap, "ip_address")),
			TCPPort:   intPtr(getInt(configMap, "port")),
			SlaveID:   intPtr(getInt(configMap, "device_slave_id")), // Device-level slave_id
		}
		config := &domain.ModbusTCP{
			SlaveID:         getInt(configMap, "slave_id"), // Parameter-level slave_id
			FunctionCode:    getInt(configMap, "function_code"),
			RegisterAddress: getInt(configMap, "register_address"),
			Quantity:        getInt(configMap, "quantity"),
			ByteOrder:       getString(configMap, "byte_order"),
			DataType:        getString(configMap, "data_type"), // Data type (uint16, int32, float32, float64)
		}

		// Set default data_type if not provided
		if config.DataType == "" {
			config.DataType = "uint16"
		}

		// Use slave_id from parameter config if provided, otherwise from device
		slaveID := device.SlaveID
		if config.SlaveID > 0 {
			slaveID = &config.SlaveID
		}
		client, err := modbus.NewTCPClient(device, slaveID)
		if err != nil {
			c.JSON(http.StatusOK, gin.H{
				"success": true,
				"data": TestConnectionResponse{
					Connected: false,
					Message:   "Connection failed",
					Details:   err.Error(),
				},
			})
			return
		}
		defer client.Close()

		// Coba baca; kalau gagal tetap anggap koneksi berhasil.
		data, err := client.Read(ctx, config.FunctionCode, uint16(config.RegisterAddress), uint16(config.Quantity))
		if err != nil {
			response = TestConnectionResponse{
				Connected: true,
				Message:   "Connection OK, read failed",
				Details:   err.Error(),
			}
			break
		}

		// Always return raw bytes in hex format and as array
		rawHex := modbus.FormatHex(data)
		rawBytes := make([]int, len(data))
		for i, b := range data {
			rawBytes[i] = int(b)
		}

		// Function code 1 (Read Coils) and 2 (Read Discrete Inputs) read BITS, not registers
		// They return packed bits, not register values
		if config.FunctionCode == 1 || config.FunctionCode == 2 {
			// Unpack bits from response
			bits := modbus.ConvertBitsToArray(data, config.Quantity)
			// Convert to float64 array for JSON response
			bitsArray := make([]float64, len(bits))
			for i, bit := range bits {
				bitsArray[i] = float64(bit)
			}

			response = TestConnectionResponse{
				Connected: true,
				RawValue:  bitsArray,
				RawHex:    rawHex,
				RawBytes:  rawBytes,
				Message:   "Connection successful",
			}
			break
		}

		// Convert bytes ke value; kalau gagal tetap Connected=true.
		dataType := config.DataType
		if dataType == "" {
			dataType = "uint16"
		}

		var rawValue interface{}

		// For uint16 with quantity > 1, return array of individual register values
		if dataType == "uint16" && config.Quantity > 1 {
			registerArray, err := modbus.ConvertBytesToRegisterArray(data, config.Quantity)
			if err != nil {
				response = TestConnectionResponse{
					Connected: true,
					RawHex:    rawHex,
					RawBytes:  rawBytes,
					Message:   "Connection OK, conversion failed",
					Details:   err.Error(),
				}
				break
			}
			// Convert []uint16 to []float64 for JSON response
			rawValueArray := make([]float64, len(registerArray))
			for i, v := range registerArray {
				rawValueArray[i] = float64(v)
			}
			rawValue = rawValueArray
		} else {
			// For other data types or single uint16, convert to single value
			var rawValueFloat float64
			var conversionErr error
			switch dataType {
			case "float32", "float": // Support both "float32" and "float" (alias)
				rawFloat32, err := modbus.ConvertBytesToFloat32(data, config.ByteOrder, config.Quantity)
				if err != nil {
					conversionErr = err
				} else {
					rawValueFloat = float64(rawFloat32)
				}
			case "float64":
				rawFloat64, err := modbus.ConvertBytesToFloat64(data, config.ByteOrder, config.Quantity)
				if err != nil {
					conversionErr = err
				} else {
					rawValueFloat = rawFloat64
				}
			case "int32":
				rawInt32, err := modbus.ConvertBytesToSignedInt32(data, config.ByteOrder, config.Quantity)
				if err != nil {
					conversionErr = err
				} else {
					rawValueFloat = float64(rawInt32)
				}
			default: // uint16 (single), uint32, or default
				rawValueUint, err := modbus.ConvertBytesToValue(data, config.ByteOrder, config.Quantity)
				if err != nil {
					conversionErr = err
				} else {
					rawValueFloat = float64(rawValueUint)
				}
			}
			if conversionErr != nil {
				response = TestConnectionResponse{
					Connected: true,
					RawHex:    rawHex,
					RawBytes:  rawBytes,
					Message:   "Connection OK, conversion failed",
					Details:   conversionErr.Error(),
				}
				break
			}
			rawValue = rawValueFloat
		}

		response = TestConnectionResponse{
			Connected: true,
			RawValue:  rawValue,
			RawHex:    rawHex,
			RawBytes:  rawBytes,
			Message:   "Connection successful",
		}

	case "snmp":
		configMap := req.Config.(map[string]interface{})
		config := &domain.SNMPConfig{
			IPAddress: getString(configMap, "ip_address"),
			Community: getString(configMap, "community"),
			OID:       getString(configMap, "oid"),
			Version:   getString(configMap, "version"),
			Port:      getInt(configMap, "port"),
		}
		if config.Version == "" {
			config.Version = "v2c" // default
		}
		if config.Port == 0 {
			config.Port = 161 // default SNMP port
		}

		client, err := snmp.NewSNMPClient(config)
		if err != nil {
			c.JSON(http.StatusOK, gin.H{
				"success": true,
				"data": TestConnectionResponse{
					Connected: false,
					Message:   "Connection failed",
					Details:   err.Error(),
				},
			})
			return
		}
		defer client.Close()

		rawValue, err := client.Get(ctx, config.OID)
		if err != nil {
			c.JSON(http.StatusOK, gin.H{
				"success": true,
				"data": TestConnectionResponse{
					Connected: false,
					Message:   "Get failed",
					Details:   err.Error(),
				},
			})
			return
		}

		// CRITICAL: If rawValue is string, try to convert to numeric
		// This ensures that numeric strings like "229.60" are returned as numbers, not strings
		var processedValue interface{} = rawValue
		if strValue, ok := rawValue.(string); ok {
			log.Debugf("[SNMP TEST] Received string value '%s' (type: string, len: %d)", strValue, len(strValue))
			
			// Trim whitespace first
			trimmedValue := strings.TrimSpace(strValue)
			if trimmedValue != strValue {
				log.Debugf("[SNMP TEST] Trimmed string from '%s' to '%s'", strValue, trimmedValue)
			}
			
			// Try to parse as number (float64 handles both int and float)
			parsedFloat, err := strconv.ParseFloat(trimmedValue, 64)
			if err == nil && !math.IsNaN(parsedFloat) && !math.IsInf(parsedFloat, 0) {
				// Check if it's a whole number (int) or has decimal (float)
				if parsedFloat == float64(int64(parsedFloat)) {
					// It's a whole number, use int64 for cleaner representation
					processedValue = int64(parsedFloat)
					log.Infof("[SNMP TEST] Converted string '%s' to integer %d (numeric)", strValue, int64(parsedFloat))
				} else {
					// It has decimal places, use float64
					processedValue = parsedFloat
					log.Infof("[SNMP TEST] Converted string '%s' to float %.2f (numeric)", strValue, parsedFloat)
				}
			} else {
				// Cannot parse as number, keep as string (will require string mapping)
				processedValue = strValue
				log.Debugf("[SNMP TEST] String '%s' cannot be parsed as number (error: %v), keeping as string (requires mapping)", strValue, err)
			}
		} else {
			// Already numeric or other type, use as-is
			log.Debugf("[SNMP TEST] Raw value is not string (type: %T), using as-is", rawValue)
		}

		response = TestConnectionResponse{
			Connected: true,
			RawValue:  processedValue, // Use processed value (numeric if convertible, string otherwise)
			Message:   "Connection successful",
		}

	default:
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "Unsupported type for test connection",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data":    response,
	})
}

func getString(m map[string]interface{}, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func getInt(m map[string]interface{}, key string) int {
	if v, ok := m[key]; ok {
		switch val := v.(type) {
		case int:
			return val
		case float64:
			return int(val)
		}
	}
	return 0
}

func stringPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func intPtr(i int) *int {
	if i == 0 {
		return nil
	}
	return &i
}
