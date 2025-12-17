package handlers

import (
	"context"
	"net/http"
	"time"

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
	Message   string      `json:"message"`
	Details   string      `json:"details,omitempty"`
}

func (h *ModbusHandler) TestConnection(c *gin.Context) {
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
		data, err := client.Read(ctx, config.FunctionCode, uint16(config.RegisterAddress), uint16(config.Quantity))
		if err != nil {
			response = TestConnectionResponse{
				Connected: true,
				Message:   "Connection OK, read failed",
				Details:   err.Error(),
			}
			break
		}

		// Convert bytes to value berdasarkan data_type.
		// Kalau konversi gagal, tetap Connected=true tapi dengan detail error.
		var rawValueFloat float64
		dataType := config.DataType
		if dataType == "" {
			dataType = "uint16"
		}

		switch dataType {
		case "float32", "float": // Support both "float32" and "float" (alias)
			rawFloat32, err := modbus.ConvertBytesToFloat32(data, config.ByteOrder, config.Quantity)
			if err != nil {
				response = TestConnectionResponse{
					Connected: true,
					Message:   "Connection OK, conversion failed",
					Details:   err.Error(),
				}
				break
			}
			rawValueFloat = float64(rawFloat32)
		case "float64":
			rawFloat64, err := modbus.ConvertBytesToFloat64(data, config.ByteOrder, config.Quantity)
			if err != nil {
				response = TestConnectionResponse{
					Connected: true,
					Message:   "Connection OK, conversion failed",
					Details:   err.Error(),
				}
				break
			}
			rawValueFloat = rawFloat64
		case "int32":
			rawInt32, err := modbus.ConvertBytesToSignedInt32(data, config.ByteOrder, config.Quantity)
			if err != nil {
				response = TestConnectionResponse{
					Connected: true,
					Message:   "Connection OK, conversion failed",
					Details:   err.Error(),
				}
				break
			}
			rawValueFloat = float64(rawInt32)
		default: // uint16, uint32, or default
			rawValue, err := modbus.ConvertBytesToValue(data, config.ByteOrder, config.Quantity)
			if err != nil {
				response = TestConnectionResponse{
					Connected: true,
					Message:   "Connection OK, conversion failed",
					Details:   err.Error(),
				}
				break
			}
			rawValueFloat = float64(rawValue)
		}

		response = TestConnectionResponse{
			Connected: true,
			RawValue:  rawValueFloat,
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

		// Convert bytes ke value; kalau gagal tetap Connected=true.
		var rawValueFloat float64
		dataType := config.DataType
		if dataType == "" {
			dataType = "uint16"
		}

		switch dataType {
		case "float32", "float": // Support both "float32" and "float" (alias)
			rawFloat32, err := modbus.ConvertBytesToFloat32(data, config.ByteOrder, config.Quantity)
			if err != nil {
				response = TestConnectionResponse{
					Connected: true,
					Message:   "Connection OK, conversion failed",
					Details:   err.Error(),
				}
				break
			}
			rawValueFloat = float64(rawFloat32)
		case "float64":
			rawFloat64, err := modbus.ConvertBytesToFloat64(data, config.ByteOrder, config.Quantity)
			if err != nil {
				response = TestConnectionResponse{
					Connected: true,
					Message:   "Connection OK, conversion failed",
					Details:   err.Error(),
				}
				break
			}
			rawValueFloat = rawFloat64
		case "int32":
			rawInt32, err := modbus.ConvertBytesToSignedInt32(data, config.ByteOrder, config.Quantity)
			if err != nil {
				response = TestConnectionResponse{
					Connected: true,
					Message:   "Connection OK, conversion failed",
					Details:   err.Error(),
				}
				break
			}
			rawValueFloat = float64(rawInt32)
		default: // uint16, uint32, or default
			rawValue, err := modbus.ConvertBytesToValue(data, config.ByteOrder, config.Quantity)
			if err != nil {
				response = TestConnectionResponse{
					Connected: true,
					Message:   "Connection OK, conversion failed",
					Details:   err.Error(),
				}
				break
			}
			rawValueFloat = float64(rawValue)
		}

		response = TestConnectionResponse{
			Connected: true,
			RawValue:  rawValueFloat,
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

		response = TestConnectionResponse{
			Connected: true,
			RawValue:  rawValue,
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
