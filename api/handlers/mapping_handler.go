package handlers

import (
	"aoi-backend/internal/core/database"
	"aoi-backend/internal/core/errors"
	"aoi-backend/internal/core/logger"
	"aoi-backend/internal/domain"
	"aoi-backend/internal/scheduler"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type MappingHandler struct {
	schedulerManager *scheduler.SchedulerManager
}

func NewMappingHandler(schedulerManager *scheduler.SchedulerManager) *MappingHandler {
	return &MappingHandler{
		schedulerManager: schedulerManager,
	}
}

// SaveMappingRequest represents the request payload for saving connection and mappings
type SaveMappingRequest struct {
	Connection   ConnectionData `json:"connection"`
	KoneksiID    *uint          `json:"koneksi_id,omitempty"` // If provided, use existing koneksi instead of creating new one
	Mappings     []MappingData   `json:"mappings"`
}

type ConnectionData struct {
	Name     string                 `json:"name"`
	Protocol string                 `json:"protocol"` // "Modbus TCP", "Modbus RTU", "SNMP", etc.
	Config   map[string]interface{} `json:"config"`
}

type MappingData struct {
	OutputName string `json:"outputName"`
	Formula    string `json:"formula"`
	// Test configuration from TestStep
	SlaveID         *int   `json:"slaveId,omitempty"`
	FunctionCode    *int   `json:"functionCode,omitempty"`
	RegisterAddress *int   `json:"registerAddress,omitempty"`
	Quantity        *int   `json:"quantity,omitempty"`
	DataType        string `json:"dataType,omitempty"`
	ByteOrder       string `json:"byteOrder,omitempty"`
	OID             string `json:"oid,omitempty"` // For SNMP
}

// Save handles POST /api/v1/mapping/save
// It atomically saves connection and all mappings
func (h *MappingHandler) Save(c *gin.Context) {
	var req SaveMappingRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	// Validate request
	if req.Connection.Name == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "Connection name is required",
		})
		return
	}

	if len(req.Mappings) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "At least one mapping is required",
		})
		return
	}

	// Convert protocol name to backend type
	koneksiType := convertProtocolToType(req.Connection.Protocol)
	if koneksiType == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "Invalid protocol type",
		})
		return
	}

	// Start database transaction
	db := database.GetDB()
	tx := db.Begin()
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		}
	}()

	var koneksi *domain.Koneksi

	// Check if koneksi_id is provided (existing connection)
	if req.KoneksiID != nil && *req.KoneksiID > 0 {
		// Use existing koneksi
		var existingKoneksi domain.Koneksi
		if err := tx.First(&existingKoneksi, *req.KoneksiID).Error; err != nil {
			tx.Rollback()
			c.JSON(http.StatusNotFound, gin.H{
				"success": false,
				"error":   "Connection not found",
			})
			return
		}
		koneksi = &existingKoneksi
	} else {
		// Create new Koneksi
		koneksi = &domain.Koneksi{
			Nama:    req.Connection.Name,
			Type:    koneksiType,
			Enabled: true,
		}

		// Extract connection config based on protocol
		if err := extractKoneksiConfig(koneksi, req.Connection.Protocol, req.Connection.Config); err != nil {
			tx.Rollback()
			c.JSON(http.StatusBadRequest, gin.H{
				"success": false,
				"error":   err.Error(),
			})
			return
		}

		// Save Koneksi
		if err := tx.Create(koneksi).Error; err != nil {
			tx.Rollback()
			c.JSON(http.StatusInternalServerError, gin.H{
				"success": false,
				"error":   "Failed to create connection: " + err.Error(),
			})
			return
		}
	}

	// Create Parameters and their specific configs
	var createdParams []domain.Parameter
	for _, mapping := range req.Mappings {
		// Create Parameter
		param := &domain.Parameter{
			DeviceID:    koneksi.ID,
			Nama:        mapping.OutputName,
			Type:        koneksiType,
			ReadWrite:   false, // Default to read-only
			CustomRumus: &mapping.Formula,
		}

		if err := tx.Create(param).Error; err != nil {
			tx.Rollback()
			c.JSON(http.StatusInternalServerError, gin.H{
				"success": false,
				"error":   "Failed to create parameter: " + err.Error(),
			})
			return
		}

		// Create protocol-specific config
		if err := createProtocolConfig(tx, param, koneksiType, mapping, req.Connection.Config); err != nil {
			tx.Rollback()
			c.JSON(http.StatusInternalServerError, gin.H{
				"success": false,
				"error":   "Failed to create protocol config: " + err.Error(),
			})
			return
		}

		createdParams = append(createdParams, *param)
	}

	// Commit transaction
	if err := tx.Commit().Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   "Failed to commit transaction: " + err.Error(),
		})
		return
	}

	// Reload scheduler to pick up new parameters
	if h.schedulerManager != nil {
		logger.GetLogger().Info("Reloading scheduler after mapping save")
		h.schedulerManager.Reload()
		
		// Trigger immediate execution for newly created parameters
		// This ensures data appears in dashboard immediately
		go func() {
			// Wait a bit for scheduler to finish reloading
			time.Sleep(200 * time.Millisecond)
			
			// Trigger immediate execution for each new parameter
			for _, param := range createdParams {
				logger.GetLogger().Infof("Triggering immediate execution for parameter %d (type: %s)", param.ID, param.Type)
				h.schedulerManager.TriggerImmediateExecution(param.ID, param.Type)
			}
		}()
	}

	c.JSON(http.StatusCreated, gin.H{
		"success": true,
		"data": gin.H{
			"koneksi_id": koneksi.ID,
			"koneksi_name": koneksi.Nama,
			"parameters_count": len(createdParams),
		},
		"message": "Connection and mappings saved successfully",
	})
}

// convertProtocolToType converts frontend protocol name to backend type
func convertProtocolToType(protocol string) string {
	switch protocol {
	case "Modbus TCP":
		return "modbus_tcp"
	case "Modbus RTU":
		return "modbus_rtu"
	case "SNMP":
		return "snmp"
	case "Digital Input":
		return "digital"
	case "Digital Output":
		return "digital"
	case "Analog Input":
		return "analog"
	case "Analog Output":
		return "analog"
	default:
		return ""
	}
}

// extractKoneksiConfig extracts connection-level config from request
func extractKoneksiConfig(koneksi *domain.Koneksi, protocol string, config map[string]interface{}) error {
	switch protocol {
	case "Modbus TCP":
		if host, ok := config["host"].(string); ok && host != "" {
			koneksi.IPAddress = &host
		}
		if port, ok := config["port"].(float64); ok {
			portInt := int(port)
			koneksi.TCPPort = &portInt
		}
		// Default slave ID from config if available
		if slaveID, ok := config["slaveId"].(float64); ok {
			slaveIDInt := int(slaveID)
			koneksi.SlaveID = &slaveIDInt
		}
		// Default polling from config if available
		if polling, ok := config["pollingMs"].(float64); ok {
			koneksi.PollingMs = int(polling)
		} else {
			koneksi.PollingMs = 1000 // Default
		}

	case "Modbus RTU":
		if port, ok := config["serialPort"].(string); ok && port != "" {
			koneksi.Port = &port
		}
		if baud, ok := config["baudRate"].(float64); ok {
			baudInt := int(baud)
			koneksi.Baud = &baudInt
		}
		if dataBits, ok := config["dataBits"].(float64); ok {
			dataBitsInt := int(dataBits)
			koneksi.DataBits = &dataBitsInt
		}
		if stopBits, ok := config["stopBits"].(float64); ok {
			stopBitsInt := int(stopBits)
			koneksi.StopBits = &stopBitsInt
		}
		if parity, ok := config["parity"].(string); ok {
			// Convert frontend parity to backend format
			var parityStr string
			switch parity {
			case "none":
				parityStr = "N"
			case "even":
				parityStr = "E"
			case "odd":
				parityStr = "O"
			default:
				parityStr = "N"
			}
			koneksi.Parity = &parityStr
		}
		// Default slave ID from config if available
		if slaveID, ok := config["slaveId"].(float64); ok {
			slaveIDInt := int(slaveID)
			koneksi.SlaveID = &slaveIDInt
		}
		// Default polling
		if polling, ok := config["pollingMs"].(float64); ok {
			koneksi.PollingMs = int(polling)
		} else {
			koneksi.PollingMs = 1000
		}

	case "SNMP":
		if host, ok := config["host"].(string); ok && host != "" {
			koneksi.IPAddress = &host
		}
		if port, ok := config["port"].(float64); ok {
			portInt := int(port)
			koneksi.TCPPort = &portInt
		}
		// Default polling
		if polling, ok := config["pollingMs"].(float64); ok {
			koneksi.PollingMs = int(polling)
		} else {
			koneksi.PollingMs = 1000
		}
	}

	// Default timeout and retry
	if koneksi.TimeoutMs == 0 {
		koneksi.TimeoutMs = 1000
	}
	if koneksi.RetryCount == 0 {
		koneksi.RetryCount = 3
	}

	return nil
}

// createProtocolConfig creates protocol-specific configuration for a parameter
func createProtocolConfig(tx *gorm.DB, param *domain.Parameter, paramType string, mapping MappingData, connectionConfig map[string]interface{}) error {
	switch paramType {
	case "modbus_tcp":
		if mapping.FunctionCode == nil || mapping.RegisterAddress == nil || mapping.Quantity == nil {
			return errors.NewValidationError("Missing required Modbus TCP fields", "")
		}

		// Use mapping slave_id if provided, otherwise use connection config default
		slaveID := 1 // Default
		if mapping.SlaveID != nil {
			slaveID = *mapping.SlaveID
		} else if connSlaveID, ok := connectionConfig["slaveId"].(float64); ok {
			slaveID = int(connSlaveID)
		}

		byteOrder := "ABCD" // Default
		if mapping.ByteOrder != "" {
			byteOrder = mapping.ByteOrder
		}

		dataType := "uint16" // Default
		if mapping.DataType != "" {
			dataType = mapping.DataType
		}

		modbusTCP := &domain.ModbusTCP{
			IDParameter:     param.ID,
			ReadWrite:       false,
			SlaveID:         slaveID,
			FunctionCode:    *mapping.FunctionCode,
			RegisterAddress: *mapping.RegisterAddress,
			Quantity:        *mapping.Quantity,
			ByteOrder:       byteOrder,
			DataType:        dataType,
			Scaling:         1.0,
		}

		if err := tx.Create(modbusTCP).Error; err != nil {
			return err
		}

	case "modbus_rtu":
		if mapping.FunctionCode == nil || mapping.RegisterAddress == nil || mapping.Quantity == nil {
			return errors.NewValidationError("Missing required Modbus RTU fields", "")
		}

		// Use mapping slave_id if provided, otherwise use connection config default
		slaveID := 1 // Default
		if mapping.SlaveID != nil {
			slaveID = *mapping.SlaveID
		} else if connSlaveID, ok := connectionConfig["slaveId"].(float64); ok {
			slaveID = int(connSlaveID)
		}

		byteOrder := "ABCD" // Default
		if mapping.ByteOrder != "" {
			byteOrder = mapping.ByteOrder
		}

		dataType := "uint16" // Default
		if mapping.DataType != "" {
			dataType = mapping.DataType
		}

		modbusRTU := &domain.ModbusRTU{
			IDParameter:     param.ID,
			ReadWrite:       false,
			SlaveID:         slaveID,
			FunctionCode:    *mapping.FunctionCode,
			RegisterAddress: *mapping.RegisterAddress,
			Quantity:        *mapping.Quantity,
			ByteOrder:       byteOrder,
			DataType:        dataType,
			Scaling:         1.0,
		}

		if err := tx.Create(modbusRTU).Error; err != nil {
			return err
		}

	case "snmp":
		if mapping.OID == "" {
			return errors.NewValidationError("OID is required for SNMP", "")
		}

		community := "public" // Default
		if comm, ok := connectionConfig["community"].(string); ok && comm != "" {
			community = comm
		}

		version := "v2c" // Default
		if ver, ok := connectionConfig["version"].(string); ok && ver != "" {
			version = ver
		}

		ipAddress := ""
		if ip, ok := connectionConfig["host"].(string); ok && ip != "" {
			ipAddress = ip
		}

		port := 161 // Default
		if p, ok := connectionConfig["port"].(float64); ok {
			port = int(p)
		}

		pollingMs := 1000 // Default
		if polling, ok := connectionConfig["pollingMs"].(float64); ok {
			pollingMs = int(polling)
		}

		snmpConfig := &domain.SNMPConfig{
			IDParameter: param.ID,
			Version:     version,
			Community:   community,
			OID:         mapping.OID,
			IPAddress:   ipAddress,
			Port:        port,
			PollingMs:   pollingMs,
		}

		if err := tx.Create(snmpConfig).Error; err != nil {
			return err
		}

	// TODO: Add support for digital_input, digital_output, analog_input, analog_output
	// For now, we'll skip these as they may not be used in the current flow
	}

	return nil
}

