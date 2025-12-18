package handlers

import (
	"aoi-backend/internal/core/database"
	"aoi-backend/internal/core/errors"
	"aoi-backend/internal/core/logger"
	"aoi-backend/internal/domain"
	"aoi-backend/internal/scheduler"
	"fmt"
	"net/http"
	"regexp"
	"strings"
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
	Connection ConnectionData `json:"connection"`
	KoneksiID  *uint          `json:"koneksi_id,omitempty"` // If provided, use existing koneksi instead of creating new one
	Mappings   []MappingData  `json:"mappings"`
}

type ConnectionData struct {
	Name     string                 `json:"name"`
	Protocol string                 `json:"protocol"` // "Modbus TCP", "Modbus RTU", "SNMP", etc.
	Config   map[string]interface{} `json:"config"`
}

type MappingData struct {
	Type       string `json:"type,omitempty"` // "raw" atau "formula" - jika tidak ada, auto-detect
	OutputName string `json:"outputName"`

	// Untuk RAW parameter
	Formula         string `json:"formula,omitempty"` // Formula sederhana (value * 2) - bukan field reference
	SlaveID         *int   `json:"slaveId,omitempty"`
	FunctionCode    *int   `json:"functionCode,omitempty"`
	RegisterAddress *int   `json:"registerAddress,omitempty"`
	Quantity        *int   `json:"quantity,omitempty"`
	DataType        string `json:"dataType,omitempty"`
	ByteOrder       string `json:"byteOrder,omitempty"`
	OID             string `json:"oid,omitempty"` // For SNMP

	// Untuk FORMULA parameter
	Expression string            `json:"expression,omitempty"` // "a * 2" atau "(a + b) / 2"
	Variables  []FormulaVariable `json:"variables,omitempty"`
}

type FormulaVariable struct {
	Name                string `json:"name"`                            // "a", "b", dll
	SourceParameterID   *uint  `json:"source_parameter_id,omitempty"`   // Direct reference via ID
	SourceParameterName string `json:"source_parameter_name,omitempty"` // Reference via parameter name
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

	// Initialize repositories (if needed for future use)

	// Step 1: Create RAW parameters first
	// Mapping: parameter name → parameter_id (untuk reference oleh formula)
	paramNameToID := make(map[string]uint)
	var createdParams []domain.Parameter
	var formulaMappings []MappingData

	// Phase 1: Create RAW parameters
	for _, mapping := range req.Mappings {
		if isRawParameter(mapping, koneksiType) {
			// Create raw parameter
			param := &domain.Parameter{
				DeviceID:  koneksi.ID,
				Nama:      mapping.OutputName,
				Type:      koneksiType,
				ReadWrite: false,
				IsFormula: false,
			}

			// Handle simple formula (value * 2) - bukan field reference
			if mapping.Formula != "" && !containsFieldReference(mapping.Formula) {
				param.CustomRumus = &mapping.Formula
			}

			if err := tx.Create(param).Error; err != nil {
				tx.Rollback()
				c.JSON(http.StatusInternalServerError, gin.H{
					"success": false,
					"error":   "Failed to create raw parameter: " + err.Error(),
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

			// Store mapping for formula reference
			paramNameToID[mapping.OutputName] = param.ID
			createdParams = append(createdParams, *param)
			logger.GetLogger().Infof("Created raw parameter: id=%d, name=%s", param.ID, param.Nama)
		} else if isFormulaParameter(mapping) {
			// Store for formula creation later
			formulaMappings = append(formulaMappings, mapping)
		} else {
			// Unknown mapping type - skip or error?
			logger.GetLogger().Warnf("Unknown mapping type for outputName=%s, skipping", mapping.OutputName)
		}
	}

	// Phase 2: Create FORMULA parameters
	for _, mapping := range formulaMappings {
		// Validate formula mapping
		if mapping.Expression == "" {
			tx.Rollback()
			c.JSON(http.StatusBadRequest, gin.H{
				"success": false,
				"error":   fmt.Sprintf("Expression is required for formula parameter '%s'", mapping.OutputName),
			})
			return
		}

		if len(mapping.Variables) == 0 {
			tx.Rollback()
			c.JSON(http.StatusBadRequest, gin.H{
				"success": false,
				"error":   fmt.Sprintf("Variables are required for formula parameter '%s'", mapping.OutputName),
			})
			return
		}

		// Resolve variables to parameter IDs
		varMappings, err := resolveVariables(tx, koneksi.ID, mapping.Variables, paramNameToID)
		if err != nil {
			tx.Rollback()
			c.JSON(http.StatusBadRequest, gin.H{
				"success": false,
				"error":   fmt.Sprintf("Failed to resolve variables: %v", err),
			})
			return
		}

		// Validate expression contains all variables
		if err := validateExpression(mapping.Expression, varMappings); err != nil {
			tx.Rollback()
			c.JSON(http.StatusBadRequest, gin.H{
				"success": false,
				"error":   fmt.Sprintf("Invalid expression: %v", err),
			})
			return
		}

		// Create formula parameter
		param := &domain.Parameter{
			DeviceID:  koneksi.ID,
			Nama:      mapping.OutputName,
			Type:      "formula",
			ReadWrite: false,
			IsFormula: true,
		}

		if err := tx.Create(param).Error; err != nil {
			tx.Rollback()
			c.JSON(http.StatusInternalServerError, gin.H{
				"success": false,
				"error":   "Failed to create formula parameter: " + err.Error(),
			})
			return
		}

		// Create parameter_formula
		formula := &domain.ParameterFormula{
			ParameterID:    param.ID,
			Expression:     mapping.Expression, // Sudah normalized: "a * 2"
			ExpressionType: "math",
		}

		if err := tx.Create(formula).Error; err != nil {
			tx.Rollback()
			c.JSON(http.StatusInternalServerError, gin.H{
				"success": false,
				"error":   "Failed to create formula: " + err.Error(),
			})
			return
		}

		// Create parameter_formula_var entries
		for varName, sourceParamID := range varMappings {
			formulaVar := &domain.ParameterFormulaVar{
				FormulaID:         formula.ID,
				VarName:           varName,
				SourceParameterID: sourceParamID,
			}

			if err := tx.Create(formulaVar).Error; err != nil {
				tx.Rollback()
				c.JSON(http.StatusInternalServerError, gin.H{
					"success": false,
					"error":   "Failed to create formula variable: " + err.Error(),
				})
				return
			}
		}

		createdParams = append(createdParams, *param)
		logger.GetLogger().Infof("Created formula parameter: id=%d, name=%s, expression=%s", param.ID, param.Nama, mapping.Expression)
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
			"koneksi_id":       koneksi.ID,
			"koneksi_name":     koneksi.Nama,
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

// isRawParameter checks if mapping is a raw parameter
// Raw parameter has Modbus/SNMP config (slaveId, registerAddress, or oid)
func isRawParameter(mapping MappingData, koneksiType string) bool {
	// Explicit type
	if mapping.Type == "raw" {
		return true
	}

	// Has Modbus config
	if (koneksiType == "modbus_tcp" || koneksiType == "modbus_rtu") &&
		mapping.SlaveID != nil && mapping.RegisterAddress != nil {
		return true
	}

	// Has SNMP config
	if koneksiType == "snmp" && mapping.OID != "" {
		return true
	}

	// Has formula but NOT expression+variables (legacy simple formula)
	if mapping.Formula != "" && mapping.Expression == "" && len(mapping.Variables) == 0 {
		// Check if it's a simple formula (value * 2) not field reference
		return !containsFieldReference(mapping.Formula)
	}

	return false
}

// isFormulaParameter checks if mapping is a formula parameter
// Formula parameter has expression and variables
func isFormulaParameter(mapping MappingData) bool {
	// Explicit type
	if mapping.Type == "formula" {
		return true
	}

	// Has expression and variables
	if mapping.Expression != "" && len(mapping.Variables) > 0 {
		return true
	}

	return false
}

// containsFieldReference checks if formula contains field references like "Slave X Addr Y"
// This is used to detect legacy format that should not be used anymore
func containsFieldReference(formula string) bool {
	if formula == "" {
		return false
	}

	// Pattern to match "Slave X Addr Y" or "Slave X, Address Y" or variations
	re := regexp.MustCompile(`(?i)slave\s+\d+.*?(?:addr|address)\s+\d+`)
	return re.MatchString(formula)
}

// resolveVariables resolves formula variables to parameter IDs
func resolveVariables(tx *gorm.DB, deviceID uint, variables []FormulaVariable, paramNameToID map[string]uint) (map[string]uint, error) {
	result := make(map[string]uint)

	for _, v := range variables {
		if v.Name == "" {
			return nil, fmt.Errorf("variable name is required")
		}

		var paramID uint
		var found bool

		// Priority 1: Direct parameter_id
		if v.SourceParameterID != nil && *v.SourceParameterID > 0 {
			paramID = *v.SourceParameterID
			found = true
		} else if v.SourceParameterName != "" {
			// Priority 2: Via parameter name (newly created in this batch)
			if id, exists := paramNameToID[v.SourceParameterName]; exists {
				paramID = id
				found = true
			} else {
				// Priority 3: Query from database
				var param domain.Parameter
				if err := tx.Where("device_id = ? AND nama = ?", deviceID, v.SourceParameterName).
					First(&param).Error; err == nil {
					paramID = param.ID
					found = true
				}
			}
		}

		if !found {
			return nil, fmt.Errorf("source parameter not found for variable '%s'", v.Name)
		}

		result[v.Name] = paramID
		logger.GetLogger().Debugf("Resolved variable '%s' to parameter_id=%d", v.Name, paramID)
	}

	return result, nil
}

// validateExpression validates that expression contains all variables
func validateExpression(expression string, varMappings map[string]uint) error {
	if expression == "" {
		return fmt.Errorf("expression is required")
	}

	// Check if expression contains all variable names
	for varName := range varMappings {
		// Simple check: variable name should appear in expression
		// This is basic validation - actual evaluation happens at runtime
		if !strings.Contains(expression, varName) {
			logger.GetLogger().Warnf("Variable '%s' not found in expression '%s'", varName, expression)
		}
	}

	return nil
}

// NOTE: Old functions resolveFieldReferencesWithMapping and normalizeExpression have been removed
// as they were used for parsing register addresses from formula strings, which violates the new design.
// Formula parameters now use explicit variable references via parameter_id or parameter_name.
