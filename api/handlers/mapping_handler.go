package handlers

import (
	"aoi-backend/internal/core/cache"
	"aoi-backend/internal/core/database"
	"aoi-backend/internal/core/errors"
	"aoi-backend/internal/core/logger"
	"aoi-backend/internal/domain"
	"aoi-backend/internal/scheduler"
	"aoi-backend/internal/service"
	"encoding/json"
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
	formulaEngine    *service.FormulaEngine
	cache            cache.Cache
}

func NewMappingHandler(schedulerManager *scheduler.SchedulerManager, formulaEngine *service.FormulaEngine, cacheInstance cache.Cache) *MappingHandler {
	return &MappingHandler{
		schedulerManager: schedulerManager,
		formulaEngine:    formulaEngine,
		cache:            cacheInstance,
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
	OutputType string `json:"output_type"` // "DIRECT" atau "FORMULA" - REQUIRED, no auto-detect
	OutputName string `json:"outputName"`

	// Untuk DIRECT type
	SourceField       string `json:"source_field,omitempty"`        // Source field name (e.g., "Slave 1 Addr 7")
	SourceParameterID *uint  `json:"source_parameter_id,omitempty"` // Source parameter ID for DIRECT (single source only)
	SlaveID           *int   `json:"slaveId,omitempty"`
	FunctionCode      *int   `json:"functionCode,omitempty"`
	RegisterAddress   *int   `json:"registerAddress,omitempty"`
	Quantity          *int   `json:"quantity,omitempty"`
	DataType          string `json:"dataType,omitempty"`
	ByteOrder         string `json:"byteOrder,omitempty"`
	OID               string `json:"oid,omitempty"`            // For SNMP
	StringMapping     string `json:"string_mapping,omitempty"` // JSON string mapping for SNMP string values

	// Untuk FORMULA type
	Formula            string            `json:"formula,omitempty"`              // Formula expression like "(Slave 1 Addr 7 + Slave 1 Addr 8) / 10"
	Expression         string            `json:"expression,omitempty"`           // Normalized expression "a * 2" atau "(a + b) / 2"
	Variables          []FormulaVariable `json:"variables,omitempty"`            // Formula variables
	SourceParameterIDs []uint            `json:"source_parameter_ids,omitempty"` // Multiple source parameter IDs for FORMULA
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

	// Debug: Log all mappings to see if string_mapping is received
	for i, mapping := range req.Mappings {
		logger.GetLogger().Infof("[MAPPING] Received mapping[%d]: outputName=%s, outputType=%s, oid=%s, string_mapping='%s' (len=%d)",
			i, mapping.OutputName, mapping.OutputType, mapping.OID, mapping.StringMapping, len(mapping.StringMapping))
		if mapping.StringMapping != "" {
			logger.GetLogger().Infof("[MAPPING] ✅ Found string_mapping for mapping[%d] '%s': %s", i, mapping.OutputName, mapping.StringMapping)
		} else {
			logger.GetLogger().Warnf("[MAPPING] ⚠️ No string_mapping for mapping[%d] '%s'", i, mapping.OutputName)
		}
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

	// Step 1: Validate and separate mappings by output_type
	// Mapping: parameter name → parameter_id (untuk reference oleh formula)
	paramNameToID := make(map[string]uint)
	var createdParams []domain.Parameter
	var directMappings []MappingData
	var formulaMappings []MappingData
	var sourceMappings []MappingData // Dependencies for FORMULA (should be SOURCE)

	// First pass: Identify which DIRECT mappings are dependencies for FORMULA
	// Also detect OID references in FORMULA and auto-create SOURCE parameters
	formulaDependencyNames := make(map[string]bool)
	oidToSourceMapping := make(map[string]MappingData) // Map OID -> SOURCE mapping to create

	logger.GetLogger().Infof("[MAPPING] Analyzing %d mappings to find FORMULA dependencies...", len(req.Mappings))
	for idx := range req.Mappings {
		mapping := &req.Mappings[idx] // Use pointer to modify original
		logger.GetLogger().Infof("[MAPPING] [%d/%d] Mapping: outputName=%s, outputType=%s", idx+1, len(req.Mappings), mapping.OutputName, mapping.OutputType)
		if mapping.OutputType == "FORMULA" {
			logger.GetLogger().Infof("[MAPPING] Found FORMULA mapping: name=%s, variables_count=%d, formula='%s', expression='%s'",
				mapping.OutputName, len(mapping.Variables), mapping.Formula, mapping.Expression)

			// If no variables provided, try to extract OID references from formula/expression
			if len(mapping.Variables) == 0 {
				formulaText := mapping.Expression
				if formulaText == "" {
					formulaText = mapping.Formula
				}

				if formulaText != "" {
					logger.GetLogger().Infof("[MAPPING] No variables provided, attempting to extract OID references from formula: '%s'", formulaText)

					// Extract OID references (pattern: "OID .1.3.6.1.4.1.16010.1.1.1.5.0" or "OID .1.3.6.1.4.1.16010.1.1.1.5.0 /10")
					oidPattern := regexp.MustCompile(`(?i)OID\s+([\.\d]+)`)
					oidMatches := oidPattern.FindAllStringSubmatch(formulaText, -1)

					if len(oidMatches) > 0 {
						logger.GetLogger().Infof("[MAPPING] Found %d OID references in formula", len(oidMatches))

						// Generate variables and SOURCE mappings for each OID
						varName := 'a'
						for _, oidMatch := range oidMatches {
							oid := oidMatch[1]
							varNameStr := string(varName)

							logger.GetLogger().Infof("[MAPPING]   OID: %s → variable '%s'", oid, varNameStr)

							// Check if we already have a SOURCE mapping for this OID
							if _, exists := oidToSourceMapping[oid]; !exists {
								// Create SOURCE mapping for this OID
								sourceMapping := MappingData{
									OutputType: "DIRECT",                      // Will be converted to SOURCE
									OutputName: fmt.Sprintf("SOURCE_%s", oid), // Auto-generated name
									OID:        oid,
								}
								oidToSourceMapping[oid] = sourceMapping
								formulaDependencyNames[sourceMapping.OutputName] = true
								logger.GetLogger().Infof("[MAPPING]   Created SOURCE mapping for OID %s: name=%s", oid, sourceMapping.OutputName)
							} else {
								// Use existing SOURCE mapping
								existingMapping := oidToSourceMapping[oid]
								formulaDependencyNames[existingMapping.OutputName] = true
								logger.GetLogger().Infof("[MAPPING]   Reusing existing SOURCE mapping for OID %s: name=%s", oid, existingMapping.OutputName)
							}

							varName++
						}

						// Auto-generate variables for the FORMULA mapping
						varName = 'a'
						for _, oidMatch := range oidMatches {
							oid := oidMatch[1]
							varNameStr := string(varName)

							// Get the SOURCE mapping name for this OID
							sourceMapping := oidToSourceMapping[oid]

							// Add variable to mapping
							mapping.Variables = append(mapping.Variables, FormulaVariable{
								Name:                varNameStr,
								SourceParameterName: sourceMapping.OutputName,
							})

							logger.GetLogger().Infof("[MAPPING]   Generated variable '%s' → SOURCE parameter '%s' (OID: %s)",
								varNameStr, sourceMapping.OutputName, oid)

							varName++
						}

						// Normalize expression: replace "OID .1.3.6.1.4.1.16010.1.1.1.5.0" with variable name
						varName = 'a'
						normalizedExpression := formulaText
						for _, oidMatch := range oidMatches {
							oid := oidMatch[1]
							varNameStr := string(varName)

							// Replace "OID <oid>" with variable name
							oidPattern := regexp.MustCompile(fmt.Sprintf(`(?i)OID\s+%s`, regexp.QuoteMeta(oid)))
							normalizedExpression = oidPattern.ReplaceAllString(normalizedExpression, varNameStr)

							varName++
						}

						// Update mapping with normalized expression and variables
						mapping.Expression = normalizedExpression
						logger.GetLogger().Infof("[MAPPING]   Normalized expression: '%s' → '%s'", formulaText, normalizedExpression)
					} else {
						logger.GetLogger().Warnf("[MAPPING] ⚠️ FORMULA '%s' has no variables and no OID references found in formula", mapping.OutputName)
					}
				}
			}

			// Extract parameter names from variables (after auto-generation)
			for varIdx, v := range mapping.Variables {
				logger.GetLogger().Infof("[MAPPING]   Variable[%d]: name=%s, source_parameter_name='%s', source_parameter_id=%v",
					varIdx+1, v.Name, v.SourceParameterName, v.SourceParameterID)
				if v.SourceParameterName != "" {
					formulaDependencyNames[v.SourceParameterName] = true
					logger.GetLogger().Infof("[MAPPING] ✅ FORMULA '%s' depends on parameter '%s'", mapping.OutputName, v.SourceParameterName)
				} else if v.SourceParameterID != nil {
					logger.GetLogger().Warnf("[MAPPING] ⚠️ FORMULA '%s' variable '%s' uses source_parameter_id=%d (not name), cannot auto-create SOURCE",
						mapping.OutputName, v.Name, *v.SourceParameterID)
				}
			}
		}
	}
	logger.GetLogger().Infof("[MAPPING] Found %d FORMULA dependencies (by name): %v", len(formulaDependencyNames), formulaDependencyNames)

	// Add auto-generated SOURCE mappings to sourceMappings
	for oid, sourceMapping := range oidToSourceMapping {
		logger.GetLogger().Infof("[MAPPING] Adding auto-generated SOURCE mapping for OID %s: name=%s", oid, sourceMapping.OutputName)
		sourceMappings = append(sourceMappings, sourceMapping)
	}

	// Validate all mappings first
	for _, mapping := range req.Mappings {
		// Validate output_type is present and valid
		if mapping.OutputType != "DIRECT" && mapping.OutputType != "FORMULA" {
			tx.Rollback()
			c.JSON(http.StatusBadRequest, gin.H{
				"success": false,
				"error":   fmt.Sprintf("Invalid output_type '%s' for mapping '%s'. Must be 'DIRECT' or 'FORMULA'", mapping.OutputType, mapping.OutputName),
			})
			return
		}

		// Validate DIRECT type
		if mapping.OutputType == "DIRECT" {
			if mapping.SourceField == "" && mapping.SourceParameterID == nil {
				tx.Rollback()
				c.JSON(http.StatusBadRequest, gin.H{
					"success": false,
					"error":   fmt.Sprintf("DIRECT type requires source_field or source_parameter_id for mapping '%s'", mapping.OutputName),
				})
				return
			}
			// DIRECT cannot have formula
			if mapping.Formula != "" {
				tx.Rollback()
				c.JSON(http.StatusBadRequest, gin.H{
					"success": false,
					"error":   fmt.Sprintf("DIRECT type cannot have formula for mapping '%s'", mapping.OutputName),
				})
				return
			}

			// Check if this DIRECT parameter is a dependency for FORMULA
			if formulaDependencyNames[mapping.OutputName] {
				// This is a SOURCE parameter (dependency for FORMULA)
				sourceMappings = append(sourceMappings, mapping)
				logger.GetLogger().Infof("[MAPPING] ✅ DIRECT parameter '%s' is a FORMULA dependency → will be created as SOURCE", mapping.OutputName)
			} else {
				// This is a standalone DIRECT parameter
				directMappings = append(directMappings, mapping)
				logger.GetLogger().Infof("[MAPPING] DIRECT parameter '%s' is standalone → will be created as DIRECT", mapping.OutputName)
			}
		}

		// Validate FORMULA type
		if mapping.OutputType == "FORMULA" {
			if mapping.Formula == "" && mapping.Expression == "" {
				tx.Rollback()
				c.JSON(http.StatusBadRequest, gin.H{
					"success": false,
					"error":   fmt.Sprintf("FORMULA type requires formula or expression for mapping '%s'", mapping.OutputName),
				})
				return
			}
			formulaMappings = append(formulaMappings, mapping)
		}
	}

	// Phase 1a: Create SOURCE parameters first (dependencies for FORMULA - internal only)
	logger.GetLogger().Infof("[MAPPING] Phase 1a: Creating %d SOURCE parameters (FORMULA dependencies)", len(sourceMappings))
	for idx, mapping := range sourceMappings {
		logger.GetLogger().Infof("[MAPPING] [%d/%d] Processing SOURCE mapping: name=%s, oid=%s, type=%s",
			idx+1, len(sourceMappings), mapping.OutputName, mapping.OID, koneksiType)

		// Create SOURCE parameter (internal only, not broadcast)
		param := &domain.Parameter{
			DeviceID:      koneksi.ID,
			Nama:          mapping.OutputName,
			Type:          koneksiType,
			ReadWrite:     false,
			IsFormula:     false,
			ParameterMode: domain.ParameterModeSOURCE, // SOURCE = polling device, internal only, tidak dibroadcast
		}

		logger.GetLogger().Infof("[MAPPING] Creating SOURCE parameter: name=%s, device_id=%d, type=%s, mode=SOURCE",
			param.Nama, param.DeviceID, param.Type)

		if err := tx.Create(param).Error; err != nil {
			logger.GetLogger().Errorf("[MAPPING] ❌ Failed to create SOURCE parameter: %v", err)
			tx.Rollback()
			c.JSON(http.StatusInternalServerError, gin.H{
				"success": false,
				"error":   "Failed to create SOURCE parameter: " + err.Error(),
			})
			return
		}

		logger.GetLogger().Infof("[MAPPING] ✅ SOURCE parameter created: id=%d, name=%s", param.ID, param.Nama)

		// Create protocol-specific config
		logger.GetLogger().Infof("[MAPPING] Creating protocol config for SOURCE parameter: id=%d, name=%s, type=%s, oid=%s",
			param.ID, param.Nama, koneksiType, mapping.OID)
		if err := createProtocolConfig(tx, param, koneksiType, mapping, req.Connection.Config); err != nil {
			logger.GetLogger().Errorf("[MAPPING] ❌ Failed to create protocol config for SOURCE parameter %d: %v", param.ID, err)
			tx.Rollback()
			c.JSON(http.StatusInternalServerError, gin.H{
				"success": false,
				"error":   "Failed to create protocol config: " + err.Error(),
			})
			return
		}
		logger.GetLogger().Infof("[MAPPING] ✅ Protocol config created successfully for SOURCE parameter: id=%d, name=%s", param.ID, param.Nama)

		// Store mapping for formula reference
		paramNameToID[mapping.OutputName] = param.ID
		createdParams = append(createdParams, *param)
		logger.GetLogger().Infof("[MAPPING] ✅ Created SOURCE parameter (FORMULA dependency): id=%d, name=%s, type=%s", param.ID, param.Nama, koneksiType)
	}

	if len(sourceMappings) == 0 {
		logger.GetLogger().Infof("[MAPPING] No SOURCE parameters to create (no FORMULA dependencies)")
	}

	// Phase 1b: Create DIRECT parameters (standalone, broadcast directly)
	for _, mapping := range directMappings {
		// Debug: Log string_mapping if provided
		if mapping.StringMapping != "" {
			logger.GetLogger().Infof("[MAPPING] Creating DIRECT parameter '%s' with string_mapping: %s", mapping.OutputName, mapping.StringMapping)
		}

		// Create DIRECT parameter
		param := &domain.Parameter{
			DeviceID:      koneksi.ID,
			Nama:          mapping.OutputName,
			Type:          koneksiType,
			ReadWrite:     false,
			IsFormula:     false,
			ParameterMode: domain.ParameterModeDIRECT, // DIRECT = polling device, value already clean, broadcast directly
		}

		if err := tx.Create(param).Error; err != nil {
			tx.Rollback()
			c.JSON(http.StatusInternalServerError, gin.H{
				"success": false,
				"error":   "Failed to create DIRECT parameter: " + err.Error(),
			})
			return
		}

		// Create protocol-specific config
		logger.GetLogger().Infof("[MAPPING] Creating protocol config for DIRECT parameter: id=%d, name=%s, type=%s", param.ID, param.Nama, koneksiType)
		if err := createProtocolConfig(tx, param, koneksiType, mapping, req.Connection.Config); err != nil {
			logger.GetLogger().Errorf("[MAPPING] ❌ Failed to create protocol config for DIRECT parameter %d: %v", param.ID, err)
			tx.Rollback()
			c.JSON(http.StatusInternalServerError, gin.H{
				"success": false,
				"error":   "Failed to create protocol config: " + err.Error(),
			})
			return
		}
		logger.GetLogger().Infof("[MAPPING] ✅ Protocol config created successfully for DIRECT parameter: id=%d, name=%s", param.ID, param.Nama)

		// Store mapping for formula reference
		paramNameToID[mapping.OutputName] = param.ID
		createdParams = append(createdParams, *param)
		logger.GetLogger().Infof("Created DIRECT parameter: id=%d, name=%s", param.ID, param.Nama)
	}

	// Phase 2: Create FORMULA parameters
	for idx, mapping := range formulaMappings {
		logger.GetLogger().Infof("[MAPPING] Processing FORMULA mapping [%d/%d]: name=%s, expression='%s', formula='%s', variables_count=%d",
			idx+1, len(formulaMappings), mapping.OutputName, mapping.Expression, mapping.Formula, len(mapping.Variables))

		// Use expression if provided, otherwise use formula
		expression := mapping.Expression
		if expression == "" && mapping.Formula != "" {
			// If expression is not provided but formula is, use formula as expression
			// This handles cases where frontend sends formula but backend expects expression
			expression = mapping.Formula
			logger.GetLogger().Infof("[MAPPING] Using formula as expression: '%s'", expression)
		}

		if expression == "" {
			logger.GetLogger().Errorf("[MAPPING] ❌ Expression is empty for FORMULA parameter '%s'", mapping.OutputName)
			tx.Rollback()
			c.JSON(http.StatusBadRequest, gin.H{
				"success": false,
				"error":   fmt.Sprintf("Expression or formula is required for FORMULA parameter '%s'", mapping.OutputName),
			})
			return
		}

		logger.GetLogger().Infof("[MAPPING] Final expression for FORMULA parameter '%s': '%s'", mapping.OutputName, expression)

		// Variables are required if formula contains field references
		// If variables are not provided, try to extract from formula
		varMappings := make(map[string]uint)
		if len(mapping.Variables) > 0 {
			// Resolve variables to parameter IDs
			var err error
			varMappings, err = resolveVariables(tx, koneksi.ID, mapping.Variables, paramNameToID)
			if err != nil {
				tx.Rollback()
				c.JSON(http.StatusBadRequest, gin.H{
					"success": false,
					"error":   fmt.Sprintf("Failed to resolve variables: %v", err),
				})
				return
			}

			// Validate expression contains all variables
			if err := validateExpression(expression, varMappings); err != nil {
				tx.Rollback()
				c.JSON(http.StatusBadRequest, gin.H{
					"success": false,
					"error":   fmt.Sprintf("Invalid expression: %v", err),
				})
				return
			}
		} else {
			// No variables provided - might be a simple formula without field references
			// This is allowed for backward compatibility
			logger.GetLogger().Warnf("FORMULA parameter '%s' has no variables - using expression as-is", mapping.OutputName)
		}

		// Create formula parameter
		param := &domain.Parameter{
			DeviceID:      koneksi.ID,
			Nama:          mapping.OutputName,
			Type:          "formula",
			ReadWrite:     false,
			IsFormula:     true,
			ParameterMode: domain.ParameterModeFORMULA, // FORMULA = derived parameter, not polling, broadcast
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
			Expression:     expression, // Normalized expression: "a * 2" or raw formula
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

		// Create parameter_formula_var entries (if variables exist)
		logger.GetLogger().Infof("[MAPPING] Creating %d formula variables for parameter_id=%d", len(varMappings), param.ID)
		for varName, sourceParamID := range varMappings {
			formulaVar := &domain.ParameterFormulaVar{
				FormulaID:         formula.ID,
				VarName:           varName,
				SourceParameterID: sourceParamID,
			}

			logger.GetLogger().Infof("[MAPPING] Creating formula variable: formula_id=%d, var_name='%s', source_parameter_id=%d",
				formula.ID, varName, sourceParamID)

			if err := tx.Create(formulaVar).Error; err != nil {
				logger.GetLogger().Errorf("[MAPPING] ❌ Failed to create formula variable: %v", err)
				tx.Rollback()
				c.JSON(http.StatusInternalServerError, gin.H{
					"success": false,
					"error":   "Failed to create formula variable: " + err.Error(),
				})
				return
			}

			logger.GetLogger().Infof("[MAPPING] ✅ Created formula variable: id=%d, var_name='%s', source_parameter_id=%d",
				formulaVar.ID, varName, sourceParamID)
		}

		if len(varMappings) == 0 {
			logger.GetLogger().Warnf("[MAPPING] ⚠️ No formula variables created for parameter_id=%d (expression='%s')", param.ID, expression)
		}

		createdParams = append(createdParams, *param)
		logger.GetLogger().Infof("Created FORMULA parameter: id=%d, name=%s, expression=%s", param.ID, param.Nama, expression)
	}

	// Commit transaction
	logger.GetLogger().Infof("[MAPPING] Committing transaction: %d parameters created (%d SOURCE, %d DIRECT, %d FORMULA)",
		len(createdParams), len(sourceMappings), len(directMappings), len(formulaMappings))
	if err := tx.Commit().Error; err != nil {
		logger.GetLogger().Errorf("[MAPPING] ❌ Failed to commit transaction: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   "Failed to commit transaction: " + err.Error(),
		})
		return
	}
	logger.GetLogger().Infof("[MAPPING] ✅ Transaction committed successfully")

	// Log summary of created parameters
	if len(sourceMappings) > 0 {
		logger.GetLogger().Infof("[MAPPING] Summary: Created %d SOURCE parameters (should have SNMP config)", len(sourceMappings))
		for _, mapping := range sourceMappings {
			if paramID, exists := paramNameToID[mapping.OutputName]; exists {
				logger.GetLogger().Infof("[MAPPING]   - SOURCE parameter '%s' (id=%d) should have SNMP config",
					mapping.OutputName, paramID)
			} else {
				logger.GetLogger().Errorf("[MAPPING] ❌ SOURCE parameter '%s' not found in paramNameToID map!", mapping.OutputName)
			}
		}
	}

	// Reload scheduler to pick up new parameters
	if h.schedulerManager != nil {
		logger.GetLogger().Info("[MAPPING] Reloading scheduler after mapping save")
		h.schedulerManager.Reload()
	}

	// Reload formula engine to pick up new formulas
	if h.formulaEngine != nil {
		logger.GetLogger().Info("[MAPPING] Reloading formula engine after mapping save")
		if err := h.formulaEngine.LoadAll(); err != nil {
			logger.GetLogger().Warnf("[MAPPING] Failed to reload formula engine: %v", err)
		} else {
			logger.GetLogger().Info("[MAPPING] Formula engine reloaded successfully")
			// Evaluate all formulas after reload to ensure all appear in dashboard
			go func() {
				// Wait a bit for cache to be ready
				time.Sleep(500 * time.Millisecond)
				logger.GetLogger().Info("[MAPPING] Evaluating all formulas after reload")
				h.formulaEngine.EvaluateAll()
			}()
		}
	}

	// Collect clean parameters first to identify which ones need immediate execution
	// DIRECT: already clean, broadcast directly
	// CLEAN: logical parameter, clean value, broadcast
	// FORMULA: result of calculation, also clean (ready to use)
	// SOURCE: raw data, internal only, NOT included in clean_parameters
	var cleanParameters []gin.H
	var cleanParamsToTrigger []domain.Parameter

	for _, param := range createdParams {
		if param.ParameterMode == domain.ParameterModeDIRECT ||
			param.ParameterMode == domain.ParameterModeCLEAN ||
			param.ParameterMode == domain.ParameterModeFORMULA {
			cleanParamsToTrigger = append(cleanParamsToTrigger, param)
		}
	}

	// Trigger immediate execution for clean parameters to get values quickly
	// This ensures data appears in cache before we read it
	if h.schedulerManager != nil && len(cleanParamsToTrigger) > 0 {
		// Wait a bit for scheduler to finish reloading
		time.Sleep(200 * time.Millisecond)

		// Trigger immediate execution for clean parameters
		for _, param := range cleanParamsToTrigger {
			logger.GetLogger().Infof("Triggering immediate execution for clean parameter %d (type: %s)", param.ID, param.Type)
			h.schedulerManager.TriggerImmediateExecution(param.ID, param.Type)
		}

		// Wait for immediate execution to complete and data to be cached
		time.Sleep(800 * time.Millisecond)
	}

	// Now collect clean parameters with their values from cache
	// Retry mechanism to get values from cache after execution
	for _, param := range cleanParamsToTrigger {
		var value interface{} = nil

		if h.cache != nil {
			// For DIRECT and CLEAN: use clean cache key
			// For FORMULA: also use clean cache key (formula engine stores result there)
			cleanCacheKey := fmt.Sprintf("parameter:clean:%d", param.ID)

			// Retry up to 3 times with increasing delay
			maxRetries := 3
			for retry := 0; retry < maxRetries; retry++ {
				var cleanData domain.RealtimeData
				if err := h.cache.Get(cleanCacheKey, &cleanData); err == nil {
					// Found clean data
					if cleanData.ProcessedValue != nil {
						value = cleanData.ProcessedValue
						logger.GetLogger().Infof("[MAPPING] Found clean value in cache for parameter %d (retry %d): %v",
							param.ID, retry+1, value)
						break
					}
				}

				// If not found and not last retry, wait a bit longer
				if retry < maxRetries-1 {
					waitTime := time.Duration(300*(retry+1)) * time.Millisecond
					logger.GetLogger().Debugf("[MAPPING] Clean value not found for parameter %d (retry %d), waiting %v...",
						param.ID, retry+1, waitTime)
					time.Sleep(waitTime)

					// Re-trigger execution for this parameter
					if h.schedulerManager != nil {
						h.schedulerManager.TriggerImmediateExecution(param.ID, param.Type)
					}

					// For FORMULA parameters, also trigger formula evaluation
					if param.ParameterMode == domain.ParameterModeFORMULA && h.formulaEngine != nil {
						logger.GetLogger().Debugf("[MAPPING] Triggering formula evaluation for parameter %d", param.ID)
						if err := h.formulaEngine.EvaluateFormula(param.ID); err != nil {
							logger.GetLogger().Warnf("[MAPPING] Failed to evaluate formula for parameter %d: %v", param.ID, err)
						}
					}
				} else {
					logger.GetLogger().Debugf("[MAPPING] Clean value not found for parameter %d after %d retries",
						param.ID, maxRetries)
				}
			}
		}

		cleanParameters = append(cleanParameters, gin.H{
			"id":    param.ID,
			"nama":  param.Nama,
			"value": value,
		})
		logger.GetLogger().Infof("[MAPPING] Added to clean_parameters: id=%d, nama=%s, mode=%s, value=%v",
			param.ID, param.Nama, param.ParameterMode, value)
	}

	// Trigger immediate execution for all parameters in background (for dashboard update)
	if h.schedulerManager != nil {
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

	if len(cleanParameters) == 0 {
		logger.GetLogger().Warnf("[MAPPING] No clean parameters found (only SOURCE parameters created?)")
	} else {
		logger.GetLogger().Infof("[MAPPING] Collected %d clean parameters for response", len(cleanParameters))
	}

	c.JSON(http.StatusCreated, gin.H{
		"success": true,
		"data": gin.H{
			"koneksi_id":       koneksi.ID,
			"koneksi_name":     koneksi.Nama,
			"parameters_count": len(createdParams),
			"clean_parameters": cleanParameters,
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

		// Get community from connectionConfig, required field
		community := ""
		if comm, ok := connectionConfig["community"].(string); ok && comm != "" {
			community = comm
		}
		// If community is empty, use "public" as default for backward compatibility
		// but log a warning that it's using default
		if community == "" {
			logger.GetLogger().Warnf("[MAPPING] SNMP community not provided for parameter %d, using default 'public'", param.ID)
			community = "public"
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

		// Add string mapping if provided (for SNMP string values)
		// CRITICAL: Always save string_mapping if provided
		// Check both empty string and nil to ensure we capture all cases
		logger.GetLogger().Infof("[MAPPING] Processing SNMP config for parameter_id=%d, mapping.StringMapping='%s' (raw, len=%d)",
			param.ID, mapping.StringMapping, len(mapping.StringMapping))

		stringMappingValue := strings.TrimSpace(mapping.StringMapping)
		logger.GetLogger().Infof("[MAPPING] After TrimSpace: stringMappingValue='%s' (len=%d)", stringMappingValue, len(stringMappingValue))

		if stringMappingValue != "" {
			// Validate JSON format before saving
			var testMap map[string]interface{}
			if err := json.Unmarshal([]byte(stringMappingValue), &testMap); err != nil {
				logger.GetLogger().Errorf("[MAPPING] Invalid JSON format for string_mapping (parameter_id=%d): %s, error: %v", param.ID, stringMappingValue, err)
				return fmt.Errorf("invalid string_mapping JSON format: %w", err)
			}

			// CRITICAL: Set pointer to stringMappingValue
			snmpConfig.StringMapping = &stringMappingValue
			logger.GetLogger().Infof("[MAPPING] Set snmpConfig.StringMapping pointer: %p, value: '%s'", snmpConfig.StringMapping, *snmpConfig.StringMapping)
		} else {
			logger.GetLogger().Warnf("[MAPPING] No string_mapping provided for SNMP parameter_id=%d (OID=%s, OutputName=%s, raw='%s')",
				param.ID, mapping.OID, mapping.OutputName, mapping.StringMapping)
		}

		// Log before create
		if snmpConfig.StringMapping != nil {
			logger.GetLogger().Infof("[MAPPING] Before Create: snmpConfig.StringMapping=%p, value='%s'", snmpConfig.StringMapping, *snmpConfig.StringMapping)
		} else {
			logger.GetLogger().Warnf("[MAPPING] Before Create: snmpConfig.StringMapping is NULL!")
		}

		// Log SNMP config details before create
		logger.GetLogger().Infof("[MAPPING] Creating SNMP config: parameter_id=%d, oid=%s, ip=%s, port=%d, polling_ms=%d, string_mapping=%v",
			snmpConfig.IDParameter, snmpConfig.OID, snmpConfig.IPAddress, snmpConfig.Port, snmpConfig.PollingMs,
			func() string {
				if snmpConfig.StringMapping != nil {
					return *snmpConfig.StringMapping
				}
				return "NULL"
			}())

		if err := tx.Create(snmpConfig).Error; err != nil {
			logger.GetLogger().Errorf("[MAPPING] ❌ Failed to create SNMP config for parameter_id=%d: %v", param.ID, err)
			return fmt.Errorf("failed to create SNMP config: %w", err)
		}

		logger.GetLogger().Infof("[MAPPING] ✅ SNMP config created with ID=%d for parameter_id=%d", snmpConfig.ID, param.ID)

		// Verify SNMP config was saved by querying back from database
		var savedConfig domain.SNMPConfig
		if err := tx.First(&savedConfig, snmpConfig.ID).Error; err == nil {
			logger.GetLogger().Infof("[MAPPING] ✅ Verified SNMP config exists in DB: id=%d, parameter_id=%d, oid=%s",
				savedConfig.ID, savedConfig.IDParameter, savedConfig.OID)
			if savedConfig.StringMapping != nil && *savedConfig.StringMapping != "" {
				logger.GetLogger().Infof("[MAPPING] ✅ SNMP config created successfully for parameter_id=%d with string_mapping: %s", param.ID, *savedConfig.StringMapping)
			} else {
				logger.GetLogger().Warnf("[MAPPING] ⚠️ SNMP config created for parameter_id=%d but string_mapping is NULL or empty (ID=%d)", param.ID, savedConfig.ID)
			}
		} else {
			logger.GetLogger().Errorf("[MAPPING] ❌ Failed to verify SNMP config after create: %v", err)
			return fmt.Errorf("failed to verify SNMP config after create: %w", err)
		}

		// TODO: Add support for digital_input, digital_output, analog_input, analog_output
		// For now, we'll skip these as they may not be used in the current flow
	}

	return nil
}

// NOTE: isRawParameter and isFormulaParameter functions are DEPRECATED
// They are replaced by explicit output_type validation in Save handler
// These functions are kept for backward compatibility but should not be used

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
