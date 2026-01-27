package service

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"aoi-backend/internal/core/cache"
	"aoi-backend/internal/core/logger"
	"aoi-backend/internal/domain"
	"aoi-backend/internal/repository"

	"github.com/Knetic/govaluate"
)

// FormulaDefinition represents a loaded formula with its variables
type FormulaDefinition struct {
	FormulaParameterID uint
	Expression         string
	Variables          []FormulaVar
	ParameterNama      string
	DeviceNama         string
}

// FormulaVar maps a variable name to its source parameter
type FormulaVar struct {
	Name              string
	SourceParameterID uint
}

// WorkflowCallback is called after formula evaluation to trigger workflow engine
type WorkflowCallback func(parameterID uint, data *domain.RealtimeData)

// FormulaEngine manages reactive formula evaluation
type FormulaEngine struct {
	formulaRepo       repository.FormulaRepository
	paramRepo         repository.ParameterRepository
	cache             cache.Cache
	wsBroadcaster     WebSocketBroadcaster
	evaluator         *FormulaEvaluator
	thresholdService  *ThresholdService
	workflowCallback  WorkflowCallback // Callback to notify workflow engine

	// In-memory registry
	mu              sync.RWMutex
	formulasByParam map[uint]*FormulaDefinition // formulaParamID -> formula definition
	dependants      map[uint][]uint             // sourceParamID -> list of formulaParamID that depend on it
	maxDepth        int                         // Maximum evaluation depth to prevent infinite loops
}

// NewFormulaEngine creates a new Formula Engine instance
func NewFormulaEngine(
	formulaRepo repository.FormulaRepository,
	paramRepo repository.ParameterRepository,
	cache cache.Cache,
	wsBroadcaster WebSocketBroadcaster,
	thresholdService *ThresholdService,
) *FormulaEngine {
	return &FormulaEngine{
		formulaRepo:      formulaRepo,
		paramRepo:        paramRepo,
		cache:            cache,
		wsBroadcaster:    wsBroadcaster,
		evaluator:        NewFormulaEvaluator(),
		thresholdService: thresholdService,
		workflowCallback: nil, // Set via SetWorkflowCallback
		formulasByParam:  make(map[uint]*FormulaDefinition),
		dependants:       make(map[uint][]uint),
		maxDepth:         10, // Maximum depth for dependency chain
	}
}

// SetWorkflowCallback sets the callback to notify workflow engine after formula evaluation
func (e *FormulaEngine) SetWorkflowCallback(callback WorkflowCallback) {
	e.workflowCallback = callback
}

// LoadAll loads all formulas from database and builds the registry
func (e *FormulaEngine) LoadAll() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Clear existing registry
	e.formulasByParam = make(map[uint]*FormulaDefinition)
	e.dependants = make(map[uint][]uint)

	// Load all formulas
	formulas, err := e.formulaRepo.GetAll()
	if err != nil {
		return fmt.Errorf("failed to load formulas: %w", err)
	}

	log := logger.GetLogger()
	log.Infof("[FORMULA] LoadAll: Found %d formulas in database", len(formulas))

	// Build registry
	loadedCount := 0
	skippedCount := 0
	for _, formula := range formulas {
		log.Debugf("[FORMULA] Loading formula: formula_id=%d, parameter_id=%d, expression='%s', variables_count=%d",
			formula.ID, formula.ParameterID, formula.Expression, len(formula.Variables))

		// Get parameter info
		param, err := e.paramRepo.GetByID(formula.ParameterID)
		if err != nil {
			log.Warnf("[FORMULA] Failed to load parameter %d for formula %d: %v", formula.ParameterID, formula.ID, err)
			skippedCount++
			continue
		}

		// Validate: parameter must have is_formula=true
		if !param.IsFormula {
			log.Warnf("[FORMULA] Skipping formula %d: parameter %d does not have is_formula=true", formula.ID, formula.ParameterID)
			skippedCount++
			continue
		}

		// Validate: formula must have variables
		if len(formula.Variables) == 0 {
			log.Warnf("[FORMULA] Skipping formula %d (parameter_id=%d): no variables defined", formula.ID, formula.ParameterID)
			skippedCount++
			continue
		}

		// Validate: expression must not be empty
		if strings.TrimSpace(formula.Expression) == "" {
			log.Warnf("[FORMULA] Skipping formula %d (parameter_id=%d): expression is empty", formula.ID, formula.ParameterID)
			skippedCount++
			continue
		}

		// Get device info
		var deviceNama string
		if param.DeviceID > 0 {
			// Note: We would need deviceRepo here, but for now we'll get it from parameter
			// This is a simplification - in production you might want to pass deviceRepo
			deviceNama = "" // Will be populated when needed
		}

		// Build variable list
		vars := make([]FormulaVar, 0, len(formula.Variables))
		for _, v := range formula.Variables {
			log.Debugf("[FORMULA]   Variable: name='%s', source_parameter_id=%d", v.VarName, v.SourceParameterID)
			vars = append(vars, FormulaVar{
				Name:              v.VarName,
				SourceParameterID: v.SourceParameterID,
			})

			// Build dependants map
			e.dependants[v.SourceParameterID] = append(e.dependants[v.SourceParameterID], formula.ParameterID)
		}

		// Store formula definition
		e.formulasByParam[formula.ParameterID] = &FormulaDefinition{
			FormulaParameterID: formula.ParameterID,
			Expression:         formula.Expression,
			Variables:          vars,
			ParameterNama:      param.Nama,
			DeviceNama:         deviceNama,
		}

		loadedCount++
		log.Infof("[FORMULA] Loaded formula: parameter_id=%d, parameter_nama='%s', expression='%s', variables=%d",
			formula.ParameterID, param.Nama, formula.Expression, len(vars))
	}

	log.Infof("[FORMULA] LoadAll complete: %d loaded, %d skipped (out of %d total)", loadedCount, skippedCount, len(formulas))
	log.Infof("[FORMULA] Registry contains %d formulas", len(e.formulasByParam))

	// Log all loaded formula parameter IDs
	if len(e.formulasByParam) > 0 {
		paramIDs := make([]uint, 0, len(e.formulasByParam))
		for paramID := range e.formulasByParam {
			paramIDs = append(paramIDs, paramID)
		}
		log.Infof("[FORMULA] Loaded formula parameter IDs: %v", paramIDs)
	}

	// Check for parameters with is_formula=true but no formula in registry
	allParams, err := e.paramRepo.GetAll()
	if err == nil {
		missingFormulas := make([]uint, 0)
		for _, param := range allParams {
			if param.IsFormula {
				if _, exists := e.formulasByParam[param.ID]; !exists {
					missingFormulas = append(missingFormulas, param.ID)
				}
			}
		}
		if len(missingFormulas) > 0 {
			log.Warnf("[FORMULA] Found %d parameters with is_formula=true but no formula in registry: %v",
				len(missingFormulas), missingFormulas)
			log.Warnf("[FORMULA] These parameters will NOT appear in dashboard. Please create parameter_formula entries for them.")
		}
	}

	return nil
}

// EvaluateAll evaluates all loaded formulas if their dependencies are available in cache
// This is useful after LoadAll() to populate dashboard with existing formula results
func (e *FormulaEngine) EvaluateAll() {
	e.mu.RLock()
	formulaParamIDs := make([]uint, 0, len(e.formulasByParam))
	for paramID := range e.formulasByParam {
		formulaParamIDs = append(formulaParamIDs, paramID)
	}
	e.mu.RUnlock()

	log := logger.GetLogger()
	log.Infof("[FORMULA] Evaluating %d formulas after LoadAll", len(formulaParamIDs))

	if len(formulaParamIDs) == 0 {
		log.Warnf("[FORMULA] No formulas loaded! Check if parameters have is_formula=true and parameter_formula entries")
		return
	}

	successCount := 0
	failedCount := 0
	for _, paramID := range formulaParamIDs {
		e.mu.RLock()
		def := e.formulasByParam[paramID]
		e.mu.RUnlock()

		if def == nil {
			log.Warnf("[FORMULA] Formula definition not found for parameter %d", paramID)
			failedCount++
			continue
		}

		log.Debugf("[FORMULA] Attempting to evaluate formula parameter %d: expression='%s', variables=%d",
			paramID, def.Expression, len(def.Variables))

		// Check if dependencies are available
		// CLEAN derive from SOURCE, FORMULA derive from CLEAN or SOURCE
		missingDeps := make([]uint, 0)
		for _, v := range def.Variables {
			var rt domain.RealtimeData
			cleanCacheKey := fmt.Sprintf("parameter:clean:%d", v.SourceParameterID)
			if err := e.cache.Get(cleanCacheKey, &rt); err != nil {
				// Fallback: try RAW cache (for SOURCE parameters)
				rawCacheKey := fmt.Sprintf("parameter:raw:%d", v.SourceParameterID)
				if err2 := e.cache.Get(rawCacheKey, &rt); err2 != nil {
					missingDeps = append(missingDeps, v.SourceParameterID)
				}
			}
		}

		if len(missingDeps) > 0 {
			log.Debugf("[FORMULA] Cannot evaluate formula parameter %d: missing dependencies %v (will retry after polling)",
				paramID, missingDeps)
			failedCount++
			continue
		}

		if err := e.evaluateFormula(paramID, 0); err != nil {
			log.Warnf("[FORMULA] Failed to evaluate formula parameter %d: %v", paramID, err)
			failedCount++
		} else {
			log.Infof("[FORMULA] Successfully evaluated formula parameter %d", paramID)
			successCount++
		}
	}

	log.Infof("[FORMULA] Evaluation complete: %d succeeded, %d failed (out of %d total)",
		successCount, failedCount, len(formulaParamIDs))

	if failedCount > 0 {
		log.Infof("[FORMULA] Failed formulas will be evaluated automatically when their dependencies are polled")
	}
}

// OnRawParameterUpdated is called by workers after a raw parameter is updated in cache
// This triggers reactive evaluation of all formulas that depend on this parameter
func (e *FormulaEngine) OnRawParameterUpdated(sourceParamID uint, data *domain.RealtimeData) {
	log := logger.GetLogger()
	log.Infof("[FORMULA] OnRawParameterUpdated: source_param_id=%d, value=%v", sourceParamID, data.ProcessedValue)

	e.mu.RLock()
	dependantFormulaParamIDs := e.dependants[sourceParamID]
	e.mu.RUnlock()

	if len(dependantFormulaParamIDs) == 0 {
		log.Debugf("[FORMULA] No formulas depend on parameter %d", sourceParamID)
		// No formulas depend on this parameter, but workflow might monitor it
		// Notify workflow engine about raw parameter update
		if e.workflowCallback != nil {
			e.workflowCallback(sourceParamID, data)
		}
		return
	}

	log.Infof("[FORMULA] Found %d formulas depending on parameter %d: %v", len(dependantFormulaParamIDs), sourceParamID, dependantFormulaParamIDs)
	// Evaluate all dependent formulas
	successCount := 0
	for _, formulaParamID := range dependantFormulaParamIDs {
		log.Debugf("[FORMULA] Evaluating formula parameter %d triggered by source %d", formulaParamID, sourceParamID)
		if err := e.evaluateFormula(formulaParamID, 0); err != nil {
			log.Warnf("[FORMULA] Failed to evaluate formula parameter %d: %v", formulaParamID, err)
		} else {
			successCount++
			log.Infof("[FORMULA] Successfully evaluated formula parameter %d (triggered by source %d)", formulaParamID, sourceParamID)
		}
	}
	log.Infof("[FORMULA] Evaluated %d/%d formulas successfully after raw parameter %d update", successCount, len(dependantFormulaParamIDs), sourceParamID)
}

// IsParameterUsedByFormula checks if a parameter is used as source by any formula
func (e *FormulaEngine) IsParameterUsedByFormula(paramID uint) bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	dependants := e.dependants[paramID]
	return len(dependants) > 0
}

// evaluateFormula evaluates a formula and updates cache + broadcasts WebSocket
// depth is used to prevent infinite loops in dependency chains
func (e *FormulaEngine) evaluateFormula(formulaParamID uint, depth int) error {
	if depth > e.maxDepth {
		return fmt.Errorf("max formula depth exceeded for parameter %d", formulaParamID)
	}

	e.mu.RLock()
	def := e.formulasByParam[formulaParamID]
	e.mu.RUnlock()

	if def == nil {
		return fmt.Errorf("formula not found for parameter %d", formulaParamID)
	}

	// Prepare variable values from cache
	// CLEAN derive from SOURCE (parameter:raw)
	// FORMULA derive from CLEAN (parameter:clean) or SOURCE (parameter:raw)
	values := make(map[string]interface{})
	for _, v := range def.Variables {
		var rt domain.RealtimeData
		var err error

		// Try CLEAN cache first (for CLEAN parameters or DIRECT parameters)
		cleanCacheKey := fmt.Sprintf("parameter:clean:%d", v.SourceParameterID)
		if err = e.cache.Get(cleanCacheKey, &rt); err != nil {
			// Fallback: try RAW cache (for SOURCE parameters)
			rawCacheKey := fmt.Sprintf("parameter:raw:%d", v.SourceParameterID)
			if err2 := e.cache.Get(rawCacheKey, &rt); err2 != nil {
				return fmt.Errorf("missing source parameter %d in cache (tried clean and raw): %w", v.SourceParameterID, err)
			}
		}

		// Use ProcessedValue if available, otherwise RawValue
		val := rt.ProcessedValue
		if val == nil {
			val = rt.RawValue
		}

		// Convert to float64
		f, err := toFloat64(val)
		if err != nil {
			return fmt.Errorf("failed to convert value for variable %s (param %d): %w", v.Name, v.SourceParameterID, err)
		}

		values[v.Name] = f
	}

	// Log expression and values for debugging
	logger.GetLogger().Debugf("Formula Engine: Evaluating formula for parameter %d", formulaParamID)
	logger.GetLogger().Debugf("Formula Engine: Expression: '%s' (length: %d)", def.Expression, len(def.Expression))
	logger.GetLogger().Debugf("Formula Engine: Variables: %+v", values)

	// Trim whitespace from expression
	expressionStr := strings.TrimSpace(def.Expression)
	if expressionStr == "" {
		return fmt.Errorf("expression is empty for parameter %d", formulaParamID)
	}

	// Evaluate expression using govaluate
	expression, err := govaluate.NewEvaluableExpression(expressionStr)
	if err != nil {
		logger.GetLogger().Errorf("Formula Engine: Failed to parse expression '%s': %v", expressionStr, err)
		return fmt.Errorf("invalid expression '%s': %w", expressionStr, err)
	}

	// Log which variables are required by the expression
	parameters := expression.Vars()
	logger.GetLogger().Debugf("Formula Engine: Expression requires variables: %v", parameters)

	// Check if all required variables are provided
	for _, param := range parameters {
		if _, exists := values[param]; !exists {
			logger.GetLogger().Warnf("Formula Engine: Variable '%s' required by expression but not provided. Available variables: %v", param, values)
		}
	}

	result, err := expression.Evaluate(values)
	if err != nil {
		logger.GetLogger().Errorf("Formula Engine: Failed to evaluate expression '%s' with values %+v: %v", expressionStr, values, err)
		return fmt.Errorf("failed to evaluate expression: %w", err)
	}

	logger.GetLogger().Debugf("Formula Engine: Evaluation result: %v (type: %T)", result, result)

	// Convert result to float64
	resultFloat, err := toFloat64(result)
	if err != nil {
		return fmt.Errorf("formula result is not a number: %v", result)
	}

	// Round to 3 decimal places
	resultFloat = math.Round(resultFloat*1000) / 1000

	// Get parameter info with device relation preloaded
	param, err := e.paramRepo.GetByID(formulaParamID)
	if err != nil {
		return fmt.Errorf("failed to get parameter: %w", err)
	}

	// Get device name from parameter relation
	// Try to load device if not already loaded
	deviceNama := ""
	if param.DeviceID > 0 {
		if param.Device.Nama != "" {
			deviceNama = param.Device.Nama
		} else {
			// If device relation not loaded, try to get from cache or use parameter name prefix
			// For now, use a fallback - in production you might want to query deviceRepo
			deviceNama = "Device" // Fallback - will be improved later
		}
	}

	// Create RealtimeData for formula result
	// Store unit in CustomRumus field temporarily (will be extracted in WebSocketHub)
	// TODO: Add Unit field to RealtimeData struct
	formulaData := domain.RealtimeData{
		ParameterID:    formulaParamID,
		ParameterNama:  param.Nama,
		ParameterType:  "formula",
		DeviceNama:     deviceNama,
		RawValue:       nil, // Formulas don't have raw values
		ProcessedValue: resultFloat,
		Timestamp:      time.Now(),
		Status:         "online",
		Error:          nil,
		CustomRumus:    param.Unit, // Temporary: use CustomRumus to pass unit to WebSocketHub
		AlarmStatus:    domain.AlarmStatusNormal, // Default to normal
		Direction:      param.Direction,          // Include parameter direction
	}

	// Apply threshold to calculate alarm status
	if e.thresholdService != nil {
		e.thresholdService.ApplyThreshold(&formulaData)
	}

	// Save CLEAN data to public cache (for FORMULA results)
	// FORMULA results are CLEAN data (no raw value)
	cleanCacheKey := fmt.Sprintf("parameter:clean:%d", formulaParamID)
	if err := e.cache.Set(cleanCacheKey, formulaData, 24*time.Hour); err != nil {
		logger.GetLogger().Warnf("Failed to cache formula clean data: %v", err)
	} else {
		logger.GetLogger().Infof("[CLEAN EMIT] parameter_id=%d, parameter_nama=%s, expression=%s, clean_value=%v",
			formulaParamID, param.Nama, def.Expression, resultFloat)
	}

	// Broadcast CLEAN DATA via WebSocket
	// FORMULA parameters are broadcast (they are CLEAN data)
	if e.wsBroadcaster != nil {
		logger.GetLogger().Infof("[BROADCAST] Broadcasting FORMULA clean data: parameter_id=%d, parameter_nama=%s, expression=%s, result=%v, device_nama=%s",
			formulaParamID, param.Nama, def.Expression, resultFloat, deviceNama)
		e.wsBroadcaster.BroadcastDataUpdate(formulaData)
	} else {
		logger.GetLogger().Warnf("[FORMULA] WebSocket broadcaster is nil, cannot broadcast: parameter_id=%d", formulaParamID)
	}

	// Recursively evaluate formulas that depend on this formula
	e.mu.RLock()
	nextDependants := e.dependants[formulaParamID]
	e.mu.RUnlock()

	for _, nextFormulaParamID := range nextDependants {
		if err := e.evaluateFormula(nextFormulaParamID, depth+1); err != nil {
			logger.GetLogger().Warnf("Failed to evaluate dependent formula %d: %v", nextFormulaParamID, err)
		}
	}

	// NOTIFY Workflow Engine (after formula evaluation)
	// Workflow Engine will evaluate workflows that monitor this parameter
	// This is called via callback to avoid circular dependency
	if e.workflowCallback != nil {
		e.workflowCallback(formulaParamID, &formulaData)
	}

	return nil
}

// EvaluateFormula is a public method to evaluate a formula parameter
// This is useful for triggering evaluation after formula creation or update
func (e *FormulaEngine) EvaluateFormula(formulaParamID uint) error {
	return e.evaluateFormula(formulaParamID, 0)
}

// toFloat64 converts various numeric types to float64
func toFloat64(v interface{}) (float64, error) {
	switch val := v.(type) {
	case float64:
		return val, nil
	case float32:
		return float64(val), nil
	case int:
		return float64(val), nil
	case int32:
		return float64(val), nil
	case int64:
		return float64(val), nil
	case uint:
		return float64(val), nil
	case uint32:
		return float64(val), nil
	case uint64:
		return float64(val), nil
	case string:
		f, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return 0, fmt.Errorf("cannot parse string as float: %s", val)
		}
		return f, nil
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", v)
	}
}

// ParseFieldRef parses a field reference like "Slave 1 Addr 7" and returns the corresponding parameter ID
// This is used when saving formulas from frontend
func ParseFieldRef(fieldRef string) (slaveID int, addr int, err error) {
	// Pattern: "Slave X Addr Y" or "Slave X, Address Y" or variations
	re := regexp.MustCompile(`(?i)slave\s+(\d+).*?(?:addr|address)\s+(\d+)`)
	matches := re.FindStringSubmatch(fieldRef)
	if len(matches) != 3 {
		return 0, 0, fmt.Errorf("invalid field reference format: %s", fieldRef)
	}

	slaveID, err = strconv.Atoi(matches[1])
	if err != nil {
		return 0, 0, fmt.Errorf("invalid slave ID: %s", matches[1])
	}

	addr, err = strconv.Atoi(matches[2])
	if err != nil {
		return 0, 0, fmt.Errorf("invalid address: %s", matches[2])
	}

	return slaveID, addr, nil
}

// NormalizeExpression normalizes a formula expression by replacing field references with variable names
// Example: "(Slave 1 Addr 7 + Slave 1 Addr 8) / 10" -> "(a + b) / 10"
// Returns normalized expression and variable mappings
func NormalizeExpression(expression string, fieldToVarMap map[string]string) (string, error) {
	normalized := expression
	for fieldRef, varName := range fieldToVarMap {
		// Escape special regex characters in field reference
		escaped := regexp.QuoteMeta(fieldRef)
		re := regexp.MustCompile(escaped)
		normalized = re.ReplaceAllString(normalized, varName)
	}
	return normalized, nil
}

// ResolveFieldToParameterID resolves a field reference (e.g., "Slave 1 Addr 7") to a parameter ID
// This searches the database for a parameter matching the slave ID and address
// Note: This is a placeholder - actual implementation should query modbus tables via repository
func (e *FormulaEngine) ResolveFieldToParameterID(deviceID uint, fieldRef string) (uint, error) {
	_, _, err := ParseFieldRef(fieldRef)
	if err != nil {
		return 0, err
	}

	// Search for parameter with matching device_id, slave_id, and register_address
	// This requires querying modbus_rtu or modbus_tcp tables
	// For now, we'll need to add this to the repository or create a helper function
	// This is a placeholder - actual implementation depends on your repository structure
	return 0, fmt.Errorf("field resolution not yet implemented - need to query modbus tables")
}
