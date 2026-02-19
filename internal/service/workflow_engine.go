package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"aoi-backend/internal/core/cache"
	"aoi-backend/internal/core/logger"
	"aoi-backend/internal/domain"
	"aoi-backend/internal/repository"
)

// WorkflowEngine manages workflow evaluation and execution
// Workflow hanya bekerja pada parameter_id, tidak tahu register/slave/address
// Workflow dievaluasi SETELAH data READ bersih (setelah formula engine)
type WorkflowEngine struct {
	workflowRepo             repository.WorkflowRepository
	paramRepo                repository.ParameterRepository
	alarmRepo                repository.AlarmRepository
	setPointRepo             repository.SetPointRepository // For getting setpoint values
	cache                    cache.Cache
	writeService             *WriteService
	internalParameterService *InternalParameterService // For managing internal parameters
	wsBroadcaster            WebSocketBroadcaster      // For broadcasting alarm notifications
	// eventBus     EventBus // TODO: Implement event bus for event actions

	// In-memory registry: parameter_id -> workflows that monitor it
	mu               sync.RWMutex
	workflowsByParam map[uint][]*domain.Workflow // parameterID -> list of workflows
	debounceMap      map[uint]time.Time          // workflowID -> last execution time (for debounce)
	debounceWindow   time.Duration               // Minimum time between executions
}

// NewWorkflowEngine creates a new WorkflowEngine instance
func NewWorkflowEngine(
	workflowRepo repository.WorkflowRepository,
	paramRepo repository.ParameterRepository,
	alarmRepo repository.AlarmRepository,
	setPointRepo repository.SetPointRepository,
	cache cache.Cache,
	writeService *WriteService,
	internalParameterService *InternalParameterService,
	wsBroadcaster WebSocketBroadcaster,
) *WorkflowEngine {
	return &WorkflowEngine{
		workflowRepo:             workflowRepo,
		paramRepo:                paramRepo,
		alarmRepo:                alarmRepo,
		setPointRepo:             setPointRepo,
		cache:                    cache,
		writeService:             writeService,
		internalParameterService: internalParameterService,
		wsBroadcaster:            wsBroadcaster,
		workflowsByParam:         make(map[uint][]*domain.Workflow),
		debounceMap:              make(map[uint]time.Time),
		debounceWindow:           5 * time.Second, // Default: 5 seconds debounce
	}
}

// LoadAll loads all enabled workflows from database and builds the registry
func (e *WorkflowEngine) LoadAll() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Clear existing registry
	e.workflowsByParam = make(map[uint][]*domain.Workflow)
	e.debounceMap = make(map[uint]time.Time)

	// Load all enabled workflows
	workflows, err := e.workflowRepo.GetAll()
	if err != nil {
		return fmt.Errorf("failed to load workflows: %w", err)
	}

	// Build registry: parameter_id -> workflows that monitor it
	for i := range workflows {
		workflow := &workflows[i]
		if !workflow.Enabled {
			continue
		}

		// Version 1: Flat structure - register for condition parameters
		if workflow.Version == domain.WorkflowVersionFlat || workflow.Version == 0 {
			for _, condition := range workflow.Conditions {
				paramID := condition.ParameterID
				e.workflowsByParam[paramID] = append(e.workflowsByParam[paramID], workflow)
			}
		}

		// Version 2: Tree structure - register for trigger source
		if workflow.Version == domain.WorkflowVersionTree && workflow.TreeData != nil {
			var tree domain.WorkflowTree
			if err := json.Unmarshal([]byte(*workflow.TreeData), &tree); err == nil {
				if tree.Trigger != nil && tree.Trigger.Type == domain.TriggerTypeParameterChanged {
					e.workflowsByParam[tree.Trigger.SourceID] = append(e.workflowsByParam[tree.Trigger.SourceID], workflow)
				}
			}
		}
	}

	return nil
}

// OnParameterUpdated is called AFTER data READ bersih (after formula engine)
// This evaluates workflows that monitor the updated parameter
func (e *WorkflowEngine) OnParameterUpdated(parameterID uint, data *domain.RealtimeData) {
	e.mu.RLock()
	workflows := e.workflowsByParam[parameterID]
	e.mu.RUnlock()

	if len(workflows) == 0 {
		return // No workflows monitor this parameter
	}

	// Evaluate each workflow
	for _, workflow := range workflows {
		// Route to appropriate execution based on version
		if workflow.Version == domain.WorkflowVersionTree {
			// Version 2: Tree-based execution
			if err := e.evaluateTreeWorkflow(workflow, parameterID, data); err != nil {
				// Silent fail
			}
		} else {
			// Version 1: Flat execution (backward compatibility)
			if err := e.evaluateWorkflow(workflow, parameterID, data); err != nil {
				// Silent fail
			}
		}
	}
}

// evaluateWorkflow evaluates a workflow's conditions and executes actions if conditions are met
// Implements edge trigger: only execute when condition changes from false to true
func (e *WorkflowEngine) evaluateWorkflow(workflow *domain.Workflow, updatedParamID uint, data *domain.RealtimeData) error {
	// Get workflow state for edge trigger
	state, err := e.workflowRepo.GetStateByWorkflowID(workflow.ID)
	if err != nil {
		state = nil
	}

	// Check debounce: don't execute if within debounce window
	e.mu.RLock()
	lastExec, exists := e.debounceMap[workflow.ID]
	e.mu.RUnlock()
	if exists && time.Since(lastExec) < e.debounceWindow {
		return nil
	}

	// Evaluate all conditions
	// All conditions must be true for workflow to trigger
	allConditionsMet := true
	var conditionValue float64

	for _, condition := range workflow.Conditions {
		// Get parameter value from cache
		// Workflow hanya bekerja pada CLEAN DATA (DIRECT, CLEAN, FORMULA)
		// Hanya baca dari clean cache, JANGAN baca dari raw cache
		var paramData *domain.RealtimeData
		cleanCacheKey := fmt.Sprintf("parameter:clean:%d", condition.ParameterID)

		if err := e.cache.Get(cleanCacheKey, &paramData); err != nil {
			// Parameter clean data tidak ditemukan
			allConditionsMet = false
			break
		}

		// Get processed value (clean data)
		var value float64
		if paramData.ProcessedValue != nil {
			if v, ok := paramData.ProcessedValue.(float64); ok {
				value = v
			} else if v, ok := paramData.ProcessedValue.(int); ok {
				value = float64(v)
			} else if v, ok := paramData.ProcessedValue.(int32); ok {
				value = float64(v)
			} else if v, ok := paramData.ProcessedValue.(int64); ok {
				value = float64(v)
			} else {
				allConditionsMet = false
				break
			}
		} else {
			allConditionsMet = false
			break
		}

		// Store value for the condition that triggered this evaluation
		if condition.ParameterID == updatedParamID {
			conditionValue = value
		}

		// Evaluate condition
		conditionMet := e.evaluateCondition(value, condition.Operator, condition.Threshold)
		if !conditionMet {
			allConditionsMet = false
			break
		}
	}

	// Edge trigger: only execute if condition changes from false to true
	shouldExecute := false
	if allConditionsMet {
		// Condition is met now
		if state == nil || state.LastConditionResult == nil || !*state.LastConditionResult {
			// Previous state was false or doesn't exist, now it's true -> edge trigger
			shouldExecute = true
		}
	}

	// Update state
	newState := &domain.WorkflowState{
		WorkflowID:         workflow.ID,
		LastConditionValue: &conditionValue,
	}
	if allConditionsMet {
		now := time.Now()
		newState.LastTriggeredAt = &now
		trueVal := true
		newState.LastConditionResult = &trueVal
	} else {
		falseVal := false
		newState.LastConditionResult = &falseVal
	}

	if err := e.workflowRepo.CreateOrUpdateState(newState); err != nil {
		// Silent fail
	}

	// Handle alarm cleanup: if condition is no longer met, remove alarm from current_alarm
	if !allConditionsMet {
		// Condition is no longer met, check if alarm exists and remove it
		existingAlarm, err := e.alarmRepo.GetByWorkflowID(workflow.ID)
		if err == nil && existingAlarm != nil {
			// Alarm exists but condition is no longer met, delete it
			if err := e.alarmRepo.DeleteByWorkflowID(workflow.ID); err == nil {
				// Broadcast alarm removal via WebSocket
				if e.wsBroadcaster != nil {
					e.wsBroadcaster.BroadcastAlarmRemoved(workflow.ID)
				}
			}
		}
	}

	// Execute actions if condition is met and edge triggered
	if shouldExecute {
		// Update debounce map
		e.mu.Lock()
		e.debounceMap[workflow.ID] = time.Now()
		e.mu.Unlock()

		// Sort actions by action_order to ensure sequential execution
		// Actions are executed in order: 0, 1, 2, 3, ...
		sortedActions := make([]domain.WorkflowAction, len(workflow.Actions))
		copy(sortedActions, workflow.Actions)

		// Sort by action_order (ascending)
		for i := 0; i < len(sortedActions)-1; i++ {
			for j := i + 1; j < len(sortedActions); j++ {
				if sortedActions[i].ActionOrder > sortedActions[j].ActionOrder {
					sortedActions[i], sortedActions[j] = sortedActions[j], sortedActions[i]
				}
			}
		}

		// Execute actions sequentially (one after another, waiting for each to complete)
		for _, action := range sortedActions {
			if err := e.executeAction(workflow.ID, workflow, action); err != nil {
				// Continue with next action even if current one fails
			}
		}
	}

	return nil
}

// evaluateCondition evaluates a single condition
func (e *WorkflowEngine) evaluateCondition(value float64, operator string, threshold float64) bool {
	switch operator {
	case ">":
		return value > threshold
	case "<":
		return value < threshold
	case ">=":
		return value >= threshold
	case "<=":
		return value <= threshold
	case "==", "=":
		return value == threshold
	case "!=", "<>":
		return value != threshold
	default:
		return false
	}
}

// executeAction executes a workflow action
func (e *WorkflowEngine) executeAction(workflowID uint, workflow *domain.Workflow, action domain.WorkflowAction) error {
	switch action.ActionType {
	case "TRIGGER_WRITE":
		// TRIGGER_WRITE: trigger PRESET parameter (no value sent)
		if action.ParameterID == nil {
			return fmt.Errorf("parameter_id is required for TRIGGER_WRITE action")
		}

		// Validate parameter is WRITE type
		param, err := e.paramRepo.GetByID(*action.ParameterID)
		if err != nil {
			return fmt.Errorf("failed to get parameter %d: %w", *action.ParameterID, err)
		}

		if param.Direction != "WRITE" {
			return fmt.Errorf("parameter %d is not a WRITE parameter", *action.ParameterID)
		}

		// Validate write_mode is PRESET
		if param.WriteMode == nil || *param.WriteMode != "PRESET" {
			return fmt.Errorf("parameter %d is not a PRESET parameter (write_mode must be PRESET for TRIGGER_WRITE)", *action.ParameterID)
		}

		// Validate preset_value exists
		if param.PresetValue == nil || *param.PresetValue == "" {
			return fmt.Errorf("parameter %d is PRESET but has no preset_value", *action.ParameterID)
		}

		// GUARD: Prevent value from being sent to PRESET parameter
		if action.Value != nil {
			return fmt.Errorf("TRIGGER_WRITE action cannot send value to PRESET parameter %d", *action.ParameterID)
		}

		// Execute trigger via WriteService (TRIGGER mode uses preset_value from parameter)
		ctx := context.Background()
		workflowNama := workflow.Nama
		writeReq := WriteRequest{
			Mode:         "TRIGGER", // New mode for triggering PRESET parameters
			WorkflowID:   &workflowID,
			WorkflowNama: &workflowNama,
		}

		result, err := e.writeService.Write(ctx, *action.ParameterID, writeReq)
		if err != nil {
			return fmt.Errorf("write service error: %w", err)
		}

		if !result.Success {
			return fmt.Errorf("write failed: %s", result.Error)
		}

	case "WRITE_WITH_VALUE":
		// WRITE_WITH_VALUE: write DYNAMIC parameter or READ_WRITE parameter (value required)
		if action.ParameterID == nil {
			return fmt.Errorf("parameter_id is required for WRITE_WITH_VALUE action")
		}
		if action.Value == nil {
			return fmt.Errorf("value is required for WRITE_WITH_VALUE action")
		}

		// Validate parameter is WRITE or READ_WRITE type
		param, err := e.paramRepo.GetByID(*action.ParameterID)
		if err != nil {
			return fmt.Errorf("failed to get parameter %d: %w", *action.ParameterID, err)
		}

		// Allow WRITE and READ_WRITE parameters (READ_WRITE is treated as write dynamic)
		if param.Direction != "WRITE" && param.Direction != "READ_WRITE" {
			return fmt.Errorf("parameter %d is not a WRITE or READ_WRITE parameter", *action.ParameterID)
		}

		// For WRITE parameters, validate write_mode is DYNAMIC
		// For READ_WRITE parameters, treat as DYNAMIC (no write_mode validation needed)
		if param.Direction == "WRITE" {
			if param.WriteMode == nil || *param.WriteMode != "DYNAMIC" {
				return fmt.Errorf("parameter %d is not a DYNAMIC parameter (write_mode must be DYNAMIC for WRITE_WITH_VALUE)", *action.ParameterID)
			}
		}

		// Execute write via WriteService
		ctx := context.Background()
		workflowNama := workflow.Nama
		writeReq := WriteRequest{
			Mode:         "MANUAL",
			Value:        action.Value,
			WorkflowID:   &workflowID,
			WorkflowNama: &workflowNama,
		}

		result, err := e.writeService.Write(ctx, *action.ParameterID, writeReq)
		if err != nil {
			return fmt.Errorf("write service error: %w", err)
		}

		if !result.Success {
			// For READ_WRITE parameters, read-back failure is critical
			if param.Direction == "READ_WRITE" || param.ReadWrite == 2 {
				return fmt.Errorf("write failed (read-back verification failed for READ_WRITE parameter): %s", result.Error)
			}
			return fmt.Errorf("write failed: %s", result.Error)
		}

	case "write":
		// Legacy support: treat as WRITE_WITH_VALUE
		if action.ParameterID == nil {
			return fmt.Errorf("parameter_id is required for write action")
		}
		if action.Value == nil {
			return fmt.Errorf("value is required for write action")
		}

		param, err := e.paramRepo.GetByID(*action.ParameterID)
		if err != nil {
			return fmt.Errorf("failed to get parameter %d: %w", *action.ParameterID, err)
		}

		// Allow WRITE and READ_WRITE parameters (READ_WRITE is treated as write dynamic)
		if param.Direction != "WRITE" && param.Direction != "READ_WRITE" {
			return fmt.Errorf("parameter %d is not a WRITE or READ_WRITE parameter", *action.ParameterID)
		}

		// Check write_mode and validate accordingly (only for WRITE parameters)
		if param.Direction == "WRITE" && param.WriteMode != nil && *param.WriteMode == "PRESET" {
			return fmt.Errorf("cannot send value to PRESET parameter %d, use TRIGGER_WRITE action instead", *action.ParameterID)
		}

		ctx := context.Background()
		writeReq := WriteRequest{
			Mode:  "MANUAL",
			Value: action.Value,
		}

		result, err := e.writeService.Write(ctx, *action.ParameterID, writeReq)
		if err != nil {
			return fmt.Errorf("write service error: %w", err)
		}

		if !result.Success {
			// For READ_WRITE parameters, read-back failure is critical
			if param.Direction == "READ_WRITE" || param.ReadWrite == 2 {
				return fmt.Errorf("write failed (read-back verification failed for READ_WRITE parameter): %s", result.Error)
			}
			return fmt.Errorf("write failed: %s", result.Error)
		}

	case "event":
		// TODO: Implement event bus
		if action.EventName == nil {
			return fmt.Errorf("event_name is required for event action")
		}
		// e.eventBus.Publish(*action.EventName, workflowID)

	case "delay":
		if action.DelayMs == nil {
			return fmt.Errorf("delay_ms is required for delay action")
		}
		time.Sleep(time.Duration(*action.DelayMs) * time.Millisecond)

	case "alarm":
		// ALARM: Save to current_alarm table and broadcast notification
		if action.AlarmMessage == nil || *action.AlarmMessage == "" {
			return fmt.Errorf("alarm_message is required for alarm action")
		}

		workflowName := "Unknown Workflow"
		if workflow != nil {
			workflowName = workflow.Nama
		}

		// Get parameter info for alarm (use first condition's parameter)
		var parameterID uint
		var parameterName *string
		if len(workflow.Conditions) > 0 {
			parameterID = workflow.Conditions[0].ParameterID
			param, err := e.paramRepo.GetByID(parameterID)
			if err == nil && param != nil {
				parameterName = &param.Nama
			}
		}

		// Create or update alarm in current_alarm table
		alarm := &domain.CurrentAlarm{
			WorkflowID:    workflowID,
			WorkflowName:  workflowName,
			ParameterID:   parameterID,
			ParameterName: parameterName,
			AlarmMessage:  *action.AlarmMessage,
			TriggeredAt:   time.Now(),
		}

		if err := e.alarmRepo.CreateOrUpdate(alarm); err != nil {
			return fmt.Errorf("failed to save alarm: %w", err)
		}

		// Broadcast alarm notification via WebSocket
		if e.wsBroadcaster != nil {
			e.wsBroadcaster.BroadcastAlarm(workflowID, workflowName, *action.AlarmMessage)
		}

		// Send alarm to external HTTP endpoint
		go e.sendAlarmToHTTPEndpoint(*action.AlarmMessage, workflowID, workflowName)

	case "WRITE_INTERNAL":
		// WRITE_INTERNAL: write to internal parameter (for workflow chaining)
		if action.ParameterID == nil {
			return fmt.Errorf("parameter_id is required for WRITE_INTERNAL action")
		}
		if action.Value == nil {
			return fmt.Errorf("value is required for WRITE_INTERNAL action")
		}

		// Validate parameter is INTERNAL type
		param, err := e.paramRepo.GetByID(*action.ParameterID)
		if err != nil {
			return fmt.Errorf("failed to get parameter %d: %w", *action.ParameterID, err)
		}

		if param.Type != "internal" {
			return fmt.Errorf("parameter %d is not an internal parameter", *action.ParameterID)
		}

		// Write to internal parameter via InternalParameterService
		if e.internalParameterService == nil {
			return fmt.Errorf("internal parameter service is not configured")
		}

		ctx := context.Background()
		if err := e.internalParameterService.SetValue(ctx, *action.ParameterID, *action.Value); err != nil {
			return fmt.Errorf("failed to write internal parameter %d: %w", *action.ParameterID, err)
		}

	default:
		return fmt.Errorf("unknown action type: %s", action.ActionType)
	}

	return nil
}

// sendAlarmToHTTPEndpoint sends alarm message to external HTTP endpoint
// This runs in a goroutine to avoid blocking workflow execution
func (e *WorkflowEngine) sendAlarmToHTTPEndpoint(message string, workflowID uint, workflowName string) {
	log := logger.GetLogger()

	// HTTP endpoint URL (can be moved to config later)
	endpointURL := "http://localhost:8085/send"

	// Prepare request payload
	payload := map[string]string{
		"message": message,
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		log.Errorf("WorkflowEngine: Failed to marshal alarm payload: %v", err)
		return
	}

	// Create HTTP request with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST", endpointURL, bytes.NewBuffer(jsonData))
	if err != nil {
		log.Errorf("WorkflowEngine: Failed to create HTTP request for alarm: %v", err)
		return
	}

	req.Header.Set("Content-Type", "application/json")

	// Send request
	client := &http.Client{
		Timeout: 5 * time.Second,
	}

	resp, err := client.Do(req)
	if err != nil {
		log.Errorf("WorkflowEngine: Failed to send alarm to HTTP endpoint %s: %v", endpointURL, err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		log.Infof("WorkflowEngine: Alarm sent to HTTP endpoint successfully: workflow=%d (%s), status=%d", workflowID, workflowName, resp.StatusCode)
	} else {
		log.Warnf("WorkflowEngine: Alarm sent to HTTP endpoint but received non-2xx status: workflow=%d (%s), status=%d", workflowID, workflowName, resp.StatusCode)
	}
}

// evaluateTreeWorkflow evaluates a version 2 tree-based workflow
func (e *WorkflowEngine) evaluateTreeWorkflow(workflow *domain.Workflow, updatedParamID uint, data *domain.RealtimeData) error {
	if workflow.TreeData == nil {
		return fmt.Errorf("workflow %d has no tree_data", workflow.ID)
	}

	// Parse tree JSON
	var tree domain.WorkflowTree
	if err := json.Unmarshal([]byte(*workflow.TreeData), &tree); err != nil {
		return fmt.Errorf("failed to parse tree_data for workflow %d: %w", workflow.ID, err)
	}

	// Check if trigger matches
	if tree.Trigger == nil {
		return fmt.Errorf("workflow %d has no trigger", workflow.ID)
	}

	if tree.Trigger.Type == domain.TriggerTypeParameterChanged && tree.Trigger.SourceID != updatedParamID {
		return nil // Not triggered by this parameter
	}

	// Check debounce
	e.mu.RLock()
	lastExec, exists := e.debounceMap[workflow.ID]
	e.mu.RUnlock()
	if exists && time.Since(lastExec) < e.debounceWindow {
		return nil
	}

	// Get workflow state for edge trigger
	state, err := e.workflowRepo.GetStateByWorkflowID(workflow.ID)
	if err != nil {
		state = nil
	}

	// Evaluate root node (recursive)
	shouldExecute, conditionMet := e.evaluateTreeNode(tree.Root, state, 0)

	// Update state
	newState := &domain.WorkflowState{
		WorkflowID: workflow.ID,
	}
	if conditionMet {
		now := time.Now()
		newState.LastTriggeredAt = &now
		trueVal := true
		newState.LastConditionResult = &trueVal
	} else {
		falseVal := false
		newState.LastConditionResult = &falseVal
	}
	e.workflowRepo.CreateOrUpdateState(newState)

	// Execute if should execute
	if shouldExecute {
		e.mu.Lock()
		e.debounceMap[workflow.ID] = time.Now()
		e.mu.Unlock()

		// Execute root node recursively
		if err := e.executeTreeNode(workflow.ID, workflow, tree.Root, 0); err != nil {
			return fmt.Errorf("failed to execute tree workflow %d: %w", workflow.ID, err)
		}
	}

	return nil
}

// evaluateTreeNode evaluates a tree node recursively (for edge trigger detection)
// Returns: (shouldExecute, conditionMet)
func (e *WorkflowEngine) evaluateTreeNode(node *domain.TreeNode, state *domain.WorkflowState, depth int) (bool, bool) {
	if depth > 10 { // Max depth safety
		return false, false
	}

	if node == nil {
		return false, false
	}

	if node.Type == "action" {
		// Actions don't have conditions, so they don't affect edge trigger
		return false, false
	}

	if node.Type == "if" {
		if node.Condition == nil {
			return false, false
		}

		// Evaluate condition
		conditionMet := e.evaluateTreeCondition(node.Condition)

		// Check edge trigger: only execute if condition changes from false to true
		shouldExecute := false
		if conditionMet {
			if state == nil || state.LastConditionResult == nil || !*state.LastConditionResult {
				shouldExecute = true
			}
		}

		// Recursively evaluate children
		if conditionMet {
			// Evaluate THEN branch
			for _, child := range node.Then {
				childShouldExecute, childConditionMet := e.evaluateTreeNode(&child, state, depth+1)
				if childShouldExecute {
					shouldExecute = true
				}
				if childConditionMet {
					conditionMet = true
				}
			}
		} else {
			// Evaluate ELSE branch
			for _, child := range node.Else {
				childShouldExecute, childConditionMet := e.evaluateTreeNode(&child, state, depth+1)
				if childShouldExecute {
					shouldExecute = true
				}
				if childConditionMet {
					conditionMet = true
				}
			}
		}

		return shouldExecute, conditionMet
	}

	return false, false
}

// evaluateTreeCondition evaluates a condition in tree format
func (e *WorkflowEngine) evaluateTreeCondition(condition *domain.Condition) bool {
	if condition == nil {
		return false
	}

	// Get left value
	leftValue, err := e.getValueFromSource(condition.Left)
	if err != nil {
		return false
	}

	// Get right value
	rightValue, err := e.getValueFromSource(condition.Right)
	if err != nil {
		return false
	}

	// Evaluate condition
	return e.evaluateCondition(leftValue, condition.Operator, rightValue)
}

// getValueFromSource gets a value from a ValueSource (parameter, setpoint, or static)
func (e *WorkflowEngine) getValueFromSource(source *domain.ValueSource) (float64, error) {
	if source == nil {
		return 0, fmt.Errorf("value source is nil")
	}

	switch source.Type {
	case "static":
		if source.Value == nil {
			return 0, fmt.Errorf("static value is nil")
		}
		return *source.Value, nil

	case "parameter":
		if source.SourceID == nil {
			return 0, fmt.Errorf("parameter source_id is nil")
		}
		cleanCacheKey := fmt.Sprintf("parameter:clean:%d", *source.SourceID)
		var paramData *domain.RealtimeData
		if err := e.cache.Get(cleanCacheKey, &paramData); err != nil {
			return 0, fmt.Errorf("parameter %d not found in cache: %w", *source.SourceID, err)
		}
		if paramData.ProcessedValue == nil {
			return 0, fmt.Errorf("parameter %d has no value", *source.SourceID)
		}
		if v, ok := paramData.ProcessedValue.(float64); ok {
			return v, nil
		} else if v, ok := paramData.ProcessedValue.(int); ok {
			return float64(v), nil
		} else if v, ok := paramData.ProcessedValue.(int32); ok {
			return float64(v), nil
		} else if v, ok := paramData.ProcessedValue.(int64); ok {
			return float64(v), nil
		}
		return 0, fmt.Errorf("parameter %d value is not a number", *source.SourceID)

	case "setpoint":
		// TODO: Implement setpoint value retrieval
		return 0, fmt.Errorf("setpoint value source not yet implemented")

	default:
		return 0, fmt.Errorf("unknown value source type: %s", source.Type)
	}
}

// executeTreeNode executes a tree node recursively
func (e *WorkflowEngine) executeTreeNode(workflowID uint, workflow *domain.Workflow, node *domain.TreeNode, depth int) error {
	if depth > 10 { // Max depth safety
		return fmt.Errorf("max depth exceeded")
	}

	if node == nil {
		return nil
	}

	if node.Type == "action" {
		// Execute action
		if err := e.executeTreeAction(workflowID, workflow, node); err != nil {
			return err
		}

		// Execute THEN branch for action chaining (sequential actions)
		for _, child := range node.Then {
			if err := e.executeTreeNode(workflowID, workflow, &child, depth+1); err != nil {
				// Continue with next child even if current one fails
			}
		}

		return nil
	}

	if node.Type == "timer" {
		// Execute timer delay
		if node.TimerData == nil {
			return fmt.Errorf("timer node has no timer_data")
		}

		// Calculate delay duration
		var delay time.Duration
		switch node.TimerData.Unit {
		case "seconds":
			delay = time.Duration(node.TimerData.Duration) * time.Second
		case "minutes":
			delay = time.Duration(node.TimerData.Duration) * time.Minute
		case "hours":
			delay = time.Duration(node.TimerData.Duration) * time.Hour
		default:
			return fmt.Errorf("invalid timer unit: %s", node.TimerData.Unit)
		}

		// Sleep for the specified duration
		// This blocks the workflow execution thread
		time.Sleep(delay)

		// Execute THEN branch to continue to next node
		for _, child := range node.Then {
			if err := e.executeTreeNode(workflowID, workflow, &child, depth+1); err != nil {
				// Continue with next child even if current one fails
			}
		}

		return nil
	}

	if node.Type == "if" {
		if node.Condition == nil {
			return fmt.Errorf("IF node has no condition")
		}

		// Evaluate condition
		conditionMet := e.evaluateTreeCondition(node.Condition)

		// Execute appropriate branch
		if conditionMet {
			// Execute THEN branch
			for _, child := range node.Then {
				if err := e.executeTreeNode(workflowID, workflow, &child, depth+1); err != nil {
					// Continue with next child even if current one fails
				}
			}
		} else {
			// Execute ELSE branch
			for _, child := range node.Else {
				if err := e.executeTreeNode(workflowID, workflow, &child, depth+1); err != nil {
					// Continue with next child even if current one fails
				}
			}
		}
	}

	return nil
}

// executeTreeAction executes an action node from tree structure
func (e *WorkflowEngine) executeTreeAction(workflowID uint, workflow *domain.Workflow, node *domain.TreeNode) error {
	if node.ActionData == nil {
		return fmt.Errorf("action node has no action_data")
	}

	switch node.ActionType {
	case domain.ActionTypeAlarmNotification:
		// Extract alarm data
		title, _ := node.ActionData["title"].(string)
		message, _ := node.ActionData["message"].(string)
		severity, _ := node.ActionData["severity"].(string)
		if severity == "" {
			severity = "info" // Default severity
		}

		if message == "" {
			message = title // Fallback to title if message is empty
		}
		if message == "" {
			message = "Alarm triggered" // Default message
		}

		// Get parameter info for alarm (use first condition's parameter if available)
		var parameterID uint
		var parameterName *string
		if workflow.TreeData != nil {
			var treeWorkflow domain.WorkflowTree
			if err := json.Unmarshal([]byte(*workflow.TreeData), &treeWorkflow); err == nil {
				if treeWorkflow.Trigger.Type == domain.TriggerTypeParameterChanged {
					parameterID = treeWorkflow.Trigger.SourceID
					param, err := e.paramRepo.GetByID(parameterID)
					if err == nil && param != nil {
						parameterName = &param.Nama
					}
				}
			}
		}

		// Create or update alarm in current_alarm table
		alarm := &domain.CurrentAlarm{
			WorkflowID:    workflowID,
			WorkflowName:  workflow.Nama,
			ParameterID:   parameterID,
			ParameterName: parameterName,
			AlarmMessage:  message,
			TriggeredAt:   time.Now(),
		}

		if err := e.alarmRepo.CreateOrUpdate(alarm); err != nil {
			return fmt.Errorf("failed to save alarm: %w", err)
		}

		// Broadcast alarm notification via WebSocket
		if e.wsBroadcaster != nil {
			e.wsBroadcaster.BroadcastAlarm(workflowID, workflow.Nama, message)
		}

		// Send alarm to external HTTP endpoint
		go e.sendAlarmToHTTPEndpoint(message, workflowID, workflow.Nama)

		return nil

	case domain.ActionTypeWriteDynamic:
		// Extract write data
		targetID, ok := node.ActionData["target_id"].(float64)
		if !ok {
			return fmt.Errorf("write_dynamic action missing target_id")
		}

		targetParamID := uint(targetID)
		ctx := context.Background()

		// Determine value source: static value or setpoint
		var valuePtr *float64
		valueSource, hasValueSource := node.ActionData["value_source"].(string)

		if hasValueSource && valueSource == "setpoint" {
			// Get value from setpoint
			setpointID, ok := node.ActionData["setpoint_id"].(float64)
			if !ok {
				return fmt.Errorf("write_dynamic action with setpoint source missing setpoint_id")
			}

			// Get setpoint value
			if e.setPointRepo == nil {
				return fmt.Errorf("set-point repository not configured")
			}

			setpoint, err := e.setPointRepo.GetByID(uint(setpointID))
			if err != nil {
				return fmt.Errorf("failed to get set-point %d: %w", uint(setpointID), err)
			}

			valuePtr = &setpoint.Value
		} else {
			// Get static value
			value, ok := node.ActionData["value"].(float64)
			if !ok {
				return fmt.Errorf("write_dynamic action missing value")
			}
			valuePtr = &value
		}

		// Use MANUAL mode (same as /write/execute endpoint)
		// MANUAL mode supports both Value (scalar) and RegisterValues (raw)
		writeReq := WriteRequest{
			Mode:         "MANUAL",
			Value:        valuePtr,
			WorkflowID:   &workflowID,
			WorkflowNama: &workflow.Nama,
		}

		result, err := e.writeService.Write(ctx, targetParamID, writeReq)
		if err != nil {
			return fmt.Errorf("write service error: %w", err)
		}

		if !result.Success {
			return fmt.Errorf("write failed: %s", result.Error)
		}

		return nil

	case domain.ActionTypeWritePreset:
		// Extract preset data
		targetID, ok := node.ActionData["target_id"].(float64)
		if !ok {
			return fmt.Errorf("write_preset action missing target_id")
		}

		targetParamID := uint(targetID)
		ctx := context.Background()
		writeReq := WriteRequest{
			Mode:         "TRIGGER",
			WorkflowID:   &workflowID,
			WorkflowNama: &workflow.Nama,
		}
		_, err := e.writeService.Write(ctx, targetParamID, writeReq)
		return err

	default:
		return fmt.Errorf("unknown action type: %s", node.ActionType)
	}
}
