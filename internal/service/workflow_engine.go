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
	workflowRepo  repository.WorkflowRepository
	paramRepo     repository.ParameterRepository
	alarmRepo     repository.AlarmRepository
	cache         cache.Cache
	writeService  *WriteService
	wsBroadcaster WebSocketBroadcaster // For broadcasting alarm notifications
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
	cache cache.Cache,
	writeService *WriteService,
	wsBroadcaster WebSocketBroadcaster,
) *WorkflowEngine {
	return &WorkflowEngine{
		workflowRepo:     workflowRepo,
		paramRepo:        paramRepo,
		alarmRepo:        alarmRepo,
		cache:            cache,
		writeService:     writeService,
		wsBroadcaster:    wsBroadcaster,
		workflowsByParam: make(map[uint][]*domain.Workflow),
		debounceMap:      make(map[uint]time.Time),
		debounceWindow:   5 * time.Second, // Default: 5 seconds debounce
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

		// Register workflow for each condition parameter
		for _, condition := range workflow.Conditions {
			paramID := condition.ParameterID
			e.workflowsByParam[paramID] = append(e.workflowsByParam[paramID], workflow)
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
		if err := e.evaluateWorkflow(workflow, parameterID, data); err != nil {
			// Silent fail
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
