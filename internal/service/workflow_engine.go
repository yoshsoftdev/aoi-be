package service

import (
	"context"
	"fmt"
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
	cache cache.Cache,
	writeService *WriteService,
	wsBroadcaster WebSocketBroadcaster,
) *WorkflowEngine {
	return &WorkflowEngine{
		workflowRepo:     workflowRepo,
		paramRepo:        paramRepo,
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

	log := logger.GetLogger()
	log.Infof("WorkflowEngine: Loaded %d enabled workflows", len(workflows))
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

	log := logger.GetLogger()
	log.Debugf("WorkflowEngine: Parameter %d updated, evaluating %d workflows", parameterID, len(workflows))

	// Evaluate each workflow
	for _, workflow := range workflows {
		if err := e.evaluateWorkflow(workflow, parameterID, data); err != nil {
			log.Warnf("WorkflowEngine: Failed to evaluate workflow %d: %v", workflow.ID, err)
		}
	}
}

// evaluateWorkflow evaluates a workflow's conditions and executes actions if conditions are met
// Implements edge trigger: only execute when condition changes from false to true
func (e *WorkflowEngine) evaluateWorkflow(workflow *domain.Workflow, updatedParamID uint, data *domain.RealtimeData) error {
	log := logger.GetLogger()

	// Get workflow state for edge trigger
	state, err := e.workflowRepo.GetStateByWorkflowID(workflow.ID)
	if err != nil {
		log.Warnf("WorkflowEngine: Failed to get state for workflow %d: %v", workflow.ID, err)
		state = nil
	}

	// Check debounce: don't execute if within debounce window
	e.mu.RLock()
	lastExec, exists := e.debounceMap[workflow.ID]
	e.mu.RUnlock()
	if exists && time.Since(lastExec) < e.debounceWindow {
		log.Debugf("WorkflowEngine: Workflow %d is within debounce window, skipping", workflow.ID)
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
			// Workflow hanya boleh monitor parameter yang sudah clean (DIRECT, CLEAN, FORMULA)
			log.Debugf("WorkflowEngine: Clean data for parameter %d not found in cache (parameter:clean:%d). Workflow only works on clean data (DIRECT, CLEAN, FORMULA).", condition.ParameterID, condition.ParameterID)
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
				log.Warnf("WorkflowEngine: Cannot convert ProcessedValue to float64 for parameter %d", condition.ParameterID)
				allConditionsMet = false
				break
			}
		} else {
			log.Debugf("WorkflowEngine: ProcessedValue is nil for parameter %d", condition.ParameterID)
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
			log.Infof("WorkflowEngine: Workflow %d condition met (edge trigger)", workflow.ID)
		} else {
			// Condition was already true, don't execute again (idempotent)
			log.Debugf("WorkflowEngine: Workflow %d condition already met, skipping (idempotent)", workflow.ID)
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
		log.Warnf("WorkflowEngine: Failed to update state for workflow %d: %v", workflow.ID, err)
	}

	// Execute actions if condition is met and edge triggered
	if shouldExecute {
		// Update debounce map
		e.mu.Lock()
		e.debounceMap[workflow.ID] = time.Now()
		e.mu.Unlock()

		// Execute all actions
		for _, action := range workflow.Actions {
			if err := e.executeAction(workflow.ID, action); err != nil {
				log.Errorf("WorkflowEngine: Failed to execute action %d for workflow %d: %v", action.ID, workflow.ID, err)
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
		logger.GetLogger().Warnf("WorkflowEngine: Unknown operator: %s", operator)
		return false
	}
}

// executeAction executes a workflow action
func (e *WorkflowEngine) executeAction(workflowID uint, action domain.WorkflowAction) error {
	log := logger.GetLogger()

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
		log.Infof("WorkflowEngine: Executing TRIGGER_WRITE action: workflow=%d, parameter=%d (PRESET)", workflowID, *action.ParameterID)

		ctx := context.Background()
		writeReq := WriteRequest{
			Mode: "TRIGGER", // New mode for triggering PRESET parameters
		}

		result, err := e.writeService.Write(ctx, *action.ParameterID, writeReq)
		if err != nil {
			return fmt.Errorf("write service error: %w", err)
		}

		if !result.Success {
			return fmt.Errorf("write failed: %s", result.Error)
		}

		log.Infof("WorkflowEngine: TRIGGER_WRITE action successful: workflow=%d, parameter=%d", workflowID, *action.ParameterID)

	case "WRITE_WITH_VALUE":
		// WRITE_WITH_VALUE: write DYNAMIC parameter (value required)
		if action.ParameterID == nil {
			return fmt.Errorf("parameter_id is required for WRITE_WITH_VALUE action")
		}
		if action.Value == nil {
			return fmt.Errorf("value is required for WRITE_WITH_VALUE action")
		}

		// Validate parameter is WRITE type
		param, err := e.paramRepo.GetByID(*action.ParameterID)
		if err != nil {
			return fmt.Errorf("failed to get parameter %d: %w", *action.ParameterID, err)
		}

		if param.Direction != "WRITE" {
			return fmt.Errorf("parameter %d is not a WRITE parameter", *action.ParameterID)
		}

		// Validate write_mode is DYNAMIC
		if param.WriteMode == nil || *param.WriteMode != "DYNAMIC" {
			return fmt.Errorf("parameter %d is not a DYNAMIC parameter (write_mode must be DYNAMIC for WRITE_WITH_VALUE)", *action.ParameterID)
		}

		// Execute write via WriteService
		log.Infof("WorkflowEngine: Executing WRITE_WITH_VALUE action: workflow=%d, parameter=%d, value=%f (DYNAMIC)", workflowID, *action.ParameterID, *action.Value)

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
			return fmt.Errorf("write failed: %s", result.Error)
		}

		log.Infof("WorkflowEngine: WRITE_WITH_VALUE action successful: workflow=%d, parameter=%d", workflowID, *action.ParameterID)

	case "write":
		// Legacy support: treat as WRITE_WITH_VALUE
		log.Warnf("WorkflowEngine: Legacy 'write' action type detected, treating as WRITE_WITH_VALUE")
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

		if param.Direction != "WRITE" {
			return fmt.Errorf("parameter %d is not a WRITE parameter", *action.ParameterID)
		}

		// Check write_mode and validate accordingly
		if param.WriteMode != nil && *param.WriteMode == "PRESET" {
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
			return fmt.Errorf("write failed: %s", result.Error)
		}

		log.Infof("WorkflowEngine: Write action successful: workflow=%d, parameter=%d", workflowID, *action.ParameterID)

	case "event":
		// TODO: Implement event bus
		if action.EventName == nil {
			return fmt.Errorf("event_name is required for event action")
		}
		log.Infof("WorkflowEngine: Emitting event: workflow=%d, event=%s", workflowID, *action.EventName)
		// e.eventBus.Publish(*action.EventName, workflowID)

	case "delay":
		if action.DelayMs == nil {
			return fmt.Errorf("delay_ms is required for delay action")
		}
		log.Infof("WorkflowEngine: Delay action: workflow=%d, delay=%dms", workflowID, *action.DelayMs)
		time.Sleep(time.Duration(*action.DelayMs) * time.Millisecond)

	case "alarm":
		// ALARM: Show notification with custom message
		if action.AlarmMessage == nil || *action.AlarmMessage == "" {
			return fmt.Errorf("alarm_message is required for alarm action")
		}

		// Get workflow name for notification
		workflow, err := e.workflowRepo.GetByID(workflowID)
		if err != nil {
			log.Warnf("WorkflowEngine: Failed to get workflow %d for alarm: %v", workflowID, err)
			workflow = nil
		}

		workflowName := "Unknown Workflow"
		if workflow != nil {
			workflowName = workflow.Nama
		}

		log.Infof("WorkflowEngine: Alarm action triggered: workflow=%d (%s), message=%s", workflowID, workflowName, *action.AlarmMessage)

		// Broadcast alarm notification via WebSocket
		if e.wsBroadcaster != nil {
			e.wsBroadcaster.BroadcastAlarm(workflowID, workflowName, *action.AlarmMessage)
		} else {
			log.Warnf("WorkflowEngine: WebSocket broadcaster not available, alarm notification not sent")
		}

	default:
		return fmt.Errorf("unknown action type: %s", action.ActionType)
	}

	return nil
}
