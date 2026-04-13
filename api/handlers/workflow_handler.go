package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"aoi-backend/internal/core/logger"
	"aoi-backend/internal/domain"
	"aoi-backend/internal/repository"
	"aoi-backend/internal/service"

	"github.com/gin-gonic/gin"
)

type WorkflowHandler struct {
	workflowRepo   repository.WorkflowRepository
	workflowEngine *service.WorkflowEngine
}

func NewWorkflowHandler(workflowRepo repository.WorkflowRepository, workflowEngine *service.WorkflowEngine) *WorkflowHandler {
	return &WorkflowHandler{
		workflowRepo:   workflowRepo,
		workflowEngine: workflowEngine,
	}
}

// GetAll returns all workflows
func (h *WorkflowHandler) GetAll(c *gin.Context) {
	workflows, err := h.workflowRepo.GetAll()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data":    workflows,
	})
}

// GetByID returns a workflow by ID
func (h *WorkflowHandler) GetByID(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "Invalid workflow ID",
		})
		return
	}

	workflow, err := h.workflowRepo.GetByID(uint(id))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{
			"success": false,
			"error":   "Workflow not found",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data":    workflow,
	})
}

// CreateRequest represents a workflow creation request
// Supports both version 1 (flat) and version 2 (tree) workflows
type CreateWorkflowRequest struct {
	Nama        string `json:"nama" binding:"required"`
	Description string `json:"description"`
	Enabled     bool   `json:"enabled"`
	Version     *int   `json:"version,omitempty"` // 1 = flat, 2 = tree (default: 1)

	// Version 1 (flat) fields
	Conditions []CreateWorkflowCondition `json:"conditions,omitempty"`
	Actions    []CreateWorkflowAction    `json:"actions,omitempty"`

	// Version 2 (tree) fields
	TreeData *domain.WorkflowTree `json:"tree_data,omitempty"`
}

type CreateWorkflowCondition struct {
	ParameterID uint    `json:"parameter_id" binding:"required"`
	Operator    string  `json:"operator" binding:"required,oneof=> < >= <= == !="`
	Threshold   float64 `json:"threshold" binding:"required"`
}

type CreateWorkflowAction struct {
	ActionType   string   `json:"action_type" binding:"required,oneof=TRIGGER_WRITE WRITE_WITH_VALUE WRITE_INTERNAL write event delay alarm"`
	ParameterID  *uint    `json:"parameter_id,omitempty"`  // Required for 'TRIGGER_WRITE', 'WRITE_WITH_VALUE', 'WRITE_INTERNAL', 'write' actions
	Value        *float64 `json:"value,omitempty"`         // Required for 'WRITE_WITH_VALUE', 'WRITE_INTERNAL', 'write' actions
	EventName    *string  `json:"event_name,omitempty"`    // Required for 'event' action
	DelayMs      *int     `json:"delay_ms,omitempty"`      // Required for 'delay' action
	AlarmMessage *string  `json:"alarm_message,omitempty"` // Required for 'alarm' action
	ActionOrder  int      `json:"action_order"`            // Order of execution (0, 1, 2, ...)
}

// Create creates a new workflow
func (h *WorkflowHandler) Create(c *gin.Context) {
	var req CreateWorkflowRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	// Determine version (default: 1)
	version := domain.WorkflowVersionFlat
	if req.Version != nil {
		version = *req.Version
	}

	// Build workflow
	workflow := domain.Workflow{
		Nama:        req.Nama,
		Description: req.Description,
		Enabled:     req.Enabled,
		Version:     version,
	}

	if version == domain.WorkflowVersionTree {
		// Version 2: Tree-based workflow
		if req.TreeData == nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"success": false,
				"error":   "tree_data is required for version 2 workflow",
			})
			return
		}

		// Validate tree structure
		if err := validateWorkflowTree(req.TreeData); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"success": false,
				"error":   err.Error(),
			})
			return
		}

		// Set tree ID to workflow ID (will be set after creation)
		req.TreeData.ID = ""

		// Serialize tree to JSON
		treeJSON, err := json.Marshal(req.TreeData)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"success": false,
				"error":   "Failed to serialize tree_data: " + err.Error(),
			})
			return
		}

		treeJSONStr := string(treeJSON)
		workflow.TreeData = &treeJSONStr
	} else {
		// Version 1: Flat workflow
		if len(req.Conditions) == 0 {
			c.JSON(http.StatusBadRequest, gin.H{
				"success": false,
				"error":   "conditions are required for version 1 workflow",
			})
			return
		}
		if len(req.Actions) == 0 {
			c.JSON(http.StatusBadRequest, gin.H{
				"success": false,
				"error":   "actions are required for version 1 workflow",
			})
			return
		}

		// Validate actions
		for _, action := range req.Actions {
			if action.ActionType == "write" {
				if action.ParameterID == nil {
					c.JSON(http.StatusBadRequest, gin.H{
						"success": false,
						"error":   "parameter_id is required for write action",
					})
					return
				}
				if action.Value == nil {
					c.JSON(http.StatusBadRequest, gin.H{
						"success": false,
						"error":   "value is required for write action",
					})
					return
				}
			} else if action.ActionType == "event" {
				if action.EventName == nil || *action.EventName == "" {
					c.JSON(http.StatusBadRequest, gin.H{
						"success": false,
						"error":   "event_name is required for event action",
					})
					return
				}
			} else if action.ActionType == "delay" {
				if action.DelayMs == nil || *action.DelayMs <= 0 {
					c.JSON(http.StatusBadRequest, gin.H{
						"success": false,
						"error":   "delay_ms is required and must be > 0 for delay action",
					})
					return
				}
			}
		}

		// Build conditions
		for _, condReq := range req.Conditions {
			workflow.Conditions = append(workflow.Conditions, domain.WorkflowCondition{
				ParameterID: condReq.ParameterID,
				Operator:    condReq.Operator,
				Threshold:   condReq.Threshold,
			})
		}

		// Build actions
		for _, actionReq := range req.Actions {
			workflow.Actions = append(workflow.Actions, domain.WorkflowAction{
				ActionType:   actionReq.ActionType,
				ParameterID:  actionReq.ParameterID,
				Value:        actionReq.Value,
				EventName:    actionReq.EventName,
				DelayMs:      actionReq.DelayMs,
				AlarmMessage: actionReq.AlarmMessage,
			})
		}
	}

	// Create workflow
	if err := h.workflowRepo.Create(&workflow); err != nil {
		logger.GetLogger().Errorf("Failed to create workflow: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   "Failed to create workflow",
		})
		return
	}

	// Update tree ID if version 2
	if version == domain.WorkflowVersionTree && workflow.TreeData != nil {
		var tree domain.WorkflowTree
		if err := json.Unmarshal([]byte(*workflow.TreeData), &tree); err == nil {
			tree.ID = strconv.FormatUint(uint64(workflow.ID), 10)
			treeJSON, _ := json.Marshal(tree)
			treeJSONStr := string(treeJSON)
			workflow.TreeData = &treeJSONStr
			h.workflowRepo.Update(&workflow)
		}
	}

	// Reload workflow engine
	if err := h.workflowEngine.LoadAll(); err != nil {
		logger.GetLogger().Warnf("Failed to reload workflow engine: %v", err)
	}

	c.JSON(http.StatusCreated, gin.H{
		"success": true,
		"data":    workflow,
	})
}

// UpdateRequest represents a workflow update request
// Supports both version 1 (flat) and version 2 (tree) workflows
type UpdateWorkflowRequest struct {
	Nama        *string `json:"nama"`
	Description *string `json:"description"`
	Enabled     *bool   `json:"enabled"`
	Version     *int    `json:"version,omitempty"` // 1 = flat, 2 = tree

	// Version 1 (flat) fields
	Conditions []CreateWorkflowCondition `json:"conditions,omitempty"`
	Actions    []CreateWorkflowAction    `json:"actions,omitempty"`

	// Version 2 (tree) fields
	TreeData *domain.WorkflowTree `json:"tree_data,omitempty"`
}

// Update updates a workflow
func (h *WorkflowHandler) Update(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "Invalid workflow ID",
		})
		return
	}

	var req UpdateWorkflowRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		logger.GetLogger().Errorf("Failed to bind update workflow request for ID %d: %v", id, err)
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "Invalid request: " + err.Error(),
		})
		return
	}

	// Get existing workflow
	workflow, err := h.workflowRepo.GetByID(uint(id))
	if err != nil {
		logger.GetLogger().Errorf("Workflow not found: ID %d, error: %v", id, err)
		c.JSON(http.StatusNotFound, gin.H{
			"success": false,
			"error":   "Workflow not found",
		})
		return
	}

	logger.GetLogger().Infof("Updating workflow %d: version=%v, hasTreeData=%v", id, req.Version, req.TreeData != nil)

	// Update fields
	if req.Nama != nil {
		workflow.Nama = *req.Nama
	}
	if req.Description != nil {
		workflow.Description = *req.Description
	}
	if req.Enabled != nil {
		workflow.Enabled = *req.Enabled
	}
	if req.Version != nil {
		workflow.Version = *req.Version
	}

	// Handle version-specific updates
	if workflow.Version == domain.WorkflowVersionTree {
		// Version 2: Tree-based workflow
		if req.TreeData != nil {
			// Validate tree structure
			if err := validateWorkflowTree(req.TreeData); err != nil {
				logger.GetLogger().Errorf("Workflow tree validation failed: %v", err)
				c.JSON(http.StatusBadRequest, gin.H{
					"success": false,
					"error":   "Tree validation failed: " + err.Error(),
				})
				return
			}

			// Set tree ID to workflow ID
			req.TreeData.ID = strconv.FormatUint(uint64(workflow.ID), 10)

			// Serialize tree to JSON
			treeJSON, err := json.Marshal(req.TreeData)
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{
					"success": false,
					"error":   "Failed to serialize tree_data: " + err.Error(),
				})
				return
			}

			treeJSONStr := string(treeJSON)
			workflow.TreeData = &treeJSONStr

			// Clear flat structure when switching to tree
			workflow.Conditions = []domain.WorkflowCondition{}
			workflow.Actions = []domain.WorkflowAction{}
		}
	} else {
		// Version 1: Flat workflow
		if req.Conditions != nil {
			workflow.Conditions = []domain.WorkflowCondition{}
			for _, condReq := range req.Conditions {
				workflow.Conditions = append(workflow.Conditions, domain.WorkflowCondition{
					ParameterID: condReq.ParameterID,
					Operator:    condReq.Operator,
					Threshold:   condReq.Threshold,
				})
			}
		}
		if req.Actions != nil {
			workflow.Actions = []domain.WorkflowAction{}
			for _, actionReq := range req.Actions {
				workflow.Actions = append(workflow.Actions, domain.WorkflowAction{
					ActionType:   actionReq.ActionType,
					ParameterID:  actionReq.ParameterID,
					Value:        actionReq.Value,
					EventName:    actionReq.EventName,
					DelayMs:      actionReq.DelayMs,
					AlarmMessage: actionReq.AlarmMessage,
				})
			}
		}

		// Clear tree data when switching to flat
		if req.Version != nil && *req.Version == domain.WorkflowVersionFlat {
			workflow.TreeData = nil
		}
	}

	// Update workflow
	if err := h.workflowRepo.Update(workflow); err != nil {
		logger.GetLogger().Errorf("Failed to update workflow: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   "Failed to update workflow",
		})
		return
	}

	// Reload workflow engine
	if err := h.workflowEngine.LoadAll(); err != nil {
		logger.GetLogger().Warnf("Failed to reload workflow engine: %v", err)
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data":    workflow,
	})
}

// Delete deletes a workflow
func (h *WorkflowHandler) Delete(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "Invalid workflow ID",
		})
		return
	}

	if err := h.workflowRepo.Delete(uint(id)); err != nil {
		logger.GetLogger().Errorf("Failed to delete workflow: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   "Failed to delete workflow",
		})
		return
	}

	// Reload workflow engine
	if err := h.workflowEngine.LoadAll(); err != nil {
		logger.GetLogger().Warnf("Failed to reload workflow engine: %v", err)
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"message": "Workflow deleted successfully",
	})
}

// validateWorkflowTree validates a workflow tree structure
// Checks: max depth (10), no loops, valid structure
func validateWorkflowTree(tree *domain.WorkflowTree) error {
	if tree == nil {
		return fmt.Errorf("tree is nil")
	}

	if tree.Trigger == nil {
		return fmt.Errorf("trigger is required")
	}

	if tree.Trigger.Type != domain.TriggerTypeParameterChanged && tree.Trigger.Type != domain.TriggerTypeSetpointChanged {
		return fmt.Errorf("invalid trigger type: %s", tree.Trigger.Type)
	}

	if tree.Trigger.SourceID == 0 {
		return fmt.Errorf("trigger source_id is required")
	}

	if tree.Root == nil {
		return fmt.Errorf("root node is required")
	}

	// Validate tree structure recursively
	visited := make(map[string]bool)
	if err := validateTreeNode(tree.Root, 0, 10, visited); err != nil {
		return err
	}

	return nil
}

// validateTreeNode validates a tree node recursively
func validateTreeNode(node *domain.TreeNode, depth int, maxDepth int, visited map[string]bool) error {
	if depth > maxDepth {
		return fmt.Errorf("max depth exceeded: %d (max: %d)", depth, maxDepth)
	}

	if node == nil {
		return fmt.Errorf("node is nil")
	}

	// Check for loops (if node has ID)
	if node.ID != "" {
		if visited[node.ID] {
			return fmt.Errorf("cyclic reference detected: node %s", node.ID)
		}
		visited[node.ID] = true
	}

	// Validate node type
	if node.Type != "if" && node.Type != "action" && node.Type != "timer" {
		return fmt.Errorf("invalid node type: %s (must be 'if', 'action', or 'timer')", node.Type)
	}

	if node.Type == "if" {
		// Validate IF node
		if node.Condition == nil {
			return fmt.Errorf("IF node condition is required")
		}

		// Check condition type
		conditionType := node.Condition.Type
		if conditionType == "" {
			conditionType = "comparison" // Default type
		}

		if conditionType == "time_window" {
			// Validate time window condition
			if node.Condition.TimeWindow == nil {
				return fmt.Errorf("time_window condition requires time_window config")
			}
			if node.Condition.TimeWindow.StartTime == "" {
				return fmt.Errorf("time_window start_time is required")
			}
			if node.Condition.TimeWindow.EndTime == "" {
				return fmt.Errorf("time_window end_time is required")
			}
			// Validate time format by trying to evaluate
			_, err := domain.EvaluateTimeWindow(*node.Condition.TimeWindow)
			if err != nil {
				return fmt.Errorf("time_window validation failed: %w", err)
			}
			// Validate days if provided
			if len(node.Condition.TimeWindow.Days) > 0 {
				validDays := []string{"mon", "tue", "wed", "thu", "fri", "sat", "sun"}
				for _, day := range node.Condition.TimeWindow.Days {
					valid := false
					for _, validDay := range validDays {
						if strings.ToLower(day) == validDay {
							valid = true
							break
						}
					}
					if !valid {
						return fmt.Errorf("invalid day in time_window days: %s (must be mon, tue, wed, thu, fri, sat, or sun)", day)
					}
				}
			}
		} else {
			// Validate comparison condition (default)
			if node.Condition.Left == nil {
				return fmt.Errorf("comparison condition left is required")
			}
			if node.Condition.Right == nil {
				return fmt.Errorf("comparison condition right is required")
			}
			if node.Condition.Operator == "" {
				return fmt.Errorf("comparison condition operator is required")
			}

			// Validate operator
			validOperators := []string{">", "<", ">=", "<=", "==", "!="}
			valid := false
			for _, op := range validOperators {
				if node.Condition.Operator == op {
					valid = true
					break
				}
			}
			if !valid {
				return fmt.Errorf("invalid operator: %s", node.Condition.Operator)
			}

			// Validate value sources
			if err := validateValueSource(node.Condition.Left); err != nil {
				return fmt.Errorf("condition left: %w", err)
			}
			if err := validateValueSource(node.Condition.Right); err != nil {
				return fmt.Errorf("condition right: %w", err)
			}
		}

		// Recursively validate THEN branch
		// Create a copy of visited map for each branch to allow same node in different branches
		for i, child := range node.Then {
			thenVisited := make(map[string]bool)
			for k, v := range visited {
				thenVisited[k] = v
			}
			if err := validateTreeNode(&child, depth+1, maxDepth, thenVisited); err != nil {
				return fmt.Errorf("then[%d]: %w", i, err)
			}
		}

		// Recursively validate ELSE branch
		// Create a copy of visited map for each branch to allow same node in different branches
		for i, child := range node.Else {
			elseVisited := make(map[string]bool)
			for k, v := range visited {
				elseVisited[k] = v
			}
			if err := validateTreeNode(&child, depth+1, maxDepth, elseVisited); err != nil {
				return fmt.Errorf("else[%d]: %w", i, err)
			}
		}
	} else if node.Type == "timer" {
		// Validate timer node
		if node.TimerData == nil {
			return fmt.Errorf("timer node timer_data is required")
		}
		if node.TimerData.Duration <= 0 {
			return fmt.Errorf("timer duration must be greater than 0")
		}
		validUnits := []string{"seconds", "minutes", "hours"}
		valid := false
		for _, unit := range validUnits {
			if node.TimerData.Unit == unit {
				valid = true
				break
			}
		}
		if !valid {
			return fmt.Errorf("invalid timer unit: %s (must be seconds, minutes, or hours)", node.TimerData.Unit)
		}

		// Recursively validate THEN branch (for continuing after delay)
		childVisited := make(map[string]bool)
		for k, v := range visited {
			childVisited[k] = v
		}
		for i, child := range node.Then {
			if err := validateTreeNode(&child, depth+1, maxDepth, childVisited); err != nil {
				return fmt.Errorf("timer then[%d]: %w", i, err)
			}
		}
	} else if node.Type == "action" {
		// Validate action node
		if node.ActionType == "" {
			return fmt.Errorf("action node action_type is required")
		}

		// Validate action type
		validActionTypes := []string{
			domain.ActionTypeAlarmNotification,
			domain.ActionTypeWriteDynamic,
			domain.ActionTypeWritePreset,
		}
		valid := false
		for _, at := range validActionTypes {
			if node.ActionType == at {
				valid = true
				break
			}
		}
		if !valid {
			return fmt.Errorf("invalid action_type: %s", node.ActionType)
		}

		// Recursively validate THEN branch (for action chaining)
		// For action chaining, we need to check for cycles within the same chain
		// But allow the same node to appear in different parallel chains
		// So we create a copy of visited for each child to allow parallel chains
		for i, child := range node.Then {
			// Create a copy of visited map for each child to allow parallel action chains
			childVisited := make(map[string]bool)
			for k, v := range visited {
				childVisited[k] = v
			}
			if err := validateTreeNode(&child, depth+1, maxDepth, childVisited); err != nil {
				return fmt.Errorf("action then[%d]: %w", i, err)
			}
		}

		// Validate action data based on type
		if node.ActionData == nil {
			return fmt.Errorf("action node action_data is required")
		}

		switch node.ActionType {
		case domain.ActionTypeAlarmNotification:
			// Alarm notification requires message
			if _, ok := node.ActionData["message"]; !ok {
				return fmt.Errorf("alarm_notification action requires 'message' in action_data")
			}

		case domain.ActionTypeWriteDynamic:
			// Write dynamic requires target_id
			if _, ok := node.ActionData["target_id"]; !ok {
				return fmt.Errorf("write_dynamic action requires 'target_id' in action_data")
			}
			// target_type is optional (default: parameter), but if present it must be valid.
			if targetTypeRaw, ok := node.ActionData["target_type"]; ok {
				if targetType, ok := targetTypeRaw.(string); !ok || (targetType != "parameter" && targetType != "setpoint") {
					return fmt.Errorf("write_dynamic action has invalid 'target_type' (must be 'parameter' or 'setpoint')")
				}
			}
			// Value can come from either static value or setpoint
			valueSource, hasValueSource := node.ActionData["value_source"]
			if hasValueSource && valueSource == "setpoint" {
				// If value_source is setpoint, require setpoint_id
				if _, ok := node.ActionData["setpoint_id"]; !ok {
					return fmt.Errorf("write_dynamic action with setpoint source requires 'setpoint_id' in action_data")
				}
			} else {
				// Otherwise, require value (static)
				if _, ok := node.ActionData["value"]; !ok {
					return fmt.Errorf("write_dynamic action requires 'value' in action_data (or 'setpoint_id' if value_source is 'setpoint')")
				}
			}

		case domain.ActionTypeWritePreset:
			// Write preset requires target_id
			if _, ok := node.ActionData["target_id"]; !ok {
				return fmt.Errorf("write_preset action requires 'target_id' in action_data")
			}
		}
	}

	return nil
}

// validateValueSource validates a value source
func validateValueSource(source *domain.ValueSource) error {
	if source == nil {
		return fmt.Errorf("value source is nil")
	}

	switch source.Type {
	case "static":
		if source.Value == nil {
			return fmt.Errorf("static value source requires 'value'")
		}

	case "parameter":
		if source.SourceID == nil || *source.SourceID == 0 {
			return fmt.Errorf("parameter value source requires 'source_id'")
		}

	case "setpoint":
		if source.SourceID == nil || *source.SourceID == 0 {
			return fmt.Errorf("setpoint value source requires 'source_id'")
		}

	default:
		return fmt.Errorf("invalid value source type: %s (must be 'static', 'parameter', or 'setpoint')", source.Type)
	}

	return nil
}
