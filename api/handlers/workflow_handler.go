package handlers

import (
	"net/http"
	"strconv"

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
type CreateWorkflowRequest struct {
	Nama        string                    `json:"nama" binding:"required"`
	Description string                    `json:"description"`
	Enabled     bool                      `json:"enabled"`
	Conditions  []CreateWorkflowCondition `json:"conditions" binding:"required,min=1"`
	Actions     []CreateWorkflowAction    `json:"actions" binding:"required,min=1"`
}

type CreateWorkflowCondition struct {
	ParameterID uint    `json:"parameter_id" binding:"required"`
	Operator    string  `json:"operator" binding:"required,oneof=> < >= <= == !="`
	Threshold   float64 `json:"threshold" binding:"required"`
}

type CreateWorkflowAction struct {
	ActionType   string   `json:"action_type" binding:"required,oneof=TRIGGER_WRITE WRITE_WITH_VALUE write event delay alarm"`
	ParameterID  *uint    `json:"parameter_id,omitempty"`  // Required for 'TRIGGER_WRITE', 'WRITE_WITH_VALUE', 'write' actions
	Value        *float64 `json:"value,omitempty"`         // Required for 'WRITE_WITH_VALUE', 'write' actions
	EventName    *string  `json:"event_name,omitempty"`    // Required for 'event' action
	DelayMs      *int     `json:"delay_ms,omitempty"`      // Required for 'delay' action
	AlarmMessage *string  `json:"alarm_message,omitempty"` // Required for 'alarm' action
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

	// Build workflow
	workflow := domain.Workflow{
		Nama:        req.Nama,
		Description: req.Description,
		Enabled:     req.Enabled,
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

	// Create workflow
	if err := h.workflowRepo.Create(&workflow); err != nil {
		logger.GetLogger().Errorf("Failed to create workflow: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   "Failed to create workflow",
		})
		return
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
type UpdateWorkflowRequest struct {
	Nama        *string                   `json:"nama"`
	Description *string                   `json:"description"`
	Enabled     *bool                     `json:"enabled"`
	Conditions  []CreateWorkflowCondition `json:"conditions"`
	Actions     []CreateWorkflowAction    `json:"actions"`
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
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	// Get existing workflow
	workflow, err := h.workflowRepo.GetByID(uint(id))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{
			"success": false,
			"error":   "Workflow not found",
		})
		return
	}

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
