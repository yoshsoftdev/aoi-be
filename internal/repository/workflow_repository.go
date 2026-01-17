package repository

import (
	"aoi-backend/internal/core/database"
	"aoi-backend/internal/domain"
	"fmt"

	"gorm.io/gorm"
)

// WorkflowRepository handles database operations for workflows
type WorkflowRepository interface {
	GetAll() ([]domain.Workflow, error)
	GetByID(id uint) (*domain.Workflow, error)
	Create(workflow *domain.Workflow) error
	Update(workflow *domain.Workflow) error
	Delete(id uint) error
	GetByParameterID(parameterID uint) ([]domain.Workflow, error) // Get workflows that monitor this parameter
	GetStateByWorkflowID(workflowID uint) (*domain.WorkflowState, error)
	CreateOrUpdateState(state *domain.WorkflowState) error
	CheckParameterUsage(parameterID uint) (bool, []uint, error)                   // Check if parameter is used in workflows, returns (isUsed, workflowIDs, error)
	CheckParameterUsageWithNames(parameterID uint) (bool, map[uint]string, error) // Check if parameter is used in workflows, returns (isUsed, map[workflowID]workflowName, error)
}

type workflowRepository struct{}

// NewWorkflowRepository creates a new WorkflowRepository
func NewWorkflowRepository(db *gorm.DB) WorkflowRepository {
	// db parameter is kept for consistency but we use database.GetDB() internally
	return &workflowRepository{}
}

func (r *workflowRepository) GetAll() ([]domain.Workflow, error) {
	var workflows []domain.Workflow
	err := database.GetDB().Preload("Conditions.Parameter").Preload("Actions.Parameter").Preload("State").Find(&workflows).Error
	return workflows, err
}

func (r *workflowRepository) GetByID(id uint) (*domain.Workflow, error) {
	var workflow domain.Workflow
	err := database.GetDB().Preload("Conditions.Parameter").Preload("Actions.Parameter").Preload("State").First(&workflow, id).Error
	if err != nil {
		return nil, err
	}
	return &workflow, nil
}

func (r *workflowRepository) Create(workflow *domain.Workflow) error {
	db := database.GetDB()

	// Store conditions and actions separately to prevent GORM from creating them twice
	conditions := workflow.Conditions
	actions := workflow.Actions

	// Use transaction to ensure atomicity
	return db.Transaction(func(tx *gorm.DB) error {
		// Reset all IDs to ensure no conflicts with deleted workflows
		workflow.ID = 0

		// Clear Conditions and Actions from workflow struct to prevent GORM from creating them
		// We'll create them separately after workflow is created
		workflow.Conditions = nil
		workflow.Actions = nil

		// Create workflow first (without relations)
		if err := tx.Create(workflow).Error; err != nil {
			return fmt.Errorf("failed to create workflow: %w", err)
		}

		// Reset IDs for all conditions and set WorkflowID
		for i := range conditions {
			conditions[i].ID = 0
			conditions[i].WorkflowID = workflow.ID
		}

		// Create conditions separately
		if len(conditions) > 0 {
			if err := tx.Create(&conditions).Error; err != nil {
				return fmt.Errorf("failed to create workflow conditions: %w", err)
			}
			// Restore conditions to workflow struct for response
			workflow.Conditions = conditions
		}

		// Reset IDs for all actions and set WorkflowID
		for i := range actions {
			actions[i].ID = 0
			actions[i].WorkflowID = workflow.ID
		}

		// Create actions separately
		if len(actions) > 0 {
			if err := tx.Create(&actions).Error; err != nil {
				return fmt.Errorf("failed to create workflow actions: %w", err)
			}
			// Restore actions to workflow struct for response
			workflow.Actions = actions
		}

		return nil
	})
}

func (r *workflowRepository) Update(workflow *domain.Workflow) error {
	db := database.GetDB()

	// Start transaction to ensure atomicity
	return db.Transaction(func(tx *gorm.DB) error {
		// Delete old conditions
		if err := tx.Where("workflow_id = ?", workflow.ID).Delete(&domain.WorkflowCondition{}).Error; err != nil {
			return err
		}

		// Delete old actions
		if err := tx.Where("workflow_id = ?", workflow.ID).Delete(&domain.WorkflowAction{}).Error; err != nil {
			return err
		}

		// Update workflow basic fields
		if err := tx.Model(&domain.Workflow{}).Where("id = ?", workflow.ID).Updates(map[string]interface{}{
			"nama":        workflow.Nama,
			"description": workflow.Description,
			"enabled":     workflow.Enabled,
		}).Error; err != nil {
			return err
		}

		// Create new conditions
		if len(workflow.Conditions) > 0 {
			for i := range workflow.Conditions {
				workflow.Conditions[i].WorkflowID = workflow.ID
			}
			if err := tx.Create(&workflow.Conditions).Error; err != nil {
				return err
			}
		}

		// Create new actions
		if len(workflow.Actions) > 0 {
			for i := range workflow.Actions {
				workflow.Actions[i].WorkflowID = workflow.ID
			}
			if err := tx.Create(&workflow.Actions).Error; err != nil {
				return err
			}
		}

		return nil
	})
}

func (r *workflowRepository) Delete(id uint) error {
	db := database.GetDB()

	// Use transaction to ensure atomicity
	return db.Transaction(func(tx *gorm.DB) error {
		// 1. Delete workflow state (if exists) - explicit delete to ensure cleanup
		if err := tx.Where("workflow_id = ?", id).Unscoped().Delete(&domain.WorkflowState{}).Error; err != nil {
			return fmt.Errorf("failed to delete workflow state: %w", err)
		}

		// 2. Delete workflow conditions - explicit delete to ensure cleanup
		if err := tx.Where("workflow_id = ?", id).Unscoped().Delete(&domain.WorkflowCondition{}).Error; err != nil {
			return fmt.Errorf("failed to delete workflow conditions: %w", err)
		}

		// 3. Delete workflow actions - explicit delete to ensure cleanup
		if err := tx.Where("workflow_id = ?", id).Unscoped().Delete(&domain.WorkflowAction{}).Error; err != nil {
			return fmt.Errorf("failed to delete workflow actions: %w", err)
		}

		// 4. Delete current alarm (if exists) - explicit delete to ensure cleanup
		// Note: Don't fail if no records found (alarm may not exist)
		if err := tx.Where("workflow_id = ?", id).Delete(&domain.CurrentAlarm{}).Error; err != nil && err != gorm.ErrRecordNotFound {
			return fmt.Errorf("failed to delete current alarm: %w", err)
		}

		// 5. Finally, delete the workflow itself
		if err := tx.Unscoped().Delete(&domain.Workflow{}, id).Error; err != nil {
			return fmt.Errorf("failed to delete workflow: %w", err)
		}

		return nil
	})
}

// GetByParameterID returns all workflows that monitor the given parameter
func (r *workflowRepository) GetByParameterID(parameterID uint) ([]domain.Workflow, error) {
	var workflows []domain.Workflow
	err := database.GetDB().
		Joins("JOIN workflow_condition ON workflow_condition.workflow_id = workflow.id").
		Where("workflow_condition.parameter_id = ? AND workflow.enabled = ?", parameterID, true).
		Preload("Conditions", "parameter_id = ?", parameterID).
		Preload("Conditions.Parameter").
		Preload("Actions.Parameter").
		Preload("State").
		Find(&workflows).Error
	return workflows, err
}

func (r *workflowRepository) GetStateByWorkflowID(workflowID uint) (*domain.WorkflowState, error) {
	var state domain.WorkflowState
	err := database.GetDB().Where("workflow_id = ?", workflowID).First(&state).Error
	if err == gorm.ErrRecordNotFound {
		return nil, nil // State doesn't exist yet
	}
	if err != nil {
		return nil, err
	}
	return &state, nil
}

func (r *workflowRepository) CreateOrUpdateState(state *domain.WorkflowState) error {
	var existing domain.WorkflowState
	err := database.GetDB().Where("workflow_id = ?", state.WorkflowID).First(&existing).Error
	if err == gorm.ErrRecordNotFound {
		// Create new state
		return database.GetDB().Create(state).Error
	}
	if err != nil {
		return err
	}
	// Update existing state
	state.ID = existing.ID
	return database.GetDB().Save(state).Error
}

// CheckParameterUsage checks if a parameter is used in any workflow
// Returns: (isUsed, workflowIDs, error)
// Checks both workflow_condition (monitored parameter) and workflow_action (write target)
// IMPORTANT: Only checks workflows that still exist (JOIN with workflow table)
func (r *workflowRepository) CheckParameterUsage(parameterID uint) (bool, []uint, error) {
	db := database.GetDB()
	var workflowIDs []uint

	// Check workflow_condition (parameter is monitored)
	// JOIN with workflow table to ensure workflow still exists
	var conditionWorkflows []uint
	err := db.Model(&domain.WorkflowCondition{}).
		Joins("JOIN workflow ON workflow.id = workflow_condition.workflow_id").
		Where("workflow_condition.parameter_id = ?", parameterID).
		Distinct().
		Pluck("workflow_condition.workflow_id", &conditionWorkflows).Error
	if err != nil {
		return false, nil, err
	}

	// Check workflow_action (parameter is written to)
	// JOIN with workflow table to ensure workflow still exists
	var actionWorkflows []uint
	err = db.Model(&domain.WorkflowAction{}).
		Joins("JOIN workflow ON workflow.id = workflow_action.workflow_id").
		Where("workflow_action.parameter_id = ?", parameterID).
		Distinct().
		Pluck("workflow_action.workflow_id", &actionWorkflows).Error
	if err != nil {
		return false, nil, err
	}

	// Combine and deduplicate workflow IDs
	workflowMap := make(map[uint]bool)
	for _, id := range conditionWorkflows {
		workflowMap[id] = true
	}
	for _, id := range actionWorkflows {
		workflowMap[id] = true
	}

	for id := range workflowMap {
		workflowIDs = append(workflowIDs, id)
	}

	return len(workflowIDs) > 0, workflowIDs, nil
}

// CheckParameterUsageWithNames checks if a parameter is used in any workflow
// Returns: (isUsed, map[workflowID]workflowName, error)
// Checks both workflow_condition (monitored parameter) and workflow_action (write target)
// IMPORTANT: Only checks workflows that still exist (JOIN with workflow table)
func (r *workflowRepository) CheckParameterUsageWithNames(parameterID uint) (bool, map[uint]string, error) {
	db := database.GetDB()
	workflowMap := make(map[uint]string)

	// Check workflow_condition (parameter is monitored)
	// JOIN with workflow table to get workflow name
	type ConditionResult struct {
		WorkflowID   uint   `gorm:"column:workflow_id"`
		WorkflowName string `gorm:"column:workflow_name"`
	}
	var conditionResults []ConditionResult
	err := db.Model(&domain.WorkflowCondition{}).
		Select("workflow_condition.workflow_id, workflow.nama as workflow_name").
		Joins("JOIN workflow ON workflow.id = workflow_condition.workflow_id").
		Where("workflow_condition.parameter_id = ?", parameterID).
		Distinct().
		Scan(&conditionResults).Error
	if err != nil {
		return false, nil, err
	}

	// Check workflow_action (parameter is written to)
	// JOIN with workflow table to get workflow name
	type ActionResult struct {
		WorkflowID   uint   `gorm:"column:workflow_id"`
		WorkflowName string `gorm:"column:workflow_name"`
	}
	var actionResults []ActionResult
	err = db.Model(&domain.WorkflowAction{}).
		Select("workflow_action.workflow_id, workflow.nama as workflow_name").
		Joins("JOIN workflow ON workflow.id = workflow_action.workflow_id").
		Where("workflow_action.parameter_id = ?", parameterID).
		Distinct().
		Scan(&actionResults).Error
	if err != nil {
		return false, nil, err
	}

	// Combine and deduplicate workflows with names
	for _, result := range conditionResults {
		workflowMap[result.WorkflowID] = result.WorkflowName
	}
	for _, result := range actionResults {
		workflowMap[result.WorkflowID] = result.WorkflowName
	}

	return len(workflowMap) > 0, workflowMap, nil
}
