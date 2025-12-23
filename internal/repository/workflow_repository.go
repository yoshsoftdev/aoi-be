package repository

import (
	"aoi-backend/internal/core/database"
	"aoi-backend/internal/domain"

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
	CheckParameterUsage(parameterID uint) (bool, []uint, error) // Check if parameter is used in workflows, returns (isUsed, workflowIDs, error)
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
	return database.GetDB().Create(workflow).Error
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
	return database.GetDB().Delete(&domain.Workflow{}, id).Error
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
func (r *workflowRepository) CheckParameterUsage(parameterID uint) (bool, []uint, error) {
	db := database.GetDB()
	var workflowIDs []uint

	// Check workflow_condition (parameter is monitored)
	var conditionWorkflows []uint
	err := db.Model(&domain.WorkflowCondition{}).
		Where("parameter_id = ?", parameterID).
		Distinct().
		Pluck("workflow_id", &conditionWorkflows).Error
	if err != nil {
		return false, nil, err
	}

	// Check workflow_action (parameter is written to)
	var actionWorkflows []uint
	err = db.Model(&domain.WorkflowAction{}).
		Where("parameter_id = ?", parameterID).
		Distinct().
		Pluck("workflow_id", &actionWorkflows).Error
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
