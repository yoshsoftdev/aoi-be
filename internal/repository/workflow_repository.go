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
	return database.GetDB().Save(workflow).Error
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
