package repository

import (
	"aoi-backend/internal/core/database"
	"aoi-backend/internal/domain"

	"gorm.io/gorm"
)

// AlarmRepository handles database operations for current alarms
type AlarmRepository interface {
	GetAll() ([]domain.CurrentAlarm, error)
	GetByWorkflowID(workflowID uint) (*domain.CurrentAlarm, error)
	CreateOrUpdate(alarm *domain.CurrentAlarm) error
	DeleteByWorkflowID(workflowID uint) error
	DeleteByID(id uint) error
}

type alarmRepository struct{}

// NewAlarmRepository creates a new AlarmRepository
func NewAlarmRepository(db *gorm.DB) AlarmRepository {
	// db parameter is kept for consistency but we use database.GetDB() internally
	return &alarmRepository{}
}

func (r *alarmRepository) GetAll() ([]domain.CurrentAlarm, error) {
	var alarms []domain.CurrentAlarm
	err := database.GetDB().
		Preload("Workflow").
		Preload("Parameter").
		Order("triggered_at DESC").
		Find(&alarms).Error
	return alarms, err
}

func (r *alarmRepository) GetByWorkflowID(workflowID uint) (*domain.CurrentAlarm, error) {
	var alarm domain.CurrentAlarm
	err := database.GetDB().
		Where("workflow_id = ?", workflowID).
		Preload("Workflow").
		Preload("Parameter").
		First(&alarm).Error
	if err == gorm.ErrRecordNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &alarm, nil
}

func (r *alarmRepository) CreateOrUpdate(alarm *domain.CurrentAlarm) error {
	db := database.GetDB()
	
	// Check if alarm already exists for this workflow
	var existing domain.CurrentAlarm
	err := db.Where("workflow_id = ?", alarm.WorkflowID).First(&existing).Error
	
	if err == gorm.ErrRecordNotFound {
		// Create new alarm
		return db.Create(alarm).Error
	}
	if err != nil {
		return err
	}
	
	// Update existing alarm
	alarm.ID = existing.ID
	return db.Save(alarm).Error
}

func (r *alarmRepository) DeleteByWorkflowID(workflowID uint) error {
	return database.GetDB().
		Where("workflow_id = ?", workflowID).
		Delete(&domain.CurrentAlarm{}).Error
}

func (r *alarmRepository) DeleteByID(id uint) error {
	return database.GetDB().Delete(&domain.CurrentAlarm{}, id).Error
}

