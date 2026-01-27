package repository

import (
	"aoi-backend/internal/core/database"
	"aoi-backend/internal/domain"
	"time"
)

// WriteLogRepository handles database operations for write logs
type WriteLogRepository interface {
	Create(log *domain.WriteLog) error
	GetByID(id uint) (*domain.WriteLog, error)
	GetByParameterID(parameterID uint, limit int) ([]domain.WriteLog, error)
	GetByWorkflowID(workflowID uint, limit int) ([]domain.WriteLog, error)
	GetByDateRange(startDate, endDate time.Time, limit int) ([]domain.WriteLog, error)
	GetRecent(limit int) ([]domain.WriteLog, error)
	DeleteOldLogs(beforeDate time.Time) error
}

type writeLogRepository struct{}

// NewWriteLogRepository creates a new WriteLogRepository
func NewWriteLogRepository() WriteLogRepository {
	return &writeLogRepository{}
}

func (r *writeLogRepository) Create(log *domain.WriteLog) error {
	return database.GetLogDB().Create(log).Error
}

func (r *writeLogRepository) GetByID(id uint) (*domain.WriteLog, error) {
	var log domain.WriteLog
	err := database.GetLogDB().First(&log, id).Error
	if err != nil {
		return nil, err
	}
	return &log, nil
}

func (r *writeLogRepository) GetByParameterID(parameterID uint, limit int) ([]domain.WriteLog, error) {
	var logs []domain.WriteLog
	query := database.GetLogDB().Where("parameter_id = ?", parameterID).
		Order("created_at DESC")

	if limit > 0 {
		query = query.Limit(limit)
	}

	err := query.Find(&logs).Error
	return logs, err
}

func (r *writeLogRepository) GetByWorkflowID(workflowID uint, limit int) ([]domain.WriteLog, error) {
	var logs []domain.WriteLog
	query := database.GetLogDB().Where("workflow_id = ?", workflowID).
		Order("created_at DESC")

	if limit > 0 {
		query = query.Limit(limit)
	}

	err := query.Find(&logs).Error
	return logs, err
}

func (r *writeLogRepository) GetByDateRange(startDate, endDate time.Time, limit int) ([]domain.WriteLog, error) {
	var logs []domain.WriteLog
	query := database.GetLogDB().
		Where("created_at >= ? AND created_at <= ?", startDate, endDate).
		Order("created_at DESC")

	if limit > 0 {
		query = query.Limit(limit)
	}

	err := query.Find(&logs).Error
	return logs, err
}

func (r *writeLogRepository) GetRecent(limit int) ([]domain.WriteLog, error) {
	var logs []domain.WriteLog
	query := database.GetLogDB().Order("created_at DESC")

	if limit > 0 {
		query = query.Limit(limit)
	}

	err := query.Find(&logs).Error
	return logs, err
}

func (r *writeLogRepository) DeleteOldLogs(beforeDate time.Time) error {
	return database.GetLogDB().
		Where("created_at < ?", beforeDate).
		Delete(&domain.WriteLog{}).Error
}
