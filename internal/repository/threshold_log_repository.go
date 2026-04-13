package repository

import (
	"aoi-backend/internal/core/database"
	"aoi-backend/internal/domain"
	"time"
)

type ThresholdLogRepository interface {
	Create(log *domain.ThresholdLog) error
	GetOpenByParameterID(parameterID uint) (*domain.ThresholdLog, error)
	GetByParameterID(parameterID uint, limit int) ([]domain.ThresholdLog, error)
	SetResolveAt(id uint, resolvedAt time.Time) error
}

type thresholdLogRepository struct{}

func NewThresholdLogRepository() ThresholdLogRepository {
	return &thresholdLogRepository{}
}

func (r *thresholdLogRepository) Create(log *domain.ThresholdLog) error {
	return database.GetDB().Create(log).Error
}

func (r *thresholdLogRepository) GetOpenByParameterID(parameterID uint) (*domain.ThresholdLog, error) {
	var log domain.ThresholdLog
	err := database.GetDB().
		Where("parameter_id = ? AND resolve_at IS NULL", parameterID).
		Order("trigger_at DESC").
		First(&log).Error
	if err != nil {
		return nil, err
	}
	return &log, nil
}

func (r *thresholdLogRepository) GetByParameterID(parameterID uint, limit int) ([]domain.ThresholdLog, error) {
	if limit <= 0 {
		limit = 20
	}
	if limit > 200 {
		limit = 200
	}

	var logs []domain.ThresholdLog
	err := database.GetDB().
		Where("parameter_id = ?", parameterID).
		Order("trigger_at DESC").
		Limit(limit).
		Find(&logs).Error
	if err != nil {
		return nil, err
	}
	return logs, nil
}

func (r *thresholdLogRepository) SetResolveAt(id uint, resolvedAt time.Time) error {
	return database.GetDB().
		Model(&domain.ThresholdLog{}).
		Where("id = ?", id).
		Update("resolve_at", resolvedAt).Error
}

