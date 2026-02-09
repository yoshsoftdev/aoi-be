package repository

import (
	"aoi-backend/internal/core/database"
	"aoi-backend/internal/domain"
	"fmt"

	"gorm.io/gorm"
)

// BypassRepository mengelola operasi DB untuk bypass + rules + actions.
type BypassRepository interface {
	GetAll() ([]domain.Bypass, error)
	GetByID(id uint) (*domain.Bypass, error)
	Create(bypass *domain.Bypass) error
	Update(bypass *domain.Bypass) error
	Delete(id uint) error
}

type bypassRepository struct {
	db *gorm.DB
}

func NewBypassRepository() BypassRepository {
	return &bypassRepository{
		db: database.GetDB(),
	}
}

func (r *bypassRepository) GetAll() ([]domain.Bypass, error) {
	var list []domain.Bypass
	err := r.db.Preload("Rules").Preload("Actions", func(tx *gorm.DB) *gorm.DB {
		return tx.Order("action_order ASC")
	}).Find(&list).Error
	return list, err
}

func (r *bypassRepository) GetByID(id uint) (*domain.Bypass, error) {
	var b domain.Bypass
	err := r.db.Preload("Rules").Preload("Actions", func(tx *gorm.DB) *gorm.DB {
		return tx.Order("action_order ASC")
	}).First(&b, id).Error
	if err != nil {
		return nil, err
	}
	return &b, nil
}

func (r *bypassRepository) Create(bypass *domain.Bypass) error {
	tx := r.db.Begin()
	if tx.Error != nil {
		return tx.Error
	}

	// Detach relations
	rules := bypass.Rules
	actions := bypass.Actions
	bypass.Rules = nil
	bypass.Actions = nil

	if err := tx.Create(bypass).Error; err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to create bypass: %w", err)
	}

	// Create rules
	for i := range rules {
		rules[i].ID = 0
		rules[i].BypassID = bypass.ID
	}
	if len(rules) > 0 {
		if err := tx.Create(&rules).Error; err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to create bypass rules: %w", err)
		}
	}

	// Create actions with action_order
	for i := range actions {
		actions[i].ID = 0
		actions[i].BypassID = bypass.ID
		actions[i].ActionOrder = i
	}
	if len(actions) > 0 {
		if err := tx.Create(&actions).Error; err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to create bypass actions: %w", err)
		}
	}

	bypass.Rules = rules
	bypass.Actions = actions

	return tx.Commit().Error
}

func (r *bypassRepository) Update(bypass *domain.Bypass) error {
	tx := r.db.Begin()
	if tx.Error != nil {
		return tx.Error
	}

	// Update header
	if err := tx.Model(&domain.Bypass{}).Where("id = ?", bypass.ID).Updates(map[string]interface{}{
		"name":        bypass.Name,
		"enabled":     bypass.Enabled,
		"description": bypass.Description,
	}).Error; err != nil {
		tx.Rollback()
		return err
	}

	// Delete old rules & actions
	if err := tx.Where("bypass_id = ?", bypass.ID).Delete(&domain.BypassRule{}).Error; err != nil {
		tx.Rollback()
		return err
	}
	if err := tx.Where("bypass_id = ?", bypass.ID).Delete(&domain.BypassAction{}).Error; err != nil {
		tx.Rollback()
		return err
	}

	// Insert new rules
	for i := range bypass.Rules {
		bypass.Rules[i].ID = 0
		bypass.Rules[i].BypassID = bypass.ID
	}
	if len(bypass.Rules) > 0 {
		if err := tx.Create(&bypass.Rules).Error; err != nil {
			tx.Rollback()
			return err
		}
	}

	// Insert new actions
	for i := range bypass.Actions {
		bypass.Actions[i].ID = 0
		bypass.Actions[i].BypassID = bypass.ID
		bypass.Actions[i].ActionOrder = i
	}
	if len(bypass.Actions) > 0 {
		if err := tx.Create(&bypass.Actions).Error; err != nil {
			tx.Rollback()
			return err
		}
	}

	return tx.Commit().Error
}

func (r *bypassRepository) Delete(id uint) error {
	return r.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("bypass_id = ?", id).Delete(&domain.BypassRule{}).Error; err != nil {
			return err
		}
		if err := tx.Where("bypass_id = ?", id).Delete(&domain.BypassAction{}).Error; err != nil {
			return err
		}
		if err := tx.Delete(&domain.Bypass{}, id).Error; err != nil {
			return err
		}
		return nil
	})
}

