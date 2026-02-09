package service

import (
	"aoi-backend/internal/domain"
	"aoi-backend/internal/repository"
	"context"
	"fmt"
	"time"
)

// SetPointService menangani logika bisnis untuk set-point.
// Sesuai req.md: set-point hanya state, tidak write langsung ke device.
type SetPointService struct {
	repo         repository.SetPointRepository
	bypassEngine *BypassEngine
}

func NewSetPointService(repo repository.SetPointRepository) *SetPointService {
	return &SetPointService{repo: repo}
}

// SetBypassEngine sets the bypass engine for triggering bypasses when set-point value changes
func (s *SetPointService) SetBypassEngine(engine *BypassEngine) {
	s.bypassEngine = engine
}

func (s *SetPointService) GetAll() ([]domain.SetPoint, error) {
	return s.repo.GetAll()
}

func (s *SetPointService) GetByID(id uint) (*domain.SetPoint, error) {
	return s.repo.GetByID(id)
}

func (s *SetPointService) Create(sp *domain.SetPoint) error {
	// Validasi basic: data_type dan range min/max
	if sp.DataType == "" {
		sp.DataType = "float"
	}
	if sp.MinValue != nil && sp.MaxValue != nil && *sp.MinValue > *sp.MaxValue {
		return fmt.Errorf("min_value cannot be greater than max_value")
	}
	return s.repo.Create(sp)
}

func (s *SetPointService) Update(sp *domain.SetPoint) error {
	// Get old value before update
	oldSP, err := s.repo.GetByID(sp.ID)
	if err != nil {
		return fmt.Errorf("failed to get set-point: %w", err)
	}

	if sp.MinValue != nil && sp.MaxValue != nil && *sp.MinValue > *sp.MaxValue {
		return fmt.Errorf("min_value cannot be greater than max_value")
	}

	// Update set-point
	if err := s.repo.Update(sp); err != nil {
		return fmt.Errorf("failed to update set-point: %w", err)
	}

	// Trigger bypass engine if value changed
	if oldSP.Value != sp.Value && s.bypassEngine != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.bypassEngine.OnSetPointChanged(ctx, sp.ID); err != nil {
			// Log warning but don't fail the update
			fmt.Printf("Warning: Failed to trigger bypasses for set-point %d: %v\n", sp.ID, err)
		}
	}

	return nil
}

// UpdateValue updates only the value of a set-point and triggers bypass evaluation.
// This method is intended to be called from bypass/workflow engines when they need to update set-point values.
func (s *SetPointService) UpdateValue(ctx context.Context, setPointID uint, newValue float64) error {
	// Get existing set-point
	sp, err := s.repo.GetByID(setPointID)
	if err != nil {
		return fmt.Errorf("failed to get set-point: %w", err)
	}

	oldValue := sp.Value
	sp.Value = newValue

	// Validate min/max if set
	if sp.MinValue != nil && sp.Value < *sp.MinValue {
		return fmt.Errorf("value %f is below minimum %f", sp.Value, *sp.MinValue)
	}
	if sp.MaxValue != nil && sp.Value > *sp.MaxValue {
		return fmt.Errorf("value %f is above maximum %f", sp.Value, *sp.MaxValue)
	}

	// Update set-point
	if err := s.repo.Update(sp); err != nil {
		return fmt.Errorf("failed to update set-point: %w", err)
	}

	// Trigger bypass engine if value changed
	if oldValue != newValue && s.bypassEngine != nil {
		if err := s.bypassEngine.OnSetPointChanged(ctx, setPointID); err != nil {
			// Log warning but don't fail the update
			fmt.Printf("Warning: Failed to trigger bypasses for set-point %d: %v\n", setPointID, err)
		}
	}

	return nil
}

func (s *SetPointService) Delete(id uint) error {
	return s.repo.Delete(id)
}

