package service

import (
	"aoi-backend/internal/domain"
	"aoi-backend/internal/repository"
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// SetPointService menangani logika bisnis untuk set-point.
// Sesuai req.md: set-point hanya state, tidak write langsung ke device.
type SetPointService struct {
	repo           repository.SetPointRepository
	bypassEngine   *BypassEngine
	workflowEngine *WorkflowEngine
	mu             sync.Mutex
	randomStopCh   chan struct{}
}

func NewSetPointService(repo repository.SetPointRepository) *SetPointService {
	return &SetPointService{repo: repo}
}

// SetBypassEngine sets the bypass engine for triggering bypasses when set-point value changes
func (s *SetPointService) SetBypassEngine(engine *BypassEngine) {
	s.bypassEngine = engine
}

// SetWorkflowEngine wires the workflow engine so that workflows with
// SETPOINT_CHANGED triggers can be evaluated when set-points change.
func (s *SetPointService) SetWorkflowEngine(engine *WorkflowEngine) {
	s.workflowEngine = engine
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
	if sp.RandomEnabled {
		if sp.MinValue == nil || sp.MaxValue == nil {
			return fmt.Errorf("random_enabled requires min_value and max_value")
		}
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
	if sp.RandomEnabled {
		if sp.MinValue == nil || sp.MaxValue == nil {
			return fmt.Errorf("random_enabled requires min_value and max_value")
		}
	}

	// Update set-point
	if err := s.repo.Update(sp); err != nil {
		return fmt.Errorf("failed to update set-point: %w", err)
	}

	// Trigger engines if value changed
	if oldSP.Value != sp.Value {
		// Bypass engine (existing behavior)
		if s.bypassEngine != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := s.bypassEngine.OnSetPointChanged(ctx, sp.ID); err != nil {
				// Silent fail
			}
		}

		// Workflow engine (new behavior for SETPOINT_CHANGED triggers)
		if s.workflowEngine != nil {
			s.workflowEngine.OnSetPointChanged(sp.ID)
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

	// Trigger engines if value changed
	if oldValue != newValue {
		// Bypass engine (existing behavior)
		if s.bypassEngine != nil {
			if err := s.bypassEngine.OnSetPointChanged(ctx, setPointID); err != nil {
				// Silent fail
			}
		}

		// Workflow engine (new behavior for SETPOINT_CHANGED triggers)
		if s.workflowEngine != nil {
			s.workflowEngine.OnSetPointChanged(setPointID)
		}
	}

	return nil
}

func (s *SetPointService) Delete(id uint) error {
	return s.repo.Delete(id)
}

func (s *SetPointService) StartRandomUpdater() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.randomStopCh != nil {
		return
	}
	s.randomStopCh = make(chan struct{})
	go s.randomLoop(s.randomStopCh)
}

func (s *SetPointService) StopRandomUpdater() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.randomStopCh != nil {
		close(s.randomStopCh)
		s.randomStopCh = nil
	}
}

func (s *SetPointService) randomLoop(stopCh <-chan struct{}) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-stopCh:
			return
		case <-ticker.C:
			s.applyRandomStep()
		}
	}
}

func (s *SetPointService) applyRandomStep() {
	items, err := s.repo.GetAll()
	if err != nil {
		return
	}
	for _, sp := range items {
		if !sp.RandomEnabled || sp.MinValue == nil || sp.MaxValue == nil {
			continue
		}
		min := *sp.MinValue
		max := *sp.MaxValue
		if min >= max {
			continue
		}
		step := 1.0
		if max-min < step {
			// kalau range kecil, pakai step pecahan agar tetap bergerak
			step = (max - min) / 5.0
			if step <= 0 {
				continue
			}
		}
		delta := rand.Float64() * step
		if rand.Intn(2) == 0 {
			delta = -delta
		}
		next := sp.Value + delta
		if next < min {
			next = min
		}
		if next > max {
			next = max
		}
		if next != sp.Value {
			_ = s.UpdateValue(context.Background(), sp.ID, next)
		}
	}
}

