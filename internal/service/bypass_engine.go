package service

import (
	"aoi-backend/internal/core/cache"
	"aoi-backend/internal/domain"
	"aoi-backend/internal/repository"
	"context"
	"fmt"
	"sync"
	"time"
)

// BypassEngine mengeksekusi bypass:
// - membaca nilai set-point / parameter dari cache / DB
// - mengevaluasi rule
// - menulis parameter via WriteService
// - mem-pause polling saat write berjalan (TODO)
type BypassEngine struct {
	bypassRepo      repository.BypassRepository
	setPointRepo    repository.SetPointRepository
	setPointService *SetPointService
	paramRepo       repository.ParameterRepository
	cache           cache.Cache
	writeService    *WriteService

	mu       sync.RWMutex
	bypasses []domain.Bypass
}

func NewBypassEngine(
	bypassRepo repository.BypassRepository,
	setPointRepo repository.SetPointRepository,
	paramRepo repository.ParameterRepository,
	cache cache.Cache,
	writeService *WriteService,
) *BypassEngine {
	return &BypassEngine{
		bypassRepo:   bypassRepo,
		setPointRepo: setPointRepo,
		paramRepo:    paramRepo,
		cache:        cache,
		writeService: writeService,
	}
}

// SetSetPointService sets the set-point service for updating set-point values
func (e *BypassEngine) SetSetPointService(svc *SetPointService) {
	e.setPointService = svc
}

// LoadAll memuat semua definisi bypass dari DB ke memory.
func (e *BypassEngine) LoadAll() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	list, err := e.bypassRepo.GetAll()
	if err != nil {
		return fmt.Errorf("failed to load bypass list: %w", err)
	}
	e.bypasses = list
	return nil
}

// OnSetPointChanged dipanggil ketika nilai set-point berubah (harus di-wire dari SetPoint handler/service).
// Engine akan mengevaluasi semua bypass yang menggunakan set-point tersebut.
func (e *BypassEngine) OnSetPointChanged(ctx context.Context, setPointID uint) error {
	e.mu.RLock()
	bypasses := e.bypasses
	e.mu.RUnlock()

	for i := range bypasses {
		b := &bypasses[i]
		if !b.Enabled {
			continue
		}

		// Cek apakah bypass punya rule yang melibatkan set-point ini
		usesSetPoint := false
		for _, r := range b.Rules {
			if r.LeftSourceType == "set_point" && r.LeftSourceID == setPointID {
				usesSetPoint = true
				break
			}
			if r.RightValueType == "set_point" && r.RightSourceID != nil && *r.RightSourceID == setPointID {
				usesSetPoint = true
				break
			}
		}
		if !usesSetPoint {
			continue
		}

		ok, err := e.evaluateBypass(ctx, b)
		if err != nil {
			continue
		}
		if ok {
			if err := e.executeActions(ctx, b); err != nil {
				// Silent fail
			}
		}
	}

	return nil
}

// evaluateBypass mengembalikan true jika semua rule terpenuhi.
func (e *BypassEngine) evaluateBypass(ctx context.Context, b *domain.Bypass) (bool, error) {
	if len(b.Rules) == 0 {
		// Tidak ada rule -> dianggap selalu true (tapi desain kita biasanya selalu ada rule)
		return true, nil
	}

	for _, r := range b.Rules {
		if r.ConditionType == "always" {
			// Selalu true
			continue
		}

		// Ambil nilai kiri
		leftValue, err := e.getSourceValue(ctx, r.LeftSourceType, r.LeftSourceID)
		if err != nil {
			return false, err
		}

		// Ambil nilai kanan
		var rightValue float64
		if r.RightValueType == "static" {
			if r.RightValue == nil {
				return false, fmt.Errorf("rule %d: right_value is nil for static type", r.ID)
			}
			rightValue = *r.RightValue
		} else if r.RightValueType == "set_point" {
			if r.RightSourceID == nil {
				return false, fmt.Errorf("rule %d: right_source_id is nil for set_point type", r.ID)
			}
			rightValue, err = e.getSourceValue(ctx, "set_point", *r.RightSourceID)
			if err != nil {
				return false, err
			}
		} else {
			return false, fmt.Errorf("rule %d: invalid right_value_type %s", r.ID, r.RightValueType)
		}

		// Evaluasi operator
		if !evaluateNumericCondition(leftValue, r.Operator, rightValue) {
			return false, nil
		}
	}

	return true, nil
}

// getSourceValue mengambil nilai dari set-point atau parameter (clean data dari cache).
func (e *BypassEngine) getSourceValue(ctx context.Context, sourceType string, sourceID uint) (float64, error) {
	switch sourceType {
	case "set_point":
		sp, err := e.setPointRepo.GetByID(sourceID)
		if err != nil {
			return 0, err
		}
		return sp.Value, nil
	case "parameter":
		// Ambil dari cache clean data
		key := fmt.Sprintf("parameter:clean:%d", sourceID)
		var data *domain.RealtimeData
		if err := e.cache.Get(key, &data); err != nil {
			return 0, fmt.Errorf("clean data for parameter %d not found in cache", sourceID)
		}
		if data == nil || data.ProcessedValue == nil {
			return 0, fmt.Errorf("clean data for parameter %d is nil", sourceID)
		}
		switch v := data.ProcessedValue.(type) {
		case float64:
			return v, nil
		case int:
			return float64(v), nil
		case int32:
			return float64(v), nil
		case int64:
			return float64(v), nil
		default:
			return 0, fmt.Errorf("unsupported processed value type for parameter %d", sourceID)
		}
	default:
		return 0, fmt.Errorf("unsupported left_source_type: %s", sourceType)
	}
}

// executeActions mengeksekusi semua aksi bypass secara sequential berdasarkan action_order.
func (e *BypassEngine) executeActions(ctx context.Context, b *domain.Bypass) error {
	for _, a := range b.Actions {
		// Tentukan nilai yang akan di-write
		var valueToWrite *float64
		if a.ValueSourceType == "static" {
			if a.Value == nil {
				return fmt.Errorf("bypass_action %d: static value is nil", a.ID)
			}
			valueToWrite = a.Value
		} else if a.ValueSourceType == "set_point" {
			if a.SourceSetPointID == nil {
				return fmt.Errorf("bypass_action %d: source_set_point_id is nil", a.ID)
			}
			sp, err := e.setPointRepo.GetByID(*a.SourceSetPointID)
			if err != nil {
				return fmt.Errorf("failed to get set-point %d for bypass_action %d: %w", *a.SourceSetPointID, a.ID, err)
			}
			v := sp.Value
			valueToWrite = &v
		} else {
			return fmt.Errorf("bypass_action %d: invalid value_source_type %s", a.ID, a.ValueSourceType)
		}

		// Check if action writes to parameter or set-point
		if a.WriteSetPointID != nil {
			// Write to set-point
			if e.setPointService == nil {
				return fmt.Errorf("bypass_action %d: set-point service not configured", a.ID)
			}
			if valueToWrite == nil {
				return fmt.Errorf("bypass_action %d: value is nil", a.ID)
			}

			if err := e.setPointService.UpdateValue(ctx, *a.WriteSetPointID, *valueToWrite); err != nil {
				return fmt.Errorf("failed to update set-point %d for bypass_action %d: %w", *a.WriteSetPointID, a.ID, err)
			}
		} else if a.WriteParameterID != 0 {
			// Write to parameter
			param, err := e.paramRepo.GetByID(a.WriteParameterID)
			if err != nil {
				return fmt.Errorf("failed to get write parameter %d: %w", a.WriteParameterID, err)
			}

			// Validasi parameter boleh di-write
			if param.Direction != "WRITE" && param.Direction != "READ_WRITE" {
				return fmt.Errorf("parameter %d is not writable", a.WriteParameterID)
			}

			// TODO: pause polling pada koneksi terkait sebelum write, resume setelahnya

			// Eksekusi write via WriteService
			writeReq := WriteRequest{
				Mode:  "MANUAL",
				Value: valueToWrite,
			}

			ctxWrite, cancel := context.WithTimeout(ctx, 5*time.Second)
			result, err := e.writeService.Write(ctxWrite, a.WriteParameterID, writeReq)
			cancel()
			if err != nil {
				return fmt.Errorf("write service error for bypass_action %d: %w", a.ID, err)
			}
			if !result.Success {
				return fmt.Errorf("write failed for bypass_action %d: %s", a.ID, result.Error)
			}
		} else {
			return fmt.Errorf("bypass_action %d: either write_parameter_id or write_set_point_id must be set", a.ID)
		}
	}

	return nil
}

// evaluateNumericCondition membandingkan dua nilai float dengan operator sederhana.
func evaluateNumericCondition(left float64, operator string, right float64) bool {
	switch operator {
	case ">":
		return left > right
	case "<":
		return left < right
	case ">=":
		return left >= right
	case "<=":
		return left <= right
	case "=", "==":
		return left == right
	default:
		// Operator tidak dikenali, anggap false supaya aman
		return false
	}
}
