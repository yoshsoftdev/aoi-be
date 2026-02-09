package service

import (
	"aoi-backend/internal/domain"
	"aoi-backend/internal/repository"
)

// BypassService membungkus operasi repository tanpa banyak logika tambahan.
// Logika eksekusi ada di BypassEngine.
type BypassService struct {
	repo       repository.BypassRepository
	setPointRepo repository.SetPointRepository
	paramRepo  repository.ParameterRepository
}

func NewBypassService(
	repo repository.BypassRepository,
	setPointRepo repository.SetPointRepository,
	paramRepo repository.ParameterRepository,
) *BypassService {
	return &BypassService{
		repo:         repo,
		setPointRepo: setPointRepo,
		paramRepo:    paramRepo,
	}
}

func (s *BypassService) GetAll() ([]domain.Bypass, error) {
	return s.repo.GetAll()
}

func (s *BypassService) GetByID(id uint) (*domain.Bypass, error) {
	return s.repo.GetByID(id)
}

func (s *BypassService) Create(b *domain.Bypass) error {
	return s.repo.Create(b)
}

func (s *BypassService) Update(b *domain.Bypass) error {
	return s.repo.Update(b)
}

func (s *BypassService) Delete(id uint) error {
	return s.repo.Delete(id)
}

