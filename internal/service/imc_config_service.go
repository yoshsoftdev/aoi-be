package service

import (
	"aoi-backend/internal/domain"
	"aoi-backend/internal/repository"
	"fmt"
)

type IMCConfigService struct {
	repo repository.IMCConfigRepository
}

func NewIMCConfigService(repo repository.IMCConfigRepository) *IMCConfigService {
	return &IMCConfigService{repo: repo}
}

func (s *IMCConfigService) GetAll() ([]domain.IMCParameterConfig, error) {
	return s.repo.GetAll()
}

func (s *IMCConfigService) GetByCardKey(cardKey string) (*domain.IMCParameterConfig, error) {
	return s.repo.GetByCardKey(cardKey)
}

func (s *IMCConfigService) CreateOrUpdate(cardKey string, parameterID *uint) error {
	if cardKey == "" {
		return fmt.Errorf("card_key is required")
	}
	
	config := &domain.IMCParameterConfig{
		CardKey:     cardKey,
		ParameterID: parameterID,
	}
	
	return s.repo.CreateOrUpdate(config)
}

func (s *IMCConfigService) Delete(cardKey string) error {
	return s.repo.Delete(cardKey)
}
