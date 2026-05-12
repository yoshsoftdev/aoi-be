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

func (s *IMCConfigService) CreateOrUpdate(cardKey string, sourceType string, parameterID *uint, setPointID *uint) error {
	if cardKey == "" {
		return fmt.Errorf("card_key is required")
	}
	if sourceType == "" {
		sourceType = "parameter"
	}
	if sourceType != "parameter" && sourceType != "set_point" {
		return fmt.Errorf("source_type must be 'parameter' or 'set_point'")
	}
	if sourceType == "parameter" {
		setPointID = nil
	} else {
		parameterID = nil
	}
	
	config := &domain.IMCParameterConfig{
		CardKey:     cardKey,
		SourceType:  sourceType,
		ParameterID: parameterID,
		SetPointID:  setPointID,
	}
	
	return s.repo.CreateOrUpdate(config)
}

func (s *IMCConfigService) Delete(cardKey string) error {
	return s.repo.Delete(cardKey)
}
