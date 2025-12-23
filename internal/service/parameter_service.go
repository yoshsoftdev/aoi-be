package service

import (
	"fmt"
	"net/http"

	"aoi-backend/internal/core/cache"
	"aoi-backend/internal/core/database"
	"aoi-backend/internal/core/errors"
	"aoi-backend/internal/core/logger"
	"aoi-backend/internal/domain"
	"aoi-backend/internal/repository"

	"gorm.io/gorm"
)

type ParameterService interface {
	Create(param *domain.Parameter) error
	GetByID(id uint) (*domain.Parameter, error)
	GetAll() ([]domain.Parameter, error)
	Update(id uint, param *domain.Parameter) error
	Delete(id uint) error
}

type parameterService struct {
	repo          repository.ParameterRepository
	workflowRepo  repository.WorkflowRepository
	formulaRepo   repository.FormulaRepository
	modbusRTURepo repository.ModbusRTURepository
	modbusTCPRepo repository.ModbusTCPRepository
	snmpRepo      repository.SNMPRepository
	presetRepo    repository.PresetRepository
	cache         cache.Cache
}

func NewParameterService(
	repo repository.ParameterRepository,
	workflowRepo repository.WorkflowRepository,
	formulaRepo repository.FormulaRepository,
	modbusRTURepo repository.ModbusRTURepository,
	modbusTCPRepo repository.ModbusTCPRepository,
	snmpRepo repository.SNMPRepository,
	presetRepo repository.PresetRepository,
	cacheInstance cache.Cache,
) ParameterService {
	return &parameterService{
		repo:          repo,
		workflowRepo:  workflowRepo,
		formulaRepo:   formulaRepo,
		modbusRTURepo: modbusRTURepo,
		modbusTCPRepo: modbusTCPRepo,
		snmpRepo:      snmpRepo,
		presetRepo:    presetRepo,
		cache:         cacheInstance,
	}
}

func (s *parameterService) Create(param *domain.Parameter) error {
	// Validate type
	validTypes := map[string]bool{
		"modbus_rtu":     true,
		"modbus_tcp":     true,
		"analog_input":   true,
		"analog_output":  true,
		"digital_input":  true,
		"digital_output": true,
		"snmp":           true,
	}

	if !validTypes[param.Type] {
		return errors.NewValidationError("Invalid parameter type", param.Type)
	}

	// Validate device_id exists
	if param.DeviceID == 0 {
		return errors.NewValidationError("Device ID is required", "")
	}

	// Set direction based on read_write if direction is not explicitly set
	// CRITICAL: direction determines if parameter is scheduled (READ) or command-based (WRITE)
	if param.Direction == "" {
		if param.ReadWrite {
			param.Direction = "WRITE"
		} else {
			param.Direction = "READ"
		}
	}

	// Validate direction
	if param.Direction != "READ" && param.Direction != "WRITE" {
		return errors.NewValidationError("Invalid direction. Must be 'READ' or 'WRITE'", param.Direction)
	}

	return s.repo.Create(param)
}

func (s *parameterService) GetByID(id uint) (*domain.Parameter, error) {
	param, err := s.repo.GetByID(id)
	if err != nil {
		return nil, errors.NewNotFoundError("Parameter not found")
	}
	return param, nil
}

func (s *parameterService) GetAll() ([]domain.Parameter, error) {
	return s.repo.GetAll()
}

func (s *parameterService) Update(id uint, param *domain.Parameter) error {
	existing, err := s.repo.GetByID(id)
	if err != nil {
		return errors.NewNotFoundError("Parameter not found")
	}

	param.ID = existing.ID
	return s.repo.Update(param)
}

// Delete deletes a parameter with full validation and cascade cleanup
// Returns error with code PARAMETER_IN_USE if parameter is used in workflow
func (s *parameterService) Delete(id uint) error {
	log := logger.GetLogger()
	log.Infof("[PARAMETER DELETE] Starting delete for parameter_id=%d", id)

	// 1. Check if parameter exists
	param, err := s.repo.GetByID(id)
	if err != nil {
		return errors.NewNotFoundError("Parameter not found")
	}

	log.Infof("[PARAMETER DELETE] Found parameter: id=%d, nama=%s, type=%s", param.ID, param.Nama, param.Type)

	// 2. VALIDASI: Check if parameter is used in workflow
	isUsed, workflowIDs, err := s.workflowRepo.CheckParameterUsage(id)
	if err != nil {
		log.Errorf("[PARAMETER DELETE] Failed to check workflow usage: %v", err)
		return fmt.Errorf("failed to check workflow dependency: %w", err)
	}

	if isUsed {
		log.Warnf("[PARAMETER DELETE] ❌ REJECTED: Parameter %d is used in %d workflow(s): %v", id, len(workflowIDs), workflowIDs)
		return errors.NewAppError(
			http.StatusConflict,
			"PARAMETER_IN_USE",
			"Parameter is used in workflow and cannot be deleted",
		)
	}

	log.Infof("[PARAMETER DELETE] ✅ Workflow validation passed: parameter not used in workflows")

	// 3. Check formula dependency (allowed, but log warning)
	formulaDependants, err := s.formulaRepo.GetDependants(id)
	if err != nil {
		log.Warnf("[PARAMETER DELETE] Failed to check formula dependency: %v", err)
	} else if len(formulaDependants) > 0 {
		log.Warnf("[PARAMETER DELETE] ⚠️  Parameter %d is used as source in %d formula(s): %v. Formulas will become invalid.", id, len(formulaDependants), formulaDependants)
	}

	// 4. ATOMIC TRANSACTION: Delete parameter and all related configs
	db := database.GetDB()
	err = db.Transaction(func(tx *gorm.DB) error {
		// 4a. Delete Modbus RTU config (if exists)
		var modbusRTU domain.ModbusRTU
		if err := tx.Where("id_parameter = ?", id).First(&modbusRTU).Error; err == nil {
			log.Infof("[PARAMETER DELETE] Deleting Modbus RTU config: id=%d", modbusRTU.ID)
			if err := tx.Delete(&modbusRTU).Error; err != nil {
				return fmt.Errorf("failed to delete Modbus RTU config: %w", err)
			}
		}

		// 4b. Delete Modbus TCP config (if exists)
		var modbusTCP domain.ModbusTCP
		if err := tx.Where("id_parameter = ?", id).First(&modbusTCP).Error; err == nil {
			log.Infof("[PARAMETER DELETE] Deleting Modbus TCP config: id=%d", modbusTCP.ID)
			if err := tx.Delete(&modbusTCP).Error; err != nil {
				return fmt.Errorf("failed to delete Modbus TCP config: %w", err)
			}
		}

		// 4c. Delete SNMP config (if exists)
		var snmpConfig domain.SNMPConfig
		if err := tx.Where("id_parameter = ?", id).First(&snmpConfig).Error; err == nil {
			log.Infof("[PARAMETER DELETE] Deleting SNMP config: id=%d", snmpConfig.ID)
			if err := tx.Delete(&snmpConfig).Error; err != nil {
				return fmt.Errorf("failed to delete SNMP config: %w", err)
			}
		}

		// 4d. Delete Analog Input/Output configs (if exists)
		var analogInput domain.AnalogInput
		if err := tx.Where("id_parameter = ?", id).First(&analogInput).Error; err == nil {
			log.Infof("[PARAMETER DELETE] Deleting Analog Input config: id=%d", analogInput.ID)
			if err := tx.Delete(&analogInput).Error; err != nil {
				return fmt.Errorf("failed to delete Analog Input config: %w", err)
			}
		}

		var analogOutput domain.AnalogOutput
		if err := tx.Where("id_parameter = ?", id).First(&analogOutput).Error; err == nil {
			log.Infof("[PARAMETER DELETE] Deleting Analog Output config: id=%d", analogOutput.ID)
			if err := tx.Delete(&analogOutput).Error; err != nil {
				return fmt.Errorf("failed to delete Analog Output config: %w", err)
			}
		}

		// 4e. Delete Digital Input/Output configs (if exists)
		var digitalInput domain.DigitalInput
		if err := tx.Where("id_parameter = ?", id).First(&digitalInput).Error; err == nil {
			log.Infof("[PARAMETER DELETE] Deleting Digital Input config: id=%d", digitalInput.ID)
			if err := tx.Delete(&digitalInput).Error; err != nil {
				return fmt.Errorf("failed to delete Digital Input config: %w", err)
			}
		}

		var digitalOutput domain.DigitalOutput
		if err := tx.Where("id_parameter = ?", id).First(&digitalOutput).Error; err == nil {
			log.Infof("[PARAMETER DELETE] Deleting Digital Output config: id=%d", digitalOutput.ID)
			if err := tx.Delete(&digitalOutput).Error; err != nil {
				return fmt.Errorf("failed to delete Digital Output config: %w", err)
			}
		}

		// 4f. Delete Parameter Presets (if exists)
		if err := tx.Where("parameter_id = ?", id).Delete(&domain.ParameterPreset{}).Error; err != nil {
			return fmt.Errorf("failed to delete parameter presets: %w", err)
		}

		// 4g. Delete Parameter Formula Var (if parameter is used as source in formula)
		// Note: We don't delete the formula itself, just the variable reference
		// Formula will become invalid but remains in database
		if err := tx.Where("source_parameter_id = ?", id).Delete(&domain.ParameterFormulaVar{}).Error; err != nil {
			return fmt.Errorf("failed to delete formula variables: %w", err)
		}

		// 4h. Finally, delete the parameter itself
		log.Infof("[PARAMETER DELETE] Deleting parameter: id=%d", id)
		if err := tx.Delete(&domain.Parameter{}, id).Error; err != nil {
			return fmt.Errorf("failed to delete parameter: %w", err)
		}

		return nil
	})

	if err != nil {
		log.Errorf("[PARAMETER DELETE] ❌ Transaction failed: %v", err)
		return fmt.Errorf("failed to delete parameter: %w", err)
	}

	// 5. Clean cache entries (outside transaction, cache operations are independent)
	if s.cache != nil {
		cacheKeys := []string{
			fmt.Sprintf("parameter:raw:%d", id),
			fmt.Sprintf("parameter:clean:%d", id),
			fmt.Sprintf("parameter:%d", id), // Legacy key
		}

		for _, key := range cacheKeys {
			if err := s.cache.Delete(key); err != nil {
				log.Warnf("[PARAMETER DELETE] Failed to delete cache key %s: %v", key, err)
			} else {
				log.Debugf("[PARAMETER DELETE] Deleted cache key: %s", key)
			}
		}
	}

	log.Infof("[PARAMETER DELETE] ✅ Successfully deleted parameter_id=%d", id)
	return nil
}
