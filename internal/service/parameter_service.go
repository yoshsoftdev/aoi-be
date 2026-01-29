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
	// read_write: 0=READ, 1=WRITE, 2=READ_WRITE
	if param.Direction == "" {
		if param.ReadWrite == 1 {
			param.Direction = "WRITE"
		} else if param.ReadWrite == 2 {
			param.Direction = "READ_WRITE"
		} else {
			param.Direction = "READ" // Default to READ for read_write=0 or any other value
		}
	}

	// Validate direction
	if param.Direction != "READ" && param.Direction != "WRITE" && param.Direction != "READ_WRITE" {
		return errors.NewValidationError("Invalid direction. Must be 'READ', 'WRITE', or 'READ_WRITE'", param.Direction)
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
	// Hanya workflow yang bisa mencegah delete - jika digunakan di workflow, REJECT delete
	isUsed, workflowMap, err := s.workflowRepo.CheckParameterUsageWithNames(id)
	if err != nil {
		log.Errorf("[PARAMETER DELETE] Failed to check workflow usage: %v", err)
		return fmt.Errorf("failed to check workflow dependency: %w", err)
	}

	if isUsed {
		// Build workflow names list for error message
		workflowNames := make([]string, 0, len(workflowMap))
		for _, name := range workflowMap {
			workflowNames = append(workflowNames, name)
		}

		log.Warnf("[PARAMETER DELETE] ❌ REJECTED: Parameter %d is used in %d workflow(s): %v", id, len(workflowMap), workflowNames)
		return errors.NewAppError(
			http.StatusConflict,
			"PARAMETER_IN_USE",
			fmt.Sprintf("Parameter is used in %d workflow(s) and cannot be deleted. Workflow(s): %v", len(workflowMap), workflowNames),
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
	// IMPORTANT: Delete in correct order to avoid foreign key constraint violations
	// Delete child tables first, then parent table
	db := database.GetDB()

	// Get raw SQL connection to disable foreign keys at connection level
	sqlDB, err := db.DB()
	if err != nil {
		return fmt.Errorf("failed to get database connection: %w", err)
	}

	// Disable foreign keys at connection level (before transaction)
	if _, err := sqlDB.Exec("PRAGMA foreign_keys = OFF"); err != nil {
		log.Warnf("[PARAMETER DELETE] Warning: Failed to disable foreign keys: %v", err)
	} else {
		log.Debugf("[PARAMETER DELETE] Foreign key checks disabled at connection level")
	}

	// Re-enable foreign keys after transaction (defer)
	defer func() {
		if _, err := sqlDB.Exec("PRAGMA foreign_keys = ON"); err != nil {
			log.Warnf("[PARAMETER DELETE] Warning: Failed to re-enable foreign keys: %v", err)
		}
	}()

	err = db.Transaction(func(tx *gorm.DB) error {
		// Step 1: Delete all child records that reference this parameter
		// Use raw SQL to ensure all records are deleted, even if foreign key constraint has issues
		// Delete in order: most dependent -> least dependent

		// 1a. Delete Parameter Formula Var first (depends on parameter_formula and parameter)
		result := tx.Exec("DELETE FROM parameter_formula_var WHERE source_parameter_id = ?", id)
		if result.Error != nil {
			log.Warnf("[PARAMETER DELETE] Warning deleting formula variables: %v", result.Error)
		} else {
			log.Debugf("[PARAMETER DELETE] Deleted %d formula variables", result.RowsAffected)
		}

		// 1b. Delete Parameter Formula (depends on parameter)
		result = tx.Exec("DELETE FROM parameter_formula WHERE parameter_id = ?", id)
		if result.Error != nil {
			log.Warnf("[PARAMETER DELETE] Warning deleting parameter formula: %v", result.Error)
		} else {
			log.Debugf("[PARAMETER DELETE] Deleted %d parameter formulas", result.RowsAffected)
		}

		// 1c. Delete Workflow Conditions (depends on parameter)
		// NOTE: We delete workflow_condition but keep the workflow itself (as per user request)
		result = tx.Exec("DELETE FROM workflow_condition WHERE parameter_id = ?", id)
		if result.Error != nil {
			log.Warnf("[PARAMETER DELETE] Warning deleting workflow conditions: %v", result.Error)
		} else {
			log.Infof("[PARAMETER DELETE] Deleted %d workflow conditions", result.RowsAffected)
		}

		// 1d. Delete Workflow Actions (depends on parameter)
		// NOTE: We delete workflow_action but keep the workflow itself (as per user request)
		result = tx.Exec("DELETE FROM workflow_action WHERE parameter_id = ?", id)
		if result.Error != nil {
			log.Warnf("[PARAMETER DELETE] Warning deleting workflow actions: %v", result.Error)
		} else {
			log.Infof("[PARAMETER DELETE] Deleted %d workflow actions", result.RowsAffected)
		}

		// 1e. Delete Current Alarm (depends on parameter)
		result = tx.Exec("DELETE FROM current_alarm WHERE parameter_id = ?", id)
		if result.Error != nil {
			log.Warnf("[PARAMETER DELETE] Warning deleting current alarm: %v", result.Error)
		} else {
			log.Debugf("[PARAMETER DELETE] Deleted %d current alarms", result.RowsAffected)
		}

		// 1f. Delete Threshold (depends on parameter)
		result = tx.Exec("DELETE FROM threshold WHERE id_parameter = ?", id)
		if result.Error != nil {
			log.Warnf("[PARAMETER DELETE] Warning deleting threshold: %v", result.Error)
		} else {
			log.Debugf("[PARAMETER DELETE] Deleted %d thresholds", result.RowsAffected)
		}

		// 1g. Delete Parameter Presets (depends on parameter)
		result = tx.Exec("DELETE FROM parameter_preset WHERE parameter_id = ?", id)
		if result.Error != nil {
			log.Warnf("[PARAMETER DELETE] Warning deleting parameter presets: %v", result.Error)
		} else {
			log.Debugf("[PARAMETER DELETE] Deleted %d parameter presets", result.RowsAffected)
		}

		// Step 2: Delete protocol-specific configs
		// 2a. Delete Modbus RTU config
		result = tx.Exec("DELETE FROM modbus_rtu WHERE id_parameter = ?", id)
		if result.Error != nil {
			log.Warnf("[PARAMETER DELETE] Warning deleting Modbus RTU config: %v", result.Error)
		} else {
			log.Debugf("[PARAMETER DELETE] Deleted %d Modbus RTU configs", result.RowsAffected)
		}

		// 2b. Delete Modbus TCP config
		result = tx.Exec("DELETE FROM modbus_tcp WHERE id_parameter = ?", id)
		if result.Error != nil {
			log.Warnf("[PARAMETER DELETE] Warning deleting Modbus TCP config: %v", result.Error)
		} else {
			log.Debugf("[PARAMETER DELETE] Deleted %d Modbus TCP configs", result.RowsAffected)
		}

		// 2c. Delete SNMP config
		result = tx.Exec("DELETE FROM snmp_config WHERE id_parameter = ?", id)
		if result.Error != nil {
			log.Warnf("[PARAMETER DELETE] Warning deleting SNMP config: %v", result.Error)
		} else {
			log.Debugf("[PARAMETER DELETE] Deleted %d SNMP configs", result.RowsAffected)
		}

		// 2d. Delete Analog Input config
		result = tx.Exec("DELETE FROM analog_input WHERE id_parameter = ?", id)
		if result.Error != nil {
			log.Warnf("[PARAMETER DELETE] Warning deleting Analog Input config: %v", result.Error)
		} else {
			log.Debugf("[PARAMETER DELETE] Deleted %d Analog Input configs", result.RowsAffected)
		}

		// 2e. Delete Analog Output config
		result = tx.Exec("DELETE FROM analog_output WHERE id_parameter = ?", id)
		if result.Error != nil {
			log.Warnf("[PARAMETER DELETE] Warning deleting Analog Output config: %v", result.Error)
		} else {
			log.Debugf("[PARAMETER DELETE] Deleted %d Analog Output configs", result.RowsAffected)
		}

		// 2f. Delete Digital Input config
		result = tx.Exec("DELETE FROM digital_input WHERE id_parameter = ?", id)
		if result.Error != nil {
			log.Warnf("[PARAMETER DELETE] Warning deleting Digital Input config: %v", result.Error)
		} else {
			log.Debugf("[PARAMETER DELETE] Deleted %d Digital Input configs", result.RowsAffected)
		}

		// 2g. Delete Digital Output config
		result = tx.Exec("DELETE FROM digital_output WHERE id_parameter = ?", id)
		if result.Error != nil {
			log.Warnf("[PARAMETER DELETE] Warning deleting Digital Output config: %v", result.Error)
		} else {
			log.Debugf("[PARAMETER DELETE] Deleted %d Digital Output configs", result.RowsAffected)
		}

		// Step 3: Verify all child records are deleted (for debugging)
		var remainingCount int64
		tablesToVerify := []struct {
			name  string
			field string
		}{
			{"parameter_formula_var", "source_parameter_id"},
			{"parameter_formula", "parameter_id"},
			{"workflow_condition", "parameter_id"},
			{"workflow_action", "parameter_id"},
			{"current_alarm", "parameter_id"},
			{"threshold", "id_parameter"},
			{"parameter_preset", "parameter_id"},
			{"modbus_rtu", "id_parameter"},
			{"modbus_tcp", "id_parameter"},
			{"snmp_config", "id_parameter"},
			{"analog_input", "id_parameter"},
			{"analog_output", "id_parameter"},
			{"digital_input", "id_parameter"},
			{"digital_output", "id_parameter"},
		}

		for _, table := range tablesToVerify {
			if err := tx.Raw(fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s = ?", table.name, table.field), id).Scan(&remainingCount).Error; err == nil {
				if remainingCount > 0 {
					log.Warnf("[PARAMETER DELETE] ⚠️  Found %d remaining records in %s.%s - attempting force delete", remainingCount, table.name, table.field)
					// Force delete with raw SQL
					if err := tx.Exec(fmt.Sprintf("DELETE FROM %s WHERE %s = ?", table.name, table.field), id).Error; err != nil {
						log.Errorf("[PARAMETER DELETE] ❌ Failed to force delete from %s: %v", table.name, err)
					}
				}
			}
		}

		// Step 4: Finally, delete the parameter itself (foreign keys OFF at connection level)
		log.Infof("[PARAMETER DELETE] Deleting parameter: id=%d", id)
		result = tx.Exec("DELETE FROM parameters WHERE id = ?", id)
		if result.Error != nil {
			// Get more detailed error information
			log.Errorf("[PARAMETER DELETE] Failed to delete parameter %d: %v", id, result.Error)
			return fmt.Errorf("failed to delete parameter: %w", result.Error)
		}
		if result.RowsAffected == 0 {
			log.Warnf("[PARAMETER DELETE] No rows affected when deleting parameter %d (may not exist)", id)
		} else {
			log.Infof("[PARAMETER DELETE] Successfully deleted parameter %d", id)
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
