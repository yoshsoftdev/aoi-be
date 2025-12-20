package scheduler

import (
	"context"
	"fmt"
	"time"

	"aoi-backend/internal/core/database"
	"aoi-backend/internal/domain"
	"aoi-backend/internal/worker"

	"github.com/robfig/cron/v3"
	"gorm.io/gorm"
)

type SchedulerManager struct {
	cron            *cron.Cron
	jobs            map[string]cron.EntryID
	modbusRTUWorker *worker.ModbusRTUWorker
	modbusTCPWorker *worker.ModbusTCPWorker
	snmpWorker      *worker.SNMPWorker
	db              *gorm.DB
}

func NewSchedulerManager(
	modbusRTUWorker *worker.ModbusRTUWorker,
	modbusTCPWorker *worker.ModbusTCPWorker,
	snmpWorker *worker.SNMPWorker,
) *SchedulerManager {
	return &SchedulerManager{
		cron:            cron.New(cron.WithSeconds()),
		jobs:            make(map[string]cron.EntryID),
		modbusRTUWorker: modbusRTUWorker,
		modbusTCPWorker: modbusTCPWorker,
		snmpWorker:      snmpWorker,
		db:              database.GetDB(),
	}
}

func (s *SchedulerManager) Start() {
	s.cron.Start()
	s.loadAndScheduleAll()
}

func (s *SchedulerManager) Stop() {
	s.cron.Stop()
}

func (s *SchedulerManager) loadAndScheduleAll() {
	// CRITICAL: Only schedule READ parameters (direction='READ')
	// WRITE parameters are command/on-demand and should NOT be scheduled

	// Load Modbus RTU with Parameter and Device - ONLY READ parameters
	var modbusRTUConfigs []domain.ModbusRTU
	s.db.Preload("Parameter.Device").
		Joins("JOIN parameters ON parameters.id = modbus_rtu.id_parameter").
		Where("parameters.direction = ? OR (parameters.direction = '' AND parameters.read_write = 0)", "READ").
		Find(&modbusRTUConfigs)
	for _, config := range modbusRTUConfigs {
		// Double-check: skip if direction is WRITE
		if config.Parameter.Direction == "WRITE" {
			continue
		}
		pollingMs := s.getPollingMs(config.PollingMs, &config.Parameter)
		if pollingMs > 0 {
			s.ScheduleModbusRTU(config.ID, pollingMs)
		}
	}

	// Load Modbus TCP with Parameter and Device - ONLY READ parameters
	var modbusTCPConfigs []domain.ModbusTCP
	s.db.Preload("Parameter.Device").
		Joins("JOIN parameters ON parameters.id = modbus_tcp.id_parameter").
		Where("parameters.direction = ? OR (parameters.direction = '' AND parameters.read_write = 0)", "READ").
		Find(&modbusTCPConfigs)
	for _, config := range modbusTCPConfigs {
		// Double-check: skip if direction is WRITE
		if config.Parameter.Direction == "WRITE" {
			continue
		}
		pollingMs := s.getPollingMs(config.PollingMs, &config.Parameter)
		if pollingMs > 0 {
			s.ScheduleModbusTCP(config.ID, pollingMs)
		}
	}

	// Load SNMP Config with Parameter and Device - ONLY READ parameters
	var snmpConfigs []domain.SNMPConfig
	s.db.Preload("Parameter.Device").
		Joins("JOIN parameters ON parameters.id = snmp_config.id_parameter").
		Where("parameters.direction = ? OR (parameters.direction = '' AND parameters.read_write = 0)", "READ").
		Find(&snmpConfigs)
	for _, config := range snmpConfigs {
		// Double-check: skip if direction is WRITE
		if config.Parameter.Direction == "WRITE" {
			continue
		}
		pollingMs := s.getPollingMs(&config.PollingMs, &config.Parameter)
		if pollingMs > 0 {
			s.ScheduleSNMP(config.ID, pollingMs)
		}
	}
}

// getPollingMs returns polling interval in milliseconds
// If parameter-level polling is set, use it; otherwise use device-level polling
func (s *SchedulerManager) getPollingMs(paramPollingMs *int, param *domain.Parameter) int {
	if paramPollingMs != nil && *paramPollingMs > 0 {
		return *paramPollingMs
	}
	// Use device polling if parameter-level is not set
	if param != nil && param.Device.PollingMs > 0 {
		return param.Device.PollingMs
	}
	return 1000 // default
}

func (s *SchedulerManager) ScheduleModbusRTU(configID uint, pollingMs int) {
	jobID := fmt.Sprintf("modbus_rtu_%d", configID)

	// Remove existing job if any
	if entryID, exists := s.jobs[jobID]; exists {
		s.cron.Remove(entryID)
	}

	// Calculate cron expression (every pollingMs milliseconds)
	interval := time.Duration(pollingMs) * time.Millisecond
	cronExpr := fmt.Sprintf("@every %s", interval)

	entryID, err := s.cron.AddFunc(cronExpr, func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := s.modbusRTUWorker.Execute(ctx, configID); err != nil {
			fmt.Printf("Error executing Modbus RTU job %d: %v\n", configID, err)
		}
	})

	if err != nil {
		fmt.Printf("Failed to schedule Modbus RTU job %d: %v\n", configID, err)
		return
	}

	s.jobs[jobID] = entryID
}

func (s *SchedulerManager) ScheduleModbusTCP(configID uint, pollingMs int) {
	jobID := fmt.Sprintf("modbus_tcp_%d", configID)

	// Remove existing job if any
	if entryID, exists := s.jobs[jobID]; exists {
		s.cron.Remove(entryID)
	}

	// Calculate cron expression
	interval := time.Duration(pollingMs) * time.Millisecond
	cronExpr := fmt.Sprintf("@every %s", interval)

	entryID, err := s.cron.AddFunc(cronExpr, func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := s.modbusTCPWorker.Execute(ctx, configID); err != nil {
			fmt.Printf("Error executing Modbus TCP job %d: %v\n", configID, err)
		}
	})

	if err != nil {
		fmt.Printf("Failed to schedule Modbus TCP job %d: %v\n", configID, err)
		return
	}

	s.jobs[jobID] = entryID
}

func (s *SchedulerManager) ScheduleSNMP(configID uint, pollingMs int) {
	jobID := fmt.Sprintf("snmp_%d", configID)

	// Remove existing job if any
	if entryID, exists := s.jobs[jobID]; exists {
		s.cron.Remove(entryID)
	}

	// Calculate cron expression
	interval := time.Duration(pollingMs) * time.Millisecond
	cronExpr := fmt.Sprintf("@every %s", interval)

	entryID, err := s.cron.AddFunc(cronExpr, func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := s.snmpWorker.Execute(ctx, configID); err != nil {
			fmt.Printf("Error executing SNMP job %d: %v\n", configID, err)
		}
	})

	if err != nil {
		fmt.Printf("Failed to schedule SNMP job %d: %v\n", configID, err)
		return
	}

	s.jobs[jobID] = entryID
}

func (s *SchedulerManager) RemoveJob(jobID string) {
	if entryID, exists := s.jobs[jobID]; exists {
		s.cron.Remove(entryID)
		delete(s.jobs, jobID)
	}
}

// Reload reloads all scheduled jobs from database
// This should be called after parameter/modbus/snmp config changes
func (s *SchedulerManager) Reload() {
	// Remove all existing jobs
	for jobID, entryID := range s.jobs {
		s.cron.Remove(entryID)
		delete(s.jobs, jobID)
	}

	// Reload and schedule all configurations
	s.loadAndScheduleAll()
}

// TriggerImmediateExecution triggers immediate execution for a parameter
// This is useful after creating new parameters to get data immediately
func (s *SchedulerManager) TriggerImmediateExecution(parameterID uint, paramType string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	switch paramType {
	case "modbus_rtu":
		var modbusRTU domain.ModbusRTU
		if err := s.db.Where("id_parameter = ?", parameterID).First(&modbusRTU).Error; err == nil {
			if err := s.modbusRTUWorker.Execute(ctx, modbusRTU.ID); err != nil {
				fmt.Printf("Failed to execute Modbus RTU for parameter %d: %v\n", parameterID, err)
			}
		}
	case "modbus_tcp":
		var modbusTCP domain.ModbusTCP
		if err := s.db.Where("id_parameter = ?", parameterID).First(&modbusTCP).Error; err == nil {
			if err := s.modbusTCPWorker.Execute(ctx, modbusTCP.ID); err != nil {
				fmt.Printf("Failed to execute Modbus TCP for parameter %d: %v\n", parameterID, err)
			}
		}
	case "snmp":
		var snmpConfig domain.SNMPConfig
		if err := s.db.Where("id_parameter = ?", parameterID).First(&snmpConfig).Error; err == nil {
			if err := s.snmpWorker.Execute(ctx, snmpConfig.ID); err != nil {
				fmt.Printf("Failed to execute SNMP for parameter %d: %v\n", parameterID, err)
			}
		}
	}
}
