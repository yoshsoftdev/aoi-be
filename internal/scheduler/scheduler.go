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
	cron        *cron.Cron
	jobs        map[string]cron.EntryID
	modbusRTUWorker *worker.ModbusRTUWorker
	modbusTCPWorker *worker.ModbusTCPWorker
	snmpWorker      *worker.SNMPWorker
	db          *gorm.DB
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
	// Load Modbus RTU with Parameter and Device
	var modbusRTUConfigs []domain.ModbusRTU
	s.db.Preload("Parameter.Device").Find(&modbusRTUConfigs)
	for _, config := range modbusRTUConfigs {
		pollingMs := s.getPollingMs(config.PollingMs, &config.Parameter)
		if pollingMs > 0 {
			s.ScheduleModbusRTU(config.ID, pollingMs)
		}
	}

	// Load Modbus TCP with Parameter and Device
	var modbusTCPConfigs []domain.ModbusTCP
	s.db.Preload("Parameter.Device").Find(&modbusTCPConfigs)
	for _, config := range modbusTCPConfigs {
		pollingMs := s.getPollingMs(config.PollingMs, &config.Parameter)
		if pollingMs > 0 {
			s.ScheduleModbusTCP(config.ID, pollingMs)
		}
	}

	// Load SNMP Config with Parameter and Device
	var snmpConfigs []domain.SNMPConfig
	s.db.Preload("Parameter.Device").Find(&snmpConfigs)
	for _, config := range snmpConfigs {
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

