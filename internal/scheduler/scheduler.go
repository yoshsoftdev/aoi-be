package scheduler

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"aoi-backend/internal/core/database"
	"aoi-backend/internal/core/logger"
	"aoi-backend/internal/domain"
	"aoi-backend/internal/worker"

	"github.com/robfig/cron/v3"
	"gorm.io/gorm"
)

// PollingJob represents a polling job with sorting information
type PollingJob struct {
	ConfigID     uint
	ParameterID  uint
	ConnectionID uint
	SlaveID      int
	JobType      string // "modbus_rtu", "modbus_tcp", "snmp"
	PollingMs    int
	ExecuteFn    func(ctx context.Context) error
}

// SchedulerManager manages polling jobs with per-connection queues
type SchedulerManager struct {
	cron            *cron.Cron
	jobs            map[string]cron.EntryID // Legacy: keep for compatibility
	modbusRTUWorker *worker.ModbusRTUWorker
	modbusTCPWorker *worker.ModbusTCPWorker
	snmpWorker      *worker.SNMPWorker
	db              *gorm.DB

	// New: Per-connection queues
	connectionQueues map[uint]*ConnectionQueue // connection_id -> queue
	queuesMu         sync.RWMutex
}

func NewSchedulerManager(
	modbusRTUWorker *worker.ModbusRTUWorker,
	modbusTCPWorker *worker.ModbusTCPWorker,
	snmpWorker *worker.SNMPWorker,
) *SchedulerManager {
	return &SchedulerManager{
		cron:             cron.New(cron.WithSeconds()),
		jobs:             make(map[string]cron.EntryID),
		modbusRTUWorker:  modbusRTUWorker,
		modbusTCPWorker:  modbusTCPWorker,
		snmpWorker:       snmpWorker,
		db:               database.GetDB(),
		connectionQueues: make(map[uint]*ConnectionQueue),
	}
}

func (s *SchedulerManager) Start() {
	s.cron.Start()
	s.loadAndScheduleAll()
}

func (s *SchedulerManager) Stop() {
	s.cron.Stop()

	// Stop all connection workers
	s.queuesMu.Lock()
	for _, queue := range s.connectionQueues {
		queue.StopWorker()
	}
	s.queuesMu.Unlock()
}

// GetOrCreateQueue gets or creates a connection queue
func (s *SchedulerManager) GetOrCreateQueue(connectionID uint, connectionName string) *ConnectionQueue {
	s.queuesMu.Lock()
	defer s.queuesMu.Unlock()

	if queue, exists := s.connectionQueues[connectionID]; exists {
		return queue
	}

	queue := NewConnectionQueue(connectionID, connectionName, 100) // Queue size 100
	s.connectionQueues[connectionID] = queue
	queue.StartWorker()

	logger.GetLogger().Infof("[SCHEDULER] Created connection queue: connection_id=%d, connection_name=%s", connectionID, connectionName)
	return queue
}

// LockConnection locks a connection (suspends polling)
func (s *SchedulerManager) LockConnection(connectionID uint, reason string) {
	s.queuesMu.RLock()
	queue, exists := s.connectionQueues[connectionID]
	s.queuesMu.RUnlock()

	if exists {
		queue.Lock(reason)
	} else {
		logger.GetLogger().Warnf("[SCHEDULER] Attempted to lock non-existent connection: connection_id=%d", connectionID)
	}
}

// UnlockConnection unlocks a connection (resumes polling)
func (s *SchedulerManager) UnlockConnection(connectionID uint, reason string) {
	s.queuesMu.RLock()
	queue, exists := s.connectionQueues[connectionID]
	s.queuesMu.RUnlock()

	if exists {
		queue.Unlock(reason)
	} else {
		logger.GetLogger().Warnf("[SCHEDULER] Attempted to unlock non-existent connection: connection_id=%d", connectionID)
	}
}

func (s *SchedulerManager) loadAndScheduleAll() {
	log := logger.GetLogger()
	log.Info("[SCHEDULER] Loading and scheduling all polling jobs...")

	// CRITICAL: Only schedule READ parameters (direction='READ')
	// WRITE parameters are command/on-demand and should NOT be scheduled

	// Collect all polling jobs
	var pollingJobs []PollingJob

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
			pollingJobs = append(pollingJobs, PollingJob{
				ConfigID:     config.ID,
				ParameterID:  config.IDParameter,
				ConnectionID: config.Parameter.DeviceID,
				SlaveID:      config.SlaveID,
				JobType:      "modbus_rtu",
				PollingMs:    pollingMs,
				ExecuteFn: func(ctx context.Context) error {
					return s.modbusRTUWorker.Execute(ctx, config.ID)
				},
			})
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
			pollingJobs = append(pollingJobs, PollingJob{
				ConfigID:     config.ID,
				ParameterID:  config.IDParameter,
				ConnectionID: config.Parameter.DeviceID,
				SlaveID:      config.SlaveID,
				JobType:      "modbus_tcp",
				PollingMs:    pollingMs,
				ExecuteFn: func(ctx context.Context) error {
					return s.modbusTCPWorker.Execute(ctx, config.ID)
				},
			})
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
			pollingJobs = append(pollingJobs, PollingJob{
				ConfigID:     config.ID,
				ParameterID:  config.IDParameter,
				ConnectionID: config.Parameter.DeviceID,
				SlaveID:      0, // SNMP doesn't have slave_id, use 0
				JobType:      "snmp",
				PollingMs:    pollingMs,
				ExecuteFn: func(ctx context.Context) error {
					return s.snmpWorker.Execute(ctx, config.ID)
				},
			})
		}
	}

	// Group jobs by connection
	jobsByConnection := make(map[uint][]PollingJob)
	for _, job := range pollingJobs {
		jobsByConnection[job.ConnectionID] = append(jobsByConnection[job.ConnectionID], job)
	}

	// Sort and schedule jobs per connection
	for connectionID, jobs := range jobsByConnection {
		// Sort jobs: slave_id ASC, then parameter.id ASC
		sort.Slice(jobs, func(i, j int) bool {
			if jobs[i].SlaveID != jobs[j].SlaveID {
				return jobs[i].SlaveID < jobs[j].SlaveID
			}
			return jobs[i].ParameterID < jobs[j].ParameterID
		})

		// Get connection name
		var device domain.Device
		if err := s.db.First(&device, connectionID).Error; err != nil {
			log.Warnf("[SCHEDULER] Failed to load device %d: %v", connectionID, err)
			continue
		}

		// Get or create queue for this connection
		queue := s.GetOrCreateQueue(connectionID, device.Nama)

		// Log job order for this connection
		log.Infof("[SCHEDULER] Scheduling %d jobs for connection %d (%s):", len(jobs), connectionID, device.Nama)
		for i, job := range jobs {
			log.Infof("[SCHEDULER]   Job %d: slave_id=%d, parameter_id=%d, config_id=%d, type=%s, polling_ms=%d",
				i+1, job.SlaveID, job.ParameterID, job.ConfigID, job.JobType, job.PollingMs)
		}

		// Schedule each job to enqueue to connection queue at its interval
		for _, job := range jobs {
			s.scheduleJobToQueue(job, queue)
		}
	}

	log.Infof("[SCHEDULER] Scheduled %d polling jobs across %d connections", len(pollingJobs), len(jobsByConnection))
}

// scheduleJobToQueue schedules a job to enqueue to connection queue at regular intervals
func (s *SchedulerManager) scheduleJobToQueue(job PollingJob, queue *ConnectionQueue) {
	jobID := fmt.Sprintf("%s_%d", job.JobType, job.ConfigID)

	// Remove existing job if any
	if entryID, exists := s.jobs[jobID]; exists {
		s.cron.Remove(entryID)
	}

	// Calculate cron expression (every pollingMs milliseconds)
	interval := time.Duration(job.PollingMs) * time.Millisecond
	cronExpr := fmt.Sprintf("@every %s", interval)

	entryID, err := s.cron.AddFunc(cronExpr, func() {
		// Enqueue job to connection queue (will be executed sequentially by worker)
		queue.Enqueue(Job{
			Type:        JobTypePolling,
			ConfigID:    job.ConfigID,
			ParameterID: job.ParameterID,
			SlaveID:     job.SlaveID,
			ExecuteFn:   job.ExecuteFn,
			Priority:    job.SlaveID*10000 + int(job.ParameterID), // For sorting: slave_id * 10000 + parameter_id
		})
	})

	if err != nil {
		logger.GetLogger().Errorf("[SCHEDULER] Failed to schedule job %s: %v", jobID, err)
		return
	}

	s.jobs[jobID] = entryID
	logger.GetLogger().Debugf("[SCHEDULER] Scheduled job to queue: job_id=%s, connection_id=%d, polling_ms=%d",
		jobID, job.ConnectionID, job.PollingMs)
}

// getPollingMs returns polling interval in milliseconds
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

// Legacy methods for backward compatibility (now use queue system)
func (s *SchedulerManager) ScheduleModbusRTU(configID uint, pollingMs int) {
	// Load config to get connection info
	var config domain.ModbusRTU
	if err := s.db.Preload("Parameter.Device").First(&config, configID).Error; err != nil {
		logger.GetLogger().Errorf("[SCHEDULER] Failed to load Modbus RTU config %d: %v", configID, err)
		return
	}

	if config.Parameter.Direction == "WRITE" {
		logger.GetLogger().Debugf("[SCHEDULER] Skipping WRITE parameter: config_id=%d", configID)
		return
	}

	queue := s.GetOrCreateQueue(config.Parameter.DeviceID, config.Parameter.Device.Nama)

	job := PollingJob{
		ConfigID:     configID,
		ParameterID:  config.IDParameter,
		ConnectionID: config.Parameter.DeviceID,
		SlaveID:      config.SlaveID,
		JobType:      "modbus_rtu",
		PollingMs:    pollingMs,
		ExecuteFn: func(ctx context.Context) error {
			return s.modbusRTUWorker.Execute(ctx, configID)
		},
	}

	s.scheduleJobToQueue(job, queue)
}

func (s *SchedulerManager) ScheduleModbusTCP(configID uint, pollingMs int) {
	// Load config to get connection info
	var config domain.ModbusTCP
	if err := s.db.Preload("Parameter.Device").First(&config, configID).Error; err != nil {
		logger.GetLogger().Errorf("[SCHEDULER] Failed to load Modbus TCP config %d: %v", configID, err)
		return
	}

	if config.Parameter.Direction == "WRITE" {
		logger.GetLogger().Debugf("[SCHEDULER] Skipping WRITE parameter: config_id=%d", configID)
		return
	}

	queue := s.GetOrCreateQueue(config.Parameter.DeviceID, config.Parameter.Device.Nama)

	job := PollingJob{
		ConfigID:     configID,
		ParameterID:  config.IDParameter,
		ConnectionID: config.Parameter.DeviceID,
		SlaveID:      config.SlaveID,
		JobType:      "modbus_tcp",
		PollingMs:    pollingMs,
		ExecuteFn: func(ctx context.Context) error {
			return s.modbusTCPWorker.Execute(ctx, configID)
		},
	}

	s.scheduleJobToQueue(job, queue)
}

func (s *SchedulerManager) ScheduleSNMP(configID uint, pollingMs int) {
	// Load config to get connection info
	var config domain.SNMPConfig
	if err := s.db.Preload("Parameter.Device").First(&config, configID).Error; err != nil {
		logger.GetLogger().Errorf("[SCHEDULER] Failed to load SNMP config %d: %v", configID, err)
		return
	}

	if config.Parameter.Direction == "WRITE" {
		logger.GetLogger().Debugf("[SCHEDULER] Skipping WRITE parameter: config_id=%d", configID)
		return
	}

	queue := s.GetOrCreateQueue(config.Parameter.DeviceID, config.Parameter.Device.Nama)

	job := PollingJob{
		ConfigID:     configID,
		ParameterID:  config.IDParameter,
		ConnectionID: config.Parameter.DeviceID,
		SlaveID:      0, // SNMP doesn't have slave_id
		JobType:      "snmp",
		PollingMs:    pollingMs,
		ExecuteFn: func(ctx context.Context) error {
			return s.snmpWorker.Execute(ctx, configID)
		},
	}

	s.scheduleJobToQueue(job, queue)
}

func (s *SchedulerManager) RemoveJob(jobID string) {
	if entryID, exists := s.jobs[jobID]; exists {
		s.cron.Remove(entryID)
		delete(s.jobs, jobID)
	}
}

// Reload reloads all scheduled jobs from database
func (s *SchedulerManager) Reload() {
	log := logger.GetLogger()
	log.Info("[SCHEDULER] Reloading all scheduled jobs...")

	// Remove all existing cron jobs
	for jobID, entryID := range s.jobs {
		s.cron.Remove(entryID)
		delete(s.jobs, jobID)
	}

	// Stop all connection workers
	s.queuesMu.Lock()
	for _, queue := range s.connectionQueues {
		queue.StopWorker()
	}
	s.connectionQueues = make(map[uint]*ConnectionQueue)
	s.queuesMu.Unlock()

	// Reload and schedule all configurations
	s.loadAndScheduleAll()
}

// TriggerImmediateExecution triggers immediate execution for a parameter
// This enqueues to connection queue (respects locking)
func (s *SchedulerManager) TriggerImmediateExecution(parameterID uint, paramType string) {
	var connectionID uint
	var connectionName string
	var configID uint
	var slaveID int
	var executeFn func(ctx context.Context) error

	switch paramType {
	case "modbus_rtu":
		var modbusRTU domain.ModbusRTU
		if err := s.db.Preload("Parameter.Device").Where("id_parameter = ?", parameterID).First(&modbusRTU).Error; err == nil {
			connectionID = modbusRTU.Parameter.DeviceID
			connectionName = modbusRTU.Parameter.Device.Nama
			configID = modbusRTU.ID
			slaveID = modbusRTU.SlaveID
			executeFn = func(ctx context.Context) error {
				return s.modbusRTUWorker.Execute(ctx, modbusRTU.ID)
			}
		} else {
			logger.GetLogger().Warnf("[SCHEDULER] Failed to find Modbus RTU config for parameter %d: %v", parameterID, err)
			return
		}
	case "modbus_tcp":
		var modbusTCP domain.ModbusTCP
		if err := s.db.Preload("Parameter.Device").Where("id_parameter = ?", parameterID).First(&modbusTCP).Error; err == nil {
			connectionID = modbusTCP.Parameter.DeviceID
			connectionName = modbusTCP.Parameter.Device.Nama
			configID = modbusTCP.ID
			slaveID = modbusTCP.SlaveID
			executeFn = func(ctx context.Context) error {
				return s.modbusTCPWorker.Execute(ctx, modbusTCP.ID)
			}
		} else {
			logger.GetLogger().Warnf("[SCHEDULER] Failed to find Modbus TCP config for parameter %d: %v", parameterID, err)
			return
		}
	case "snmp":
		var snmpConfig domain.SNMPConfig
		if err := s.db.Preload("Parameter.Device").Where("id_parameter = ?", parameterID).First(&snmpConfig).Error; err == nil {
			connectionID = snmpConfig.Parameter.DeviceID
			connectionName = snmpConfig.Parameter.Device.Nama
			configID = snmpConfig.ID
			slaveID = 0
			executeFn = func(ctx context.Context) error {
				return s.snmpWorker.Execute(ctx, snmpConfig.ID)
			}
		} else {
			logger.GetLogger().Warnf("[SCHEDULER] Failed to find SNMP config for parameter %d: %v", parameterID, err)
			return
		}
	default:
		logger.GetLogger().Warnf("[SCHEDULER] Unknown parameter type: %s", paramType)
		return
	}

	// Enqueue to connection queue
	queue := s.GetOrCreateQueue(connectionID, connectionName)
	queue.Enqueue(Job{
		Type:        JobTypePolling,
		ConfigID:    configID,
		ParameterID: parameterID,
		SlaveID:     slaveID,
		ExecuteFn:   executeFn,
		Priority:    slaveID*10000 + int(parameterID),
	})
}

// EnqueueTestJob enqueues a test job to connection queue (with lock)
// Caller should call LockConnection before and UnlockConnection after
func (s *SchedulerManager) EnqueueTestJob(connectionID uint, connectionName string, executeFn func(ctx context.Context) error) {
	queue := s.GetOrCreateQueue(connectionID, connectionName)

	queue.Enqueue(Job{
		Type:      JobTypeTest,
		ExecuteFn: executeFn,
	})
}

// EnqueueWriteJob enqueues a write job to connection queue (with lock)
// Caller should call LockConnection before and UnlockConnection after
func (s *SchedulerManager) EnqueueWriteJob(connectionID uint, connectionName string, executeFn func(ctx context.Context) error) {
	queue := s.GetOrCreateQueue(connectionID, connectionName)

	queue.Enqueue(Job{
		Type:      JobTypeWrite,
		ExecuteFn: executeFn,
	})
}
