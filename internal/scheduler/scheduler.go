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

// SchedulerManager manages polling jobs with per-connection round-robin workers
type SchedulerManager struct {
	cron            *cron.Cron
	jobs            map[string]cron.EntryID // Legacy: keep for compatibility (not used in new implementation)
	snmpJobs        map[uint]cron.EntryID   // SNMP jobs: parameter_id -> entry_id (for parallel execution)
	snmpJobsMu      sync.RWMutex            // Mutex for SNMP jobs map
	snmpSemaphore   chan struct{}           // Semaphore to limit concurrent SNMP requests (prevent resource exhaustion)
	modbusRTUWorker *worker.ModbusRTUWorker
	modbusTCPWorker *worker.ModbusTCPWorker
	snmpWorker      *worker.SNMPWorker
	db              *gorm.DB

	// Per-connection polling workers (round-robin) - only for Modbus
	pollingWorkers map[uint]*ConnectionPollingWorker // connection_id -> worker
	workersMu      sync.RWMutex
}

const (
	// MaxConcurrentSNMP limits the number of concurrent SNMP requests
	// This prevents resource exhaustion and crashes when many SNMP parameters are polled simultaneously
	maxConcurrentSNMP = 50
)

func NewSchedulerManager(
	modbusRTUWorker *worker.ModbusRTUWorker,
	modbusTCPWorker *worker.ModbusTCPWorker,
	snmpWorker *worker.SNMPWorker,
) *SchedulerManager {
	return &SchedulerManager{
		cron:            cron.New(cron.WithSeconds()),
		jobs:            make(map[string]cron.EntryID),
		snmpJobs:        make(map[uint]cron.EntryID),
		snmpSemaphore:   make(chan struct{}, maxConcurrentSNMP), // Limit concurrent SNMP requests
		modbusRTUWorker: modbusRTUWorker,
		modbusTCPWorker: modbusTCPWorker,
		snmpWorker:      snmpWorker,
		db:              database.GetDB(),
		pollingWorkers:  make(map[uint]*ConnectionPollingWorker),
	}
}

func (s *SchedulerManager) Start() {
	s.cron.Start()
	s.loadAndScheduleAll()
}

func (s *SchedulerManager) Stop() {
	s.cron.Stop()

	// Remove all SNMP cron jobs
	s.snmpJobsMu.Lock()
	s.snmpJobs = make(map[uint]cron.EntryID)
	s.snmpJobsMu.Unlock()

	// Stop all polling workers
	s.workersMu.Lock()
	for _, worker := range s.pollingWorkers {
		worker.Stop()
	}
	s.pollingWorkers = make(map[uint]*ConnectionPollingWorker)
	s.workersMu.Unlock()
}

// GetOrCreatePollingWorker gets or creates a polling worker for a connection
func (s *SchedulerManager) GetOrCreatePollingWorker(connectionID uint, connectionName string, jobs []PollingJob) *ConnectionPollingWorker {
	s.workersMu.Lock()
	defer s.workersMu.Unlock()

	if worker, exists := s.pollingWorkers[connectionID]; exists {
		// Update jobs if worker exists - TIDAK reset currentIndex
		log := logger.GetLogger()
		log.Infof("[SCHEDULER] Worker already exists for connection %d, updating jobs without resetting currentIndex", connectionID)
		worker.UpdateJobs(jobs)
		return worker
	}

	worker := NewConnectionPollingWorker(connectionID, connectionName, jobs)
	s.pollingWorkers[connectionID] = worker
	worker.Start()

	logger.GetLogger().Infof("[SCHEDULER] Created polling worker: connection_id=%d, connection_name=%s", connectionID, connectionName)
	return worker
}

// LockConnection locks a connection (suspends polling)
func (s *SchedulerManager) LockConnection(connectionID uint, reason string) {
	s.workersMu.RLock()
	worker, exists := s.pollingWorkers[connectionID]
	s.workersMu.RUnlock()

	if exists {
		worker.Lock(reason)
	} else {
		logger.GetLogger().Warnf("[SCHEDULER] Attempted to lock non-existent connection: connection_id=%d", connectionID)
	}
}

// UnlockConnection unlocks a connection (resumes polling)
func (s *SchedulerManager) UnlockConnection(connectionID uint, reason string) {
	s.workersMu.RLock()
	worker, exists := s.pollingWorkers[connectionID]
	s.workersMu.RUnlock()

	if exists {
		worker.Unlock(reason)
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
	var snmpJobs []PollingJob // Separate SNMP jobs for parallel execution

	// Load Modbus RTU with Parameter and Device
	// CRITICAL: Only poll SOURCE & DIRECT parameters (not CLEAN, not FORMULA)
	// SOURCE = polling device, internal only
	// DIRECT = polling device, nilai sudah clean, dibroadcast
	var modbusRTUConfigs []domain.ModbusRTU
	result := s.db.Preload("Parameter.Device").
		Joins("JOIN parameters ON parameters.id = modbus_rtu.id_parameter").
		Where("(parameters.direction = ? OR parameters.direction = ? OR (parameters.direction = '' AND (parameters.read_write = 0 OR parameters.read_write = 2)))", "READ", "READ_WRITE").
		Where("(parameters.parameter_mode = ? OR parameters.parameter_mode = ? OR (parameters.parameter_mode = '' OR parameters.parameter_mode IS NULL))", domain.ParameterModeSOURCE, domain.ParameterModeDIRECT).
		Find(&modbusRTUConfigs)

	log.Infof("[SCHEDULER] ===== QUERY RESULT: Modbus RTU READ Configs =====")
	log.Infof("[SCHEDULER] Query executed: RowsAffected=%d, Error=%v", result.RowsAffected, result.Error)
	log.Infof("[SCHEDULER] Total configs found: %d", len(modbusRTUConfigs))

	for idx, config := range modbusRTUConfigs {
		// Default to SOURCE if parameter_mode is empty/null (backward compatibility)
		paramMode := config.Parameter.ParameterMode
		if paramMode == "" {
			paramMode = domain.ParameterModeSOURCE
		}

		log.Infof("[SCHEDULER]   [%d] Config ID: %d, Parameter ID: %d, Parameter Name: %s, Direction: %s, Mode: %s, Device ID: %d, Slave ID: %d",
			idx+1, config.ID, config.IDParameter, config.Parameter.Nama, config.Parameter.Direction, paramMode, config.Parameter.DeviceID, config.SlaveID)

		// GUARD: Skip WRITE parameters (but allow READ_WRITE for polling)
		if config.Parameter.Direction == "WRITE" {
			log.Warnf("[SCHEDULER]   [%d] ⚠️  Skipping WRITE parameter: Config ID=%d, Parameter ID=%d", idx+1, config.ID, config.IDParameter)
			continue
		}
		// READ_WRITE parameters are scheduled as READ (polling enabled)

		// GUARD: Only poll SOURCE & DIRECT (skip CLEAN & FORMULA)
		if paramMode != domain.ParameterModeSOURCE && paramMode != domain.ParameterModeDIRECT {
			log.Warnf("[SCHEDULER]   [%d] ⚠️  Skipping non-pollable parameter (Mode=%s): Config ID=%d, Parameter ID=%d. Only SOURCE and DIRECT are polled.", idx+1, paramMode, config.ID, config.IDParameter)
			continue
		}

		pollingMs := s.getPollingMs(config.PollingMs, &config.Parameter)
		log.Infof("[SCHEDULER]   [%d] PollingMs: %d (config=%v, device=%d)", idx+1, pollingMs, config.PollingMs, config.Parameter.Device.PollingMs)

		if pollingMs > 0 {
			// CRITICAL FIX: Capture config.ID in local variable to avoid closure issue
			configID := config.ID
			pollingJobs = append(pollingJobs, PollingJob{
				ConfigID:     config.ID,
				ParameterID:  config.IDParameter,
				ConnectionID: config.Parameter.DeviceID,
				SlaveID:      config.SlaveID,
				JobType:      "modbus_rtu",
				PollingMs:    pollingMs,
				ExecuteFn: func(ctx context.Context) error {
					return s.modbusRTUWorker.Execute(ctx, configID)
				},
			})
			log.Infof("[SCHEDULER]   [%d] ✅ Added to pollingJobs: Parameter ID=%d, Connection ID=%d, Config ID=%d (captured)", idx+1, config.IDParameter, config.Parameter.DeviceID, configID)
		} else {
			log.Warnf("[SCHEDULER]   [%d] ⚠️  Skipping (pollingMs=0): Config ID=%d, Parameter ID=%d", idx+1, config.ID, config.IDParameter)
		}
	}

	log.Infof("[SCHEDULER] ===== END QUERY RESULT: Modbus RTU =====")
	log.Infof("[SCHEDULER] Total pollingJobs after Modbus RTU: %d", len(pollingJobs))

	// Load Modbus TCP with Parameter and Device
	// CRITICAL: Only poll SOURCE & DIRECT parameters (not CLEAN, not FORMULA)
	var modbusTCPConfigs []domain.ModbusTCP
	s.db.Preload("Parameter.Device").
		Joins("JOIN parameters ON parameters.id = modbus_tcp.id_parameter").
		Where("(parameters.direction = ? OR parameters.direction = ? OR (parameters.direction = '' AND (parameters.read_write = 0 OR parameters.read_write = 2)))", "READ", "READ_WRITE").
		Where("(parameters.parameter_mode = ? OR parameters.parameter_mode = ? OR (parameters.parameter_mode = '' OR parameters.parameter_mode IS NULL))", domain.ParameterModeSOURCE, domain.ParameterModeDIRECT).
		Find(&modbusTCPConfigs)

	for _, config := range modbusTCPConfigs {
		// Default to SOURCE if parameter_mode is empty/null
		paramMode := config.Parameter.ParameterMode
		if paramMode == "" {
			paramMode = domain.ParameterModeSOURCE
		}

		// GUARD: Skip WRITE parameters
		if config.Parameter.Direction == "WRITE" {
			log.Warnf("[SCHEDULER] ⚠️  Skipping WRITE parameter: Config ID=%d, Parameter ID=%d", config.ID, config.IDParameter)
			continue
		}

		// GUARD: Only poll SOURCE & DIRECT
		if paramMode != domain.ParameterModeSOURCE && paramMode != domain.ParameterModeDIRECT {
			log.Warnf("[SCHEDULER] ⚠️  Skipping non-pollable parameter (Mode=%s): Config ID=%d, Parameter ID=%d", paramMode, config.ID, config.IDParameter)
			continue
		}
		pollingMs := s.getPollingMs(config.PollingMs, &config.Parameter)
		if pollingMs > 0 {
			// CRITICAL FIX: Capture config.ID in local variable to avoid closure issue
			configID := config.ID
			pollingJobs = append(pollingJobs, PollingJob{
				ConfigID:     config.ID,
				ParameterID:  config.IDParameter,
				ConnectionID: config.Parameter.DeviceID,
				SlaveID:      config.SlaveID,
				JobType:      "modbus_tcp",
				PollingMs:    pollingMs,
				ExecuteFn: func(ctx context.Context) error {
					return s.modbusTCPWorker.Execute(ctx, configID)
				},
			})
		}
	}

	// Load SNMP Config with Parameter and Device
	// CRITICAL: Only poll SOURCE & DIRECT parameters (not CLEAN, not FORMULA)
	var snmpConfigs []domain.SNMPConfig
	s.db.Preload("Parameter.Device").
		Joins("JOIN parameters ON parameters.id = snmp_config.id_parameter").
		Where("(parameters.direction = ? OR parameters.direction = ? OR (parameters.direction = '' AND (parameters.read_write = 0 OR parameters.read_write = 2)))", "READ", "READ_WRITE").
		Where("(parameters.parameter_mode = ? OR parameters.parameter_mode = ? OR (parameters.parameter_mode = '' OR parameters.parameter_mode IS NULL))", domain.ParameterModeSOURCE, domain.ParameterModeDIRECT).
		Find(&snmpConfigs)

	log.Infof("[SCHEDULER] ===== QUERY RESULT: SNMP Configs =====")
	log.Infof("[SCHEDULER] Total SNMP configs found: %d", len(snmpConfigs))

	for idx, config := range snmpConfigs {
		// Default to SOURCE if parameter_mode is empty/null
		paramMode := config.Parameter.ParameterMode
		if paramMode == "" {
			paramMode = domain.ParameterModeSOURCE
		}

		log.Infof("[SCHEDULER]   [%d] Config ID: %d, Parameter ID: %d, Parameter Name: %s, Direction: %s, Mode: %s, Device ID: %d",
			idx+1, config.ID, config.IDParameter, config.Parameter.Nama, config.Parameter.Direction, paramMode, config.Parameter.DeviceID)

		// GUARD: Skip WRITE parameters (but allow READ_WRITE for polling)
		if config.Parameter.Direction == "WRITE" {
			log.Warnf("[SCHEDULER]   [%d] ⚠️  Skipping WRITE parameter: Config ID=%d, Parameter ID=%d", idx+1, config.ID, config.IDParameter)
			continue
		}
		// READ_WRITE parameters are scheduled as READ (polling enabled)

		// GUARD: Only poll SOURCE & DIRECT
		if paramMode != domain.ParameterModeSOURCE && paramMode != domain.ParameterModeDIRECT {
			log.Warnf("[SCHEDULER]   [%d] ⚠️  Skipping non-pollable parameter (Mode=%s): Config ID=%d, Parameter ID=%d. Only SOURCE and DIRECT are polled.", idx+1, paramMode, config.ID, config.IDParameter)
			continue
		}

		pollingMs := s.getPollingMs(&config.PollingMs, &config.Parameter)
		log.Infof("[SCHEDULER]   [%d] PollingMs: %d (config=%d, device=%d)", idx+1, pollingMs, config.PollingMs, config.Parameter.Device.PollingMs)

		if pollingMs > 0 {
			// CRITICAL FIX: Capture config.ID in local variable to avoid closure issue
			configID := config.ID
			// SNMP jobs go to separate list for parallel execution (no queue)
			snmpJobs = append(snmpJobs, PollingJob{
				ConfigID:     config.ID,
				ParameterID:  config.IDParameter,
				ConnectionID: config.Parameter.DeviceID,
				SlaveID:      0, // SNMP doesn't have slave_id, use 0
				JobType:      "snmp",
				PollingMs:    pollingMs,
				ExecuteFn: func(ctx context.Context) error {
					return s.snmpWorker.Execute(ctx, configID)
				},
			})
			log.Infof("[SCHEDULER]   [%d] ✅ Added to SNMP jobs (parallel): Parameter ID=%d, Connection ID=%d, Config ID=%d (captured)", idx+1, config.IDParameter, config.Parameter.DeviceID, configID)
		} else {
			log.Warnf("[SCHEDULER]   [%d] ⚠️  Skipping (pollingMs=0): Config ID=%d, Parameter ID=%d", idx+1, config.ID, config.IDParameter)
		}
	}

	log.Infof("[SCHEDULER] ===== END QUERY RESULT: SNMP =====")
	log.Infof("[SCHEDULER] Total Modbus pollingJobs: %d", len(pollingJobs))
	log.Infof("[SCHEDULER] Total SNMP jobs (parallel): %d", len(snmpJobs))

	// Schedule SNMP jobs separately (parallel execution, no queue)
	log.Infof("[SCHEDULER] ===== SCHEDULING SNMP JOBS (PARALLEL) =====")
	for _, job := range snmpJobs {
		s.scheduleSNMPJob(job)
	}
	log.Infof("[SCHEDULER] Scheduled %d SNMP jobs for parallel execution", len(snmpJobs))

	// Group Modbus jobs by connection (for queue/sequential execution)
	log.Infof("[SCHEDULER] ===== GROUPING MODBUS JOBS BY CONNECTION (QUEUE) =====")
	log.Infof("[SCHEDULER] Total Modbus pollingJobs before grouping: %d", len(pollingJobs))

	jobsByConnection := make(map[uint][]PollingJob)
	for _, job := range pollingJobs {
		log.Debugf("[SCHEDULER] Job: Parameter ID=%d, Connection ID=%d, Slave ID=%d", job.ParameterID, job.ConnectionID, job.SlaveID)
		jobsByConnection[job.ConnectionID] = append(jobsByConnection[job.ConnectionID], job)
	}

	log.Infof("[SCHEDULER] Grouped into %d connections:", len(jobsByConnection))
	for connectionID, jobs := range jobsByConnection {
		log.Infof("[SCHEDULER]   Connection ID %d: %d jobs", connectionID, len(jobs))
		for i, job := range jobs {
			log.Infof("[SCHEDULER]     [%d] Parameter ID=%d, Slave ID=%d, Config ID=%d", i+1, job.ParameterID, job.SlaveID, job.ConfigID)
		}
	}
	log.Infof("[SCHEDULER] ===== END GROUPING =====")

	// Sort and start round-robin polling workers per connection
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

		// LANGKAH 1: VERIFIKASI DATA INPUT - Logging sebelum worker dibuat
		log.Infof("[SCHEDULER] ===== VERIFICATION: Connection %d (%s) =====", connectionID, device.Nama)
		log.Infof("[SCHEDULER] Total READ parameters: %d", len(jobs))

		// Validasi: len(jobs) > 1
		if len(jobs) == 0 {
			log.Warnf("[SCHEDULER] ⚠️  No jobs for connection %d, skipping", connectionID)
			continue
		}

		if len(jobs) == 1 {
			log.Warnf("[SCHEDULER] ⚠️  Only 1 parameter found for connection %d. This may indicate a bug in DB query/mapping.", connectionID)
			log.Warnf("[SCHEDULER] ⚠️  Parameter: parameter_id=%d, slave_id=%d, config_id=%d, polling_ms=%d",
				jobs[0].ParameterID, jobs[0].SlaveID, jobs[0].ConfigID, jobs[0].PollingMs)
			log.Warnf("[SCHEDULER] ⚠️  Continuing anyway, but please verify database has multiple READ parameters for this connection.")
		}

		// Log daftar lengkap parameter
		log.Infof("[SCHEDULER] Parameter list for connection %d:", connectionID)
		for i, job := range jobs {
			log.Infof("[SCHEDULER]   [%d] parameter_id=%d, slave_id=%d, config_id=%d, type=%s, polling_ms=%d",
				i, job.ParameterID, job.SlaveID, job.ConfigID, job.JobType, job.PollingMs)
		}
		log.Infof("[SCHEDULER] ===== END VERIFICATION =====")

		// Create or update round-robin polling worker for this connection
		s.GetOrCreatePollingWorker(connectionID, device.Nama, jobs)
	}

	log.Infof("[SCHEDULER] Scheduled %d Modbus polling jobs (queue) across %d connections", len(pollingJobs), len(jobsByConnection))
	log.Infof("[SCHEDULER] Scheduled %d SNMP jobs (parallel)", len(snmpJobs))
}

// scheduleSNMPJob schedules a single SNMP job for parallel execution (no queue)
// Each SNMP parameter gets its own cron job that runs independently
func (s *SchedulerManager) scheduleSNMPJob(job PollingJob) {
	log := logger.GetLogger()

	// Convert polling interval to cron expression
	// pollingMs is in milliseconds, convert to seconds for cron
	pollingSeconds := job.PollingMs / 1000
	if pollingSeconds < 1 {
		pollingSeconds = 1 // Minimum 1 second
	}

	// Create cron expression: every N seconds
	// Format: "*/N * * * * *" (every N seconds)
	cronExpr := fmt.Sprintf("*/%d * * * * *", pollingSeconds)

	// Capture job variables for closure
	configID := job.ConfigID
	parameterID := job.ParameterID
	connectionID := job.ConnectionID

	log.Infof("[SCHEDULER] Scheduling SNMP job: parameter_id=%d, connection_id=%d, config_id=%d, polling_ms=%d, cron=%s",
		parameterID, connectionID, configID, job.PollingMs, cronExpr)

	// Schedule cron job for this SNMP parameter
	// CRITICAL: Add panic recovery and semaphore to prevent crashes and resource exhaustion
	entryID, err := s.cron.AddFunc(cronExpr, func() {
		// Panic recovery to prevent crash
		defer func() {
			if r := recover(); r != nil {
				log.Errorf("[SNMP-PARALLEL] PANIC RECOVERED: parameter_id=%d, connection_id=%d, config_id=%d, panic=%v",
					parameterID, connectionID, configID, r)
			}
		}()

		// Acquire semaphore to limit concurrent SNMP requests
		// This prevents resource exhaustion when many SNMP parameters are polled simultaneously
		select {
		case s.snmpSemaphore <- struct{}{}:
			// Acquired semaphore, proceed with execution
			defer func() {
				<-s.snmpSemaphore // Release semaphore when done
			}()
		case <-time.After(5 * time.Second):
			// Timeout waiting for semaphore (too many concurrent requests)
			log.Warnf("[SNMP-PARALLEL] Semaphore timeout: parameter_id=%d, connection_id=%d, config_id=%d (too many concurrent requests)",
				parameterID, connectionID, configID)
			return
		}

		// Create context with timeout (reduced to 1s for fastest error detection and polling)
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		log.Debugf("[SNMP-PARALLEL] Executing SNMP poll: parameter_id=%d, connection_id=%d, config_id=%d",
			parameterID, connectionID, configID)

		// Execute SNMP poll with error handling
		if err := s.snmpWorker.Execute(ctx, configID); err != nil {
			log.Warnf("[SNMP-PARALLEL] SNMP poll failed: parameter_id=%d, connection_id=%d, error=%v",
				parameterID, connectionID, err)
		} else {
			log.Debugf("[SNMP-PARALLEL] SNMP poll completed: parameter_id=%d, connection_id=%d",
				parameterID, connectionID)
		}
	})

	if err != nil {
		log.Errorf("[SCHEDULER] Failed to schedule SNMP job: parameter_id=%d, error=%v", parameterID, err)
		return
	}

	// Store entry ID for later removal during reload
	s.snmpJobsMu.Lock()
	s.snmpJobs[parameterID] = entryID
	s.snmpJobsMu.Unlock()

	log.Infof("[SCHEDULER] ✅ SNMP job scheduled: parameter_id=%d, entry_id=%d, cron=%s",
		parameterID, entryID, cronExpr)
}

// scheduleJobToQueue is deprecated - replaced by round-robin polling workers
// Kept for backward compatibility but not used in new implementation
func (s *SchedulerManager) scheduleJobToQueue(job PollingJob, queue *ConnectionQueue) {
	logger.GetLogger().Warnf("[SCHEDULER] scheduleJobToQueue is deprecated, using round-robin workers instead")
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

// Legacy methods for backward compatibility (now use round-robin workers)
func (s *SchedulerManager) ScheduleModbusRTU(configID uint, pollingMs int) {
	// Individual scheduling is deprecated - use Reload() to reload all jobs
	logger.GetLogger().Warnf("[SCHEDULER] ScheduleModbusRTU is deprecated, use Reload() instead")
}

func (s *SchedulerManager) ScheduleModbusTCP(configID uint, pollingMs int) {
	// Individual scheduling is deprecated - use Reload() to reload all jobs
	logger.GetLogger().Warnf("[SCHEDULER] ScheduleModbusTCP is deprecated, use Reload() instead")
}

func (s *SchedulerManager) ScheduleSNMP(configID uint, pollingMs int) {
	// Individual scheduling is deprecated - use Reload() to reload all jobs
	logger.GetLogger().Warnf("[SCHEDULER] ScheduleSNMP is deprecated, use Reload() instead")
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

	// Remove all existing SNMP cron jobs
	s.snmpJobsMu.Lock()
	for paramID, entryID := range s.snmpJobs {
		s.cron.Remove(entryID)
		log.Debugf("[SCHEDULER] Removed SNMP cron job: parameter_id=%d, entry_id=%d", paramID, entryID)
	}
	s.snmpJobs = make(map[uint]cron.EntryID)
	s.snmpJobsMu.Unlock()

	// Stop all polling workers
	s.workersMu.Lock()
	for _, worker := range s.pollingWorkers {
		worker.Stop()
	}
	s.pollingWorkers = make(map[uint]*ConnectionPollingWorker)
	s.workersMu.Unlock()

	// Reload and schedule all configurations
	s.loadAndScheduleAll()
}

// TriggerImmediateExecution triggers immediate execution for a parameter
func (s *SchedulerManager) TriggerImmediateExecution(parameterID uint, paramType string) {
	var executeFn func(ctx context.Context) error

	switch paramType {
	case "modbus_rtu":
		var modbusRTU domain.ModbusRTU
		if err := s.db.Preload("Parameter.Device").Where("id_parameter = ?", parameterID).First(&modbusRTU).Error; err == nil {
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

	// Execute immediately in a goroutine (connection locking handled by caller if needed)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := executeFn(ctx); err != nil {
			logger.GetLogger().Warnf("[SCHEDULER] Immediate execution failed for parameter %d: %v", parameterID, err)
		} else {
			logger.GetLogger().Infof("[SCHEDULER] Immediate execution completed for parameter %d", parameterID)
		}
	}()
}

// EnqueueTestJob executes a test job immediately (with lock)
// Caller should call LockConnection before and UnlockConnection after
func (s *SchedulerManager) EnqueueTestJob(connectionID uint, connectionName string, executeFn func(ctx context.Context) error) {
	// Execute immediately in a goroutine (connection is already locked by caller)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := executeFn(ctx); err != nil {
			logger.GetLogger().Warnf("[SCHEDULER] Test job execution failed: connection_id=%d, error=%v", connectionID, err)
		}
	}()
}

// EnqueueWriteJob executes a write job immediately (with lock)
// Caller should call LockConnection before and UnlockConnection after
func (s *SchedulerManager) EnqueueWriteJob(connectionID uint, connectionName string, executeFn func(ctx context.Context) error) {
	// Execute immediately in a goroutine (connection is already locked by caller)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := executeFn(ctx); err != nil {
			logger.GetLogger().Warnf("[SCHEDULER] Write job execution failed: connection_id=%d, error=%v", connectionID, err)
		}
	}()
}
