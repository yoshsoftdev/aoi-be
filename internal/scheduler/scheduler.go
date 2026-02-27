package scheduler

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"aoi-backend/internal/core/database"
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
	httpWorker      *worker.HTTPWorker
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
	httpWorker *worker.HTTPWorker,
) *SchedulerManager {
	return &SchedulerManager{
		cron:            cron.New(cron.WithSeconds()),
		jobs:            make(map[string]cron.EntryID),
		snmpJobs:        make(map[uint]cron.EntryID),
		snmpSemaphore:   make(chan struct{}, maxConcurrentSNMP), // Limit concurrent SNMP requests
		modbusRTUWorker: modbusRTUWorker,
		modbusTCPWorker: modbusTCPWorker,
		snmpWorker:      snmpWorker,
		httpWorker:      httpWorker,
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
		worker.UpdateJobs(jobs)
		return worker
	}

	worker := NewConnectionPollingWorker(connectionID, connectionName, jobs)
	s.pollingWorkers[connectionID] = worker
	worker.Start()

	return worker
}

// LockConnection locks a connection (suspends polling)
func (s *SchedulerManager) LockConnection(connectionID uint, reason string) {
	s.workersMu.RLock()
	worker, exists := s.pollingWorkers[connectionID]
	s.workersMu.RUnlock()

	if exists {
		worker.Lock(reason)
	}
}

// UnlockConnection unlocks a connection (resumes polling)
func (s *SchedulerManager) UnlockConnection(connectionID uint, reason string) {
	s.workersMu.RLock()
	worker, exists := s.pollingWorkers[connectionID]
	s.workersMu.RUnlock()

	if exists {
		worker.Unlock(reason)
	}
}

func (s *SchedulerManager) loadAndScheduleAll() {
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
	s.db.Preload("Parameter.Device").
		Joins("JOIN parameters ON parameters.id = modbus_rtu.id_parameter").
		Where("(parameters.direction = ? OR parameters.direction = ? OR (parameters.direction = '' AND (parameters.read_write = 0 OR parameters.read_write = 2)))", "READ", "READ_WRITE").
		Where("(parameters.parameter_mode = ? OR parameters.parameter_mode = ? OR (parameters.parameter_mode = '' OR parameters.parameter_mode IS NULL))", domain.ParameterModeSOURCE, domain.ParameterModeDIRECT).
		Find(&modbusRTUConfigs)

	for _, config := range modbusRTUConfigs {
		// Default to SOURCE if parameter_mode is empty/null (backward compatibility)
		paramMode := config.Parameter.ParameterMode
		if paramMode == "" {
			paramMode = domain.ParameterModeSOURCE
		}

		// GUARD: Skip WRITE parameters (but allow READ_WRITE for polling)
		if config.Parameter.Direction == "WRITE" {
			continue
		}
		// READ_WRITE parameters are scheduled as READ (polling enabled)

		// GUARD: Only poll SOURCE & DIRECT (skip CLEAN & FORMULA)
		if paramMode != domain.ParameterModeSOURCE && paramMode != domain.ParameterModeDIRECT {
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
				JobType:      "modbus_rtu",
				PollingMs:    pollingMs,
				ExecuteFn: func(ctx context.Context) error {
					return s.modbusRTUWorker.Execute(ctx, configID)
				},
			})
		}
	}

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
			continue
		}

		// GUARD: Only poll SOURCE & DIRECT
		if paramMode != domain.ParameterModeSOURCE && paramMode != domain.ParameterModeDIRECT {
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

	for _, config := range snmpConfigs {
		// Default to SOURCE if parameter_mode is empty/null
		paramMode := config.Parameter.ParameterMode
		if paramMode == "" {
			paramMode = domain.ParameterModeSOURCE
		}

		// GUARD: Skip WRITE parameters (but allow READ_WRITE for polling)
		if config.Parameter.Direction == "WRITE" {
			continue
		}
		// READ_WRITE parameters are scheduled as READ (polling enabled)

		// GUARD: Only poll SOURCE & DIRECT
		if paramMode != domain.ParameterModeSOURCE && paramMode != domain.ParameterModeDIRECT {
			continue
		}

		pollingMs := s.getPollingMs(&config.PollingMs, &config.Parameter)

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
		}
	}

	// Load HTTP Config with Parameter and Device
	// CRITICAL: Only poll SOURCE & DIRECT parameters (not CLEAN, not FORMULA)
	var httpConfigs []domain.HTTPConfig
	s.db.Preload("Parameter.Device").
		Joins("JOIN parameters ON parameters.id = http_config.id_parameter").
		Where("(parameters.direction = ? OR parameters.direction = ? OR (parameters.direction = '' AND (parameters.read_write = 0 OR parameters.read_write = 2)))", "READ", "READ_WRITE").
		Where("(parameters.parameter_mode = ? OR parameters.parameter_mode = ? OR (parameters.parameter_mode = '' OR parameters.parameter_mode IS NULL))", domain.ParameterModeSOURCE, domain.ParameterModeDIRECT).
		Find(&httpConfigs)

	httpJobs := []PollingJob{}
	for _, config := range httpConfigs {
		// Default to SOURCE if parameter_mode is empty/null
		paramMode := config.Parameter.ParameterMode
		if paramMode == "" {
			paramMode = domain.ParameterModeSOURCE
		}

		// GUARD: Skip WRITE parameters (but allow READ_WRITE for polling)
		if config.Parameter.Direction == "WRITE" {
			continue
		}

		// GUARD: Only poll SOURCE & DIRECT
		if paramMode != domain.ParameterModeSOURCE && paramMode != domain.ParameterModeDIRECT {
			continue
		}

		pollingMs := s.getPollingMs(&config.PollingMs, &config.Parameter)

		if pollingMs > 0 {
			// CRITICAL FIX: Capture config.ID in local variable to avoid closure issue
			configID := config.ID
			// HTTP jobs go to separate list for parallel execution (no queue)
			httpJobs = append(httpJobs, PollingJob{
				ConfigID:     config.ID,
				ParameterID:  config.IDParameter,
				ConnectionID: config.Parameter.DeviceID,
				SlaveID:      0, // HTTP doesn't have slave_id, use 0
				JobType:      "http",
				PollingMs:    pollingMs,
				ExecuteFn: func(ctx context.Context) error {
					return s.httpWorker.Execute(ctx, configID)
				},
			})
		}
	}

	// Schedule SNMP jobs separately (parallel execution, no queue)
	for _, job := range snmpJobs {
		s.scheduleSNMPJob(job)
	}

	// Schedule HTTP jobs separately (parallel execution, no queue)
	for _, job := range httpJobs {
		s.scheduleSNMPJob(job) // Reuse SNMP scheduling logic for HTTP (parallel execution)
	}

	// Group Modbus jobs by connection (for queue/sequential execution)
	jobsByConnection := make(map[uint][]PollingJob)
	for _, job := range pollingJobs {
		jobsByConnection[job.ConnectionID] = append(jobsByConnection[job.ConnectionID], job)
	}

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
			continue
		}

		// Validasi: len(jobs) > 0
		if len(jobs) == 0 {
			continue
		}

		// Create or update round-robin polling worker for this connection
		s.GetOrCreatePollingWorker(connectionID, device.Nama, jobs)
	}
}

// scheduleSNMPJob schedules a single SNMP job for parallel execution (no queue)
// Each SNMP parameter gets its own cron job that runs independently
func (s *SchedulerManager) scheduleSNMPJob(job PollingJob) {
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
	parameterID := job.ParameterID
	executeFn := job.ExecuteFn // Capture ExecuteFn for closure

	// Schedule cron job for this SNMP/HTTP parameter
	// CRITICAL: Add panic recovery and semaphore to prevent crashes and resource exhaustion
	entryID, err := s.cron.AddFunc(cronExpr, func() {
		// Panic recovery to prevent crash
		defer func() {
			if r := recover(); r != nil {
				// Silent panic recovery
			}
		}()

		// Acquire semaphore to limit concurrent requests
		// This prevents resource exhaustion when many parameters are polled simultaneously
		select {
		case s.snmpSemaphore <- struct{}{}:
			// Acquired semaphore, proceed with execution
			defer func() {
				<-s.snmpSemaphore // Release semaphore when done
			}()
		case <-time.After(5 * time.Second):
			// Timeout waiting for semaphore (too many concurrent requests)
			return
		}

		// Create context with timeout (reduced to 1s for fastest error detection and polling)
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		// Execute job using ExecuteFn (supports both SNMP and HTTP)
		if executeFn != nil {
			_ = executeFn(ctx)
		}
	})

	if err != nil {
		return
	}

	// Store entry ID for later removal during reload
	s.snmpJobsMu.Lock()
	s.snmpJobs[parameterID] = entryID
	s.snmpJobsMu.Unlock()
}

// scheduleJobToQueue is deprecated - replaced by round-robin polling workers
// Kept for backward compatibility but not used in new implementation
func (s *SchedulerManager) scheduleJobToQueue(job PollingJob, queue *ConnectionQueue) {
	// Deprecated
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
}

func (s *SchedulerManager) ScheduleModbusTCP(configID uint, pollingMs int) {
	// Individual scheduling is deprecated - use Reload() to reload all jobs
}

func (s *SchedulerManager) ScheduleSNMP(configID uint, pollingMs int) {
	// Individual scheduling is deprecated - use Reload() to reload all jobs
}

func (s *SchedulerManager) RemoveJob(jobID string) {
	if entryID, exists := s.jobs[jobID]; exists {
		s.cron.Remove(entryID)
		delete(s.jobs, jobID)
	}
}

// Reload reloads all scheduled jobs from database
func (s *SchedulerManager) Reload() {
	// Remove all existing SNMP cron jobs
	s.snmpJobsMu.Lock()
	for _, entryID := range s.snmpJobs {
		s.cron.Remove(entryID)
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
			return
		}
	case "modbus_tcp":
		var modbusTCP domain.ModbusTCP
		if err := s.db.Preload("Parameter.Device").Where("id_parameter = ?", parameterID).First(&modbusTCP).Error; err == nil {
			executeFn = func(ctx context.Context) error {
				return s.modbusTCPWorker.Execute(ctx, modbusTCP.ID)
			}
		} else {
			return
		}
	case "snmp":
		var snmpConfig domain.SNMPConfig
		if err := s.db.Preload("Parameter.Device").Where("id_parameter = ?", parameterID).First(&snmpConfig).Error; err == nil {
			executeFn = func(ctx context.Context) error {
				return s.snmpWorker.Execute(ctx, snmpConfig.ID)
			}
		} else {
			return
		}
	default:
		return
	}

	// Execute immediately in a goroutine (connection locking handled by caller if needed)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = executeFn(ctx)
	}()
}

// EnqueueTestJob executes a test job immediately (with lock)
// Caller should call LockConnection before and UnlockConnection after
func (s *SchedulerManager) EnqueueTestJob(connectionID uint, connectionName string, executeFn func(ctx context.Context) error) {
	// Execute immediately in a goroutine (connection is already locked by caller)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = executeFn(ctx)
	}()
}

// EnqueueWriteJob executes a write job immediately (with lock)
// Caller should call LockConnection before and UnlockConnection after
func (s *SchedulerManager) EnqueueWriteJob(connectionID uint, connectionName string, executeFn func(ctx context.Context) error) {
	// Execute immediately in a goroutine (connection is already locked by caller)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = executeFn(ctx)
	}()
}
