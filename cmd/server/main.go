package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"aoi-backend/api/routes"
	"aoi-backend/internal/core/cache"
	"aoi-backend/internal/core/config"
	"aoi-backend/internal/core/database"
	"aoi-backend/internal/core/logger"
	"aoi-backend/internal/domain"
	"aoi-backend/internal/repository"
	"aoi-backend/internal/scheduler"
	"aoi-backend/internal/service"
	"aoi-backend/internal/worker"
)

func main() {
	// Parse command line flags
	devMode := flag.Bool("dev", false, "Enable development mode (Mac-compatible USB TTY paths)")
	poofMode := flag.Bool("poof", false, "Disable polling (polling enabled by default)")
	flag.Parse()

	// Set dev flag in environment for config to read
	if *devMode {
		os.Setenv("APP_DEV", "true")
	}

	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		panic(fmt.Sprintf("Failed to load config: %v", err))
	}

	// Initialize logger
	logger.Init()
	log := logger.GetLogger()
	log.Info("Starting AOI System...")

	// Initialize database
	if err := database.Init(); err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}
	log.Info("Database initialized")

	// Initialize log database
	if err := database.InitLogDB(); err != nil {
		log.Fatalf("Failed to initialize log database: %v", err)
	}
	log.Info("Log database initialized")

	// Run migrations
	if err := runMigrations(); err != nil {
		log.Fatalf("Failed to run migrations: %v", err)
	}
	log.Info("Migrations completed")

	// Run log database migrations
	if err := runLogMigrations(); err != nil {
		log.Fatalf("Failed to run log database migrations: %v", err)
	}
	log.Info("Log database migrations completed")

	// Initialize cache
	cacheInstance, err := cache.Init()
	if err != nil {
		log.Fatalf("Failed to initialize cache: %v", err)
	}
	log.Info("Cache initialized")

	// Initialize repositories
	deviceRepo := repository.NewDeviceRepository()
	paramRepo := repository.NewParameterRepository()
	modbusRTURepo := repository.NewModbusRTURepository()
	modbusTCPRepo := repository.NewModbusTCPRepository()
	snmpRepo := repository.NewSNMPRepository()
	httpRepo := repository.NewHTTPRepository()
	formulaRepo := repository.NewFormulaRepository()
	presetRepo := repository.NewPresetRepository()
	workflowRepo := repository.NewWorkflowRepository(database.GetDB())
	alarmRepo := repository.NewAlarmRepository(database.GetDB())
	writeLogRepo := repository.NewWriteLogRepository()

	// Initialize services
	deviceService := service.NewDeviceService(deviceRepo)
	paramService := service.NewParameterService(
		paramRepo,
		workflowRepo,
		formulaRepo,
		modbusRTURepo,
		modbusTCPRepo,
		snmpRepo,
		presetRepo,
		cacheInstance,
	)
	formulaEval := service.NewFormulaEvaluator()

	// Initialize WebSocket hub
	wsHub := worker.NewWebSocketHub()
	go wsHub.Run()
	log.Info("WebSocket hub started")

	// Initialize Threshold Service (needed for Formula Engine)
	thresholdRepo := repository.NewThresholdRepository()
	thresholdLogRepo := repository.NewThresholdLogRepository()
	thresholdService := service.NewThresholdService(thresholdRepo, thresholdLogRepo)
	log.Info("Threshold service initialized")

	// Initialize Formula Engine
	formulaEngine := service.NewFormulaEngine(
		formulaRepo,
		paramRepo,
		cacheInstance,
		wsHub,
		thresholdService,
	)

	// Load all formulas from database
	if err := formulaEngine.LoadAll(); err != nil {
		log.Warnf("Failed to load formulas: %v", err)
	} else {
		log.Info("Formula engine initialized and loaded")
		// Evaluate all formulas after loading (if dependencies are available)
		// This ensures existing formula parameters appear in dashboard immediately
		go func() {
			// Wait longer for raw parameters to be polled and cached
			// Increased delay to ensure polling has occurred at least once
			// Note: With sequential queue, first job executes immediately, subsequent jobs wait
			// Wait for at least 2 polling cycles to ensure all raw parameters are polled
			log.Info("[STARTUP] Waiting for raw parameters to be polled before evaluating formulas...")
			time.Sleep(3 * time.Second) // Wait for first polling cycle (1000ms) + buffer
			log.Info("[STARTUP] Evaluating all loaded formulas...")
			formulaEngine.EvaluateAll()

			// Also evaluate again after another cycle to catch any that failed first time
			time.Sleep(2 * time.Second)
			log.Info("[STARTUP] Re-evaluating formulas after second polling cycle...")
			formulaEngine.EvaluateAll()
		}()
	}

	// Initialize Write Service (needed for Workflow Engine)
	writeService := service.NewWriteService(
		paramRepo,
		presetRepo,
		modbusRTURepo,
		modbusTCPRepo,
		deviceRepo,
		cacheInstance,
		formulaEngine,
		writeLogRepo,
	)

	// Initialize Internal Parameter Service (for workflow chaining)
	internalParameterService := service.NewInternalParameterService(
		paramRepo,
		cacheInstance,
	)

	// Initialize Set-Point Repository and Service (needed for workflow write actions with setpoint value source)
	setPointRepo := repository.NewSetPointRepository()
	setPointService := service.NewSetPointService(setPointRepo)

	// Initialize Workflow Engine (pass wsHub for alarm notifications)
	workflowEngine := service.NewWorkflowEngine(
		workflowRepo,
		paramRepo,
		alarmRepo,
		setPointRepo,
		cacheInstance,
		writeService,
		internalParameterService,
		wsHub, // Pass WebSocketHub for broadcasting alarms
	)

	// Wire set-point service to workflow engine so workflows with
	// SETPOINT_CHANGED triggers can be evaluated on set-point updates.
	setPointService.SetWorkflowEngine(workflowEngine)
	workflowEngine.SetSetPointService(setPointService)

	// Load all workflows from database
	if err := workflowEngine.LoadAll(); err != nil {
		log.Warnf("Failed to load workflows: %v", err)
	} else {
		log.Info("Workflow engine initialized and loaded")
	}

	// Set-Point Service already initialized above for workflow engine

	// Initialize Bypass Engine
	bypassRepo := repository.NewBypassRepository()
	bypassEngine := service.NewBypassEngine(
		bypassRepo,
		setPointRepo,
		paramRepo,
		cacheInstance,
		writeService,
	)

	// Wire set-point service to bypass engine (for updating set-point values)
	setPointService.SetBypassEngine(bypassEngine)
	bypassEngine.SetSetPointService(setPointService)

	// Load all bypasses from database
	if err := bypassEngine.LoadAll(); err != nil {
		log.Warnf("Failed to load bypasses: %v", err)
	} else {
		log.Info("Bypass engine initialized and loaded")
	}

	// Set callback: Formula Engine -> Workflow Engine
	// Workflow Engine evaluates AFTER data READ bersih (after formula evaluation)
	formulaEngine.SetWorkflowCallback(func(parameterID uint, data *domain.RealtimeData) {
		workflowEngine.OnParameterUpdated(parameterID, data)
	})

	// Initialize workers (pass formulaEngine for reactive formula evaluation and thresholdService for alarm status)
	modbusRTUWorker := worker.NewModbusRTUWorker(
		modbusRTURepo,
		paramRepo,
		deviceRepo,
		cacheInstance,
		formulaEval,
		formulaEngine,
		thresholdService,
		wsHub,
	)

	modbusTCPWorker := worker.NewModbusTCPWorker(
		modbusTCPRepo,
		paramRepo,
		deviceRepo,
		cacheInstance,
		formulaEval,
		formulaEngine,
		thresholdService,
		wsHub,
	)

	snmpWorker := worker.NewSNMPWorker(
		snmpRepo,
		paramRepo,
		deviceRepo,
		cacheInstance,
		formulaEval,
		formulaEngine,
		thresholdService,
		wsHub,
	)

	httpWorker := worker.NewHTTPWorker(
		httpRepo,
		paramRepo,
		cacheInstance,
		formulaEval,
		thresholdService,
		wsHub,
	)

	// Initialize scheduler (but don't start yet)
	schedulerManager := scheduler.NewSchedulerManager(
		modbusRTUWorker,
		modbusTCPWorker,
		snmpWorker,
		httpWorker,
	)
	log.Info("Scheduler initialized (not started yet)")

	// Setup routes FIRST (before starting polling)
	router := routes.SetupRoutes(
		deviceService,
		paramService,
		modbusRTURepo,
		modbusTCPRepo,
		snmpRepo,
		httpRepo,
		cacheInstance,
		wsHub,
		schedulerManager,
		workflowRepo,
		workflowEngine,
		formulaEngine,
		alarmRepo,
		writeLogRepo,
		bypassEngine,
		internalParameterService,
	)
	log.Info("Routes configured successfully")

	// Start server and wait for it to be ready
	addr := fmt.Sprintf("%s:%s", cfg.Server.Host, cfg.Server.Port)
	log.Infof("Server starting on %s", addr)

	// Start server in goroutine
	go func() {
		if err := router.Run(addr); err != nil {
			log.Fatalf("Failed to start server: %v", err)
		}
	}()

	// Wait for server to be ready (give it time to bind to port and initialize routes)
	time.Sleep(500 * time.Millisecond)
	log.Info("Server is ready to accept connections")

	// NOW start polling AFTER server is ready
	// Control polling based on --poof flag
	// If --poof flag is present, disable polling
	// If --poof flag is NOT present, enable polling (default behavior)
	if *poofMode {
		log.Info("Polling disabled via --poof flag")
	} else {
		log.Info("Starting scheduler and polling...")
		schedulerManager.Start()
		log.Info("Scheduler started (polling enabled)")
	}

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Info("Shutting down server...")
	schedulerManager.Stop()
	log.Info("Server stopped")
}

func runMigrations() error {
	db := database.GetDB()

	// Migration 043: Drop FK constraint on current_alarm.parameter_id
	// This allows alarms for set-point triggered workflows (parameter_id=0)
	// Check if current_alarm table exists
	var tableExists int
	db.Raw("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='current_alarm'").Scan(&tableExists)
	migrationApplied := false
	if tableExists > 0 {
		// Check if FK constraint exists by checking sqlite_master for foreign key definition
		var tableSQL string
		db.Raw("SELECT sql FROM sqlite_master WHERE type='table' AND name='current_alarm'").Scan(&tableSQL)

		if tableSQL != "" && tableSQL != "NULL" {
			// Check if FK constraint on parameter_id exists
			// GORM creates FK with format: FOREIGN KEY (ParameterID) REFERENCES parameter(id)
			hasParameterFK := strings.Contains(strings.ToLower(tableSQL), "foreign key") &&
				(strings.Contains(tableSQL, "ParameterID") || strings.Contains(strings.ToLower(tableSQL), "parameter_id")) &&
				strings.Contains(strings.ToLower(tableSQL), "parameter")

			if hasParameterFK {
				logger.GetLogger().Info("Running migration 043: Removing FK constraint on current_alarm.parameter_id")
				// Execute migration SQL to drop FK constraint
				migrationSQL := `
					PRAGMA foreign_keys=off;
					BEGIN TRANSACTION;
					CREATE TABLE IF NOT EXISTS current_alarm_new (
						id INTEGER PRIMARY KEY AUTOINCREMENT,
						workflow_id INTEGER NOT NULL,
						workflow_name VARCHAR(100) NOT NULL,
						parameter_id INTEGER NOT NULL,
						parameter_name VARCHAR(100),
						alarm_message TEXT NOT NULL,
						triggered_at DATETIME DEFAULT CURRENT_TIMESTAMP,
						created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
						updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
						FOREIGN KEY (workflow_id) REFERENCES workflow(id) ON DELETE CASCADE
					);
					INSERT INTO current_alarm_new (
						id, workflow_id, workflow_name, parameter_id, parameter_name, 
						alarm_message, triggered_at, created_at, updated_at
					) SELECT 
						id, workflow_id, workflow_name, parameter_id, parameter_name, 
						alarm_message, triggered_at, created_at, updated_at
					FROM current_alarm;
					DROP TABLE current_alarm;
					ALTER TABLE current_alarm_new RENAME TO current_alarm;
					CREATE INDEX IF NOT EXISTS idx_current_alarm_workflow_id ON current_alarm(workflow_id);
					CREATE INDEX IF NOT EXISTS idx_current_alarm_parameter_id ON current_alarm(parameter_id);
					CREATE UNIQUE INDEX IF NOT EXISTS idx_current_alarm_workflow_unique ON current_alarm(workflow_id);
					COMMIT;
					PRAGMA foreign_keys=on;
				`
				if err := db.Exec(migrationSQL).Error; err != nil {
					logger.GetLogger().Warnf("Migration 043 failed (may already be applied): %v", err)
				} else {
					logger.GetLogger().Info("Migration 043 completed successfully")
					migrationApplied = true
				}
			}
		}
	}

	// Auto-migrate all tables
	// If migration 043 was just applied, skip CurrentAlarm to avoid GORM DDL comparison issues
	// The table structure is already correct after migration
	tablesToMigrate := []interface{}{
		&domain.Koneksi{},
		&domain.Parameter{},
		&domain.ModbusRTU{},
		&domain.ModbusTCP{},
		&domain.SNMPConfig{},
		&domain.HTTPConfig{},
		&domain.AnalogInput{},
		&domain.AnalogOutput{},
		&domain.DigitalInput{},
		&domain.DigitalOutput{},
		&domain.ParameterFormula{},
		&domain.ParameterFormulaVar{},
		&domain.ParameterPreset{},
		&domain.SetPoint{},
		&domain.Workflow{},
		&domain.WorkflowCondition{},
		&domain.WorkflowAction{},
		&domain.Threshold{},
		&domain.ThresholdLog{},
		&domain.WorkflowState{},
		&domain.Bypass{},
		&domain.BypassRule{},
		&domain.BypassAction{},
		&domain.IMCParameterConfig{},
	}

	// Only add CurrentAlarm if migration 043 was not just applied
	// If migration was applied, the table structure is already correct and we skip AutoMigrate
	// to avoid GORM's DDL comparison issues with manually migrated tables
	if !migrationApplied {
		tablesToMigrate = append(tablesToMigrate, &domain.CurrentAlarm{})
	}

	if err := db.AutoMigrate(tablesToMigrate...); err != nil {
		return err
	}

	// If migration was applied, verify table exists (it should, but check anyway)
	if migrationApplied {
		var count int
		if err := db.Raw("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='current_alarm'").Scan(&count).Error; err != nil {
			logger.GetLogger().Warnf("Failed to verify current_alarm table after migration: %v", err)
		} else if count == 0 {
			// Table doesn't exist (shouldn't happen), create it with AutoMigrate
			logger.GetLogger().Info("current_alarm table not found after migration, creating with AutoMigrate")
			if err := db.AutoMigrate(&domain.CurrentAlarm{}); err != nil {
				return fmt.Errorf("failed to create current_alarm table: %w", err)
			}
		}
	}

	return nil
}

func runLogMigrations() error {
	logDB := database.GetLogDB()

	// Auto-migrate log tables
	if err := logDB.AutoMigrate(
		&domain.WriteLog{},
	); err != nil {
		return err
	}

	return nil
}
