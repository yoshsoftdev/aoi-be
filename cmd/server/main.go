package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
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

	// Run migrations
	if err := runMigrations(); err != nil {
		log.Fatalf("Failed to run migrations: %v", err)
	}
	log.Info("Migrations completed")

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
	formulaRepo := repository.NewFormulaRepository()
	presetRepo := repository.NewPresetRepository()
	workflowRepo := repository.NewWorkflowRepository(database.GetDB())

	// Initialize services
	deviceService := service.NewDeviceService(deviceRepo)
	paramService := service.NewParameterService(paramRepo)
	formulaEval := service.NewFormulaEvaluator()

	// Initialize WebSocket hub
	wsHub := worker.NewWebSocketHub()
	go wsHub.Run()
	log.Info("WebSocket hub started")

	// Initialize Formula Engine
	formulaEngine := service.NewFormulaEngine(
		formulaRepo,
		paramRepo,
		cacheInstance,
		wsHub,
	)

	// Load all formulas from database
	if err := formulaEngine.LoadAll(); err != nil {
		log.Warnf("Failed to load formulas: %v", err)
	} else {
		log.Info("Formula engine initialized and loaded")
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
	)

	// Initialize Workflow Engine
	workflowEngine := service.NewWorkflowEngine(
		workflowRepo,
		paramRepo,
		cacheInstance,
		writeService,
	)

	// Load all workflows from database
	if err := workflowEngine.LoadAll(); err != nil {
		log.Warnf("Failed to load workflows: %v", err)
	} else {
		log.Info("Workflow engine initialized and loaded")
	}

	// Set callback: Formula Engine -> Workflow Engine
	// Workflow Engine evaluates AFTER data READ bersih (after formula evaluation)
	formulaEngine.SetWorkflowCallback(func(parameterID uint, data *domain.RealtimeData) {
		workflowEngine.OnParameterUpdated(parameterID, data)
	})

	// Initialize workers (pass formulaEngine for reactive formula evaluation)
	modbusRTUWorker := worker.NewModbusRTUWorker(
		modbusRTURepo,
		paramRepo,
		deviceRepo,
		cacheInstance,
		formulaEval,
		formulaEngine,
		wsHub,
	)

	modbusTCPWorker := worker.NewModbusTCPWorker(
		modbusTCPRepo,
		paramRepo,
		deviceRepo,
		cacheInstance,
		formulaEval,
		formulaEngine,
		wsHub,
	)

	snmpWorker := worker.NewSNMPWorker(
		snmpRepo,
		paramRepo,
		deviceRepo,
		cacheInstance,
		formulaEval,
		formulaEngine,
		wsHub,
	)

	// Initialize scheduler
	schedulerManager := scheduler.NewSchedulerManager(
		modbusRTUWorker,
		modbusTCPWorker,
		snmpWorker,
	)

	// Control polling based on --poof flag
	// If --poof flag is present, disable polling
	// If --poof flag is NOT present, enable polling (default behavior)
	if *poofMode {
		log.Info("Scheduler initialized (polling disabled via --poof flag)")
	} else {
		schedulerManager.Start()
		log.Info("Scheduler started (polling enabled)")
	}

	// Setup routes
	router := routes.SetupRoutes(
		deviceService,
		paramService,
		modbusRTURepo,
		modbusTCPRepo,
		snmpRepo,
		cacheInstance,
		wsHub,
		schedulerManager,
		workflowRepo,
		workflowEngine,
	)
	log.Info("Routes configured successfully")

	// Start server
	addr := fmt.Sprintf("%s:%s", cfg.Server.Host, cfg.Server.Port)
	log.Infof("Server starting on %s", addr)

	// Graceful shutdown
	go func() {
		if err := router.Run(addr); err != nil {
			log.Fatalf("Failed to start server: %v", err)
		}
	}()

	// Give server a moment to start
	time.Sleep(100 * time.Millisecond)
	log.Info("Server is ready to accept connections")

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

	// Auto-migrate all tables
	if err := db.AutoMigrate(
		&domain.Koneksi{},
		&domain.Parameter{},
		&domain.ModbusRTU{},
		&domain.ModbusTCP{},
		&domain.SNMPConfig{},
		&domain.AnalogInput{},
		&domain.AnalogOutput{},
		&domain.DigitalInput{},
		&domain.DigitalOutput{},
		&domain.ParameterFormula{},
		&domain.ParameterFormulaVar{},
		&domain.ParameterPreset{},
		&domain.Workflow{},
		&domain.WorkflowCondition{},
		&domain.WorkflowAction{},
		&domain.WorkflowState{},
	); err != nil {
		return err
	}

	return nil
}
