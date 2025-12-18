package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

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
	schedulerManager.Start()
	log.Info("Scheduler started")

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
	)

	// Start server
	addr := fmt.Sprintf("%s:%s", cfg.Server.Host, cfg.Server.Port)
	log.Infof("Server starting on %s", addr)

	// Graceful shutdown
	go func() {
		if err := router.Run(addr); err != nil {
			log.Fatalf("Failed to start server: %v", err)
		}
	}()

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
	); err != nil {
		return err
	}

	return nil
}
