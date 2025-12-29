package routes

import (
	"fmt"
	"strings"

	"aoi-backend/api/handlers"
	"aoi-backend/api/middleware"
	"aoi-backend/internal/core/cache"
	"aoi-backend/internal/repository"
	"aoi-backend/internal/scheduler"
	"aoi-backend/internal/service"
	"aoi-backend/internal/worker"

	"github.com/gin-gonic/gin"
)

func SetupRoutes(
	deviceService service.DeviceService,
	paramService service.ParameterService,
	modbusRTURepo repository.ModbusRTURepository,
	modbusTCPRepo repository.ModbusTCPRepository,
	snmpRepo repository.SNMPRepository,
	cache cache.Cache,
	wsHub *worker.WebSocketHub,
	schedulerManager *scheduler.SchedulerManager,
	workflowRepo repository.WorkflowRepository,
	workflowEngine *service.WorkflowEngine,
	formulaEngine *service.FormulaEngine,
) *gin.Engine {
	router := gin.Default()

	// Middleware
	router.Use(middleware.CORS())
	router.Use(middleware.Logger())

	// API v1
	v1 := router.Group("/api/v1")
	{
		// Devices
		deviceHandler := handlers.NewDeviceHandler(deviceService)
		if deviceHandler == nil {
			panic("Failed to create DeviceHandler")
		}
		devices := v1.Group("/devices")
		{
			// CRITICAL: Specific routes MUST come before parameterized routes
			devices.GET("/type", deviceHandler.GetByType) // GET /api/v1/devices/type
			// Base routes - register both empty and slash for compatibility
			devices.GET("/", deviceHandler.GetAll)  // GET /api/v1/devices/ (preferred)
			devices.GET("", deviceHandler.GetAll)   // GET /api/v1/devices (fallback)
			devices.POST("/", deviceHandler.Create) // POST /api/v1/devices/ (preferred)
			devices.POST("", deviceHandler.Create)  // POST /api/v1/devices (fallback)
			// Parameterized routes come after specific routes
			devices.GET("/:id", deviceHandler.GetByID)                         // GET /api/v1/devices/:id
			devices.PATCH("/:id", deviceHandler.Update)                        // PATCH /api/v1/devices/:id
			devices.DELETE("/:id", deviceHandler.Delete)                       // DELETE /api/v1/devices/:id
			devices.POST("/:id/test-connection", deviceHandler.TestConnection) // POST /api/v1/devices/:id/test-connection
		}

		// Modbus RTU
		modbusHandler := handlers.NewModbusHandler(modbusRTURepo, modbusTCPRepo, schedulerManager)

		// Parameters
		// Initialize WriteService (requires additional dependencies)
		paramRepo := repository.NewParameterRepository()
		presetRepo := repository.NewPresetRepository()
		deviceRepo := repository.NewDeviceRepository()
		thresholdRepo := repository.NewThresholdRepository()
		// Use the shared formulaEngine instance (passed from main.go)
		writeService := service.NewWriteService(
			paramRepo,
			presetRepo,
			modbusRTURepo,
			modbusTCPRepo,
			deviceRepo,
			cache,
			formulaEngine,
		)
		paramHandler := handlers.NewParameterHandler(paramService, writeService, schedulerManager, formulaEngine)
		params := v1.Group("/parameters")
		{
			params.GET("", paramHandler.GetAll)
			params.POST("", paramHandler.Create)
			// CRITICAL: Specific routes MUST come before parameterized routes
			// Otherwise Gin will match /test-connection as /:id
			params.POST("/test-connection", modbusHandler.TestConnection)
			// Parameterized routes come after specific routes
			params.GET("/:id", paramHandler.GetByID)
			params.PATCH("/:id", paramHandler.Update)
			params.DELETE("/:id", paramHandler.Delete)
			params.POST("/:id/write", paramHandler.Write)
		}
		modbusRTU := v1.Group("/modbus/rtu")
		{
			modbusRTU.GET("", modbusHandler.GetAllRTU)
			modbusRTU.GET("/:id", modbusHandler.GetRTUByID)
			modbusRTU.POST("", modbusHandler.CreateRTU)
			modbusRTU.PATCH("/:id", modbusHandler.UpdateRTU)
			modbusRTU.DELETE("/:id", modbusHandler.DeleteRTU)
		}

		// Modbus TCP
		modbusTCP := v1.Group("/modbus/tcp")
		{
			modbusTCP.GET("", modbusHandler.GetAllTCP)
			modbusTCP.GET("/:id", modbusHandler.GetTCPByID)
			modbusTCP.POST("", modbusHandler.CreateTCP)
			modbusTCP.PATCH("/:id", modbusHandler.UpdateTCP)
			modbusTCP.DELETE("/:id", modbusHandler.DeleteTCP)
		}

		// SNMP
		snmpHandler := handlers.NewSNMPHandler(snmpRepo, schedulerManager)
		snmp := v1.Group("/snmp")
		{
			snmp.GET("", snmpHandler.GetAll)
			snmp.GET("/:id", snmpHandler.GetByID)
			snmp.POST("", snmpHandler.Create)
			snmp.PATCH("/:id", snmpHandler.Update)
			snmp.DELETE("/:id", snmpHandler.Delete)
		}

		// Real-time data
		realtimeHandler := handlers.NewRealtimeHandler(cache)
		realtime := v1.Group("/realtime")
		{
			realtime.GET("", realtimeHandler.GetAll)
			realtime.GET("/:parameter_id", realtimeHandler.GetByParameterID)
		}

		// WebSocket
		wsHandler := handlers.NewWebSocketHandler(wsHub)
		v1.GET("/ws", wsHandler.Handle)

		// Mapping
		// Use the shared formulaEngine instance (passed from main.go)
		mappingHandler := handlers.NewMappingHandler(schedulerManager, formulaEngine)
		mapping := v1.Group("/mapping")
		{
			mapping.POST("/save", mappingHandler.Save)
		}

		// Write
		writeHandler := handlers.NewWriteHandler(writeService, schedulerManager)
		write := v1.Group("/write")
		{
			write.POST("/execute", writeHandler.Execute)
			write.POST("/test", writeHandler.TestWrite)
		}

		// Workflow
		workflowHandler := handlers.NewWorkflowHandler(workflowRepo, workflowEngine)
		workflow := v1.Group("/workflows")
		{
			// Base routes - register both empty and slash for compatibility
			workflow.GET("/", workflowHandler.GetAll)  // GET /api/v1/workflows/ (preferred)
			workflow.GET("", workflowHandler.GetAll)   // GET /api/v1/workflows (fallback)
			workflow.POST("/", workflowHandler.Create) // POST /api/v1/workflows/ (preferred)
			workflow.POST("", workflowHandler.Create)  // POST /api/v1/workflows (fallback)
			// Parameterized routes come after base routes
			workflow.GET("/:id", workflowHandler.GetByID)
			workflow.PATCH("/:id", workflowHandler.Update)
			workflow.DELETE("/:id", workflowHandler.Delete)
		}

		// Threshold (Alarm)
		thresholdHandler := handlers.NewThresholdHandler(thresholdRepo)
		threshold := v1.Group("/tracehold")
		{
			threshold.POST("", thresholdHandler.CreateOrUpdate)                // POST /api/v1/tracehold (create or update by parameter_id)
			threshold.GET("", thresholdHandler.GetAll)                         // GET /api/v1/tracehold (get all)
			threshold.GET("/:parameter_id", thresholdHandler.GetByParameterID) // GET /api/v1/tracehold/:parameter_id (get by parameter_id)
			threshold.PUT("/:id", thresholdHandler.Update)                     // PUT /api/v1/tracehold/:id (update by threshold ID)
			threshold.DELETE("/:parameter_id", thresholdHandler.Delete)        // DELETE /api/v1/tracehold/:parameter_id (delete by parameter ID)
		}
	}

	// Frontend is served by Nginx on port 80
	// Backend only serves API endpoints
	router.NoRoute(func(c *gin.Context) {
		path := c.Request.URL.Path

		// If it's an API route, return 404 JSON
		if strings.HasPrefix(path, "/api/") {
			c.JSON(404, gin.H{
				"success": false,
				"error":   fmt.Sprintf("Route not found: %s %s", c.Request.Method, path),
			})
		} else {
			// Non-API routes should be handled by Nginx (frontend)
			c.JSON(404, gin.H{
				"success": false,
				"error":   "API endpoint not found. Frontend is served by Nginx on port 80.",
			})
		}
	})

	return router
}
