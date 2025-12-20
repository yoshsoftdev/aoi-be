package routes

import (
	"fmt"

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
			devices.GET("", deviceHandler.GetAll)
			devices.GET("/type", deviceHandler.GetByType)
			devices.GET("/:id", deviceHandler.GetByID)
			devices.POST("", deviceHandler.Create)
			devices.PATCH("/:id", deviceHandler.Update)
			devices.DELETE("/:id", deviceHandler.Delete)
			devices.POST("/:id/test-connection", deviceHandler.TestConnection)
		}

		// Modbus RTU
		modbusHandler := handlers.NewModbusHandler(modbusRTURepo, modbusTCPRepo, schedulerManager)

		// Parameters
		// Initialize WriteService (requires additional dependencies)
		paramRepo := repository.NewParameterRepository()
		presetRepo := repository.NewPresetRepository()
		deviceRepo := repository.NewDeviceRepository()
		formulaEngine := service.NewFormulaEngine(
			repository.NewFormulaRepository(),
			paramRepo,
			cache,
			wsHub,
		)
		writeService := service.NewWriteService(
			paramRepo,
			presetRepo,
			modbusRTURepo,
			modbusTCPRepo,
			deviceRepo,
			cache,
			formulaEngine,
		)
		paramHandler := handlers.NewParameterHandler(paramService, writeService, schedulerManager)
		params := v1.Group("/parameters")
		{
			params.GET("", paramHandler.GetAll)
			params.GET("/:id", paramHandler.GetByID)
			params.POST("", paramHandler.Create)
			params.PATCH("/:id", paramHandler.Update)
			params.DELETE("/:id", paramHandler.Delete)
			params.POST("/:id/write", paramHandler.Write)
			params.POST("/test-connection", modbusHandler.TestConnection)
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
		mappingHandler := handlers.NewMappingHandler(schedulerManager)
		mapping := v1.Group("/mapping")
		{
			mapping.POST("/save", mappingHandler.Save)
		}

		// Write
		writeHandler := handlers.NewWriteHandler(writeService)
		write := v1.Group("/write")
		{
			write.POST("/execute", writeHandler.Execute)
		}
	}

	// 404 handler for debugging
	router.NoRoute(func(c *gin.Context) {
		c.JSON(404, gin.H{
			"success": false,
			"error":   fmt.Sprintf("Route not found: %s %s", c.Request.Method, c.Request.URL.Path),
			"available_routes": []string{
				"GET /api/v1/devices",
				"GET /api/v1/devices/:id",
				"GET /api/v1/devices/type",
				"POST /api/v1/devices",
				"PATCH /api/v1/devices/:id",
				"DELETE /api/v1/devices/:id",
			},
		})
	})

	return router
}
