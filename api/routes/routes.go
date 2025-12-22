package routes

import (
	"fmt"
	"io/fs"
	"net/http"
	"path/filepath"
	"strings"

	"aoi-backend/api/handlers"
	"aoi-backend/api/middleware"
	"aoi-backend/internal/core/cache"
	"aoi-backend/internal/embed"
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
		paramHandler := handlers.NewParameterHandler(paramService, writeService, schedulerManager, formulaEngine)
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
			workflow.GET("", workflowHandler.GetAll)
			workflow.GET("/:id", workflowHandler.GetByID)
			workflow.POST("", workflowHandler.Create)
			workflow.PATCH("/:id", workflowHandler.Update)
			workflow.DELETE("/:id", workflowHandler.Delete)
		}
	}

	// Serve static files (embedded frontend)
	staticFS := embed.GetStaticFiles()

	// Create a file server from embedded files
	fileSystem, err := fs.Sub(staticFS, ".")
	if err == nil {
		// Serve static files
		router.StaticFS("/static", http.FS(fileSystem))

		// Serve index.html for all non-API routes (SPA routing)
		router.NoRoute(func(c *gin.Context) {
			path := c.Request.URL.Path

			// If it's an API route, return 404 JSON
			if strings.HasPrefix(path, "/api/") {
				c.JSON(404, gin.H{
					"success": false,
					"error":   fmt.Sprintf("Route not found: %s %s", c.Request.Method, path),
				})
				return
			}

			// Try to serve the file from embedded filesystem
			file, err := fileSystem.Open(strings.TrimPrefix(path, "/"))
			if err == nil {
				defer file.Close()

				// Check if it's a directory
				stat, err := file.Stat()
				if err == nil && stat.IsDir() {
					// Try index.html in directory
					indexPath := filepath.Join(strings.TrimPrefix(path, "/"), "index.html")
					indexFile, err := fileSystem.Open(indexPath)
					if err == nil {
						defer indexFile.Close()
						indexStat, _ := indexFile.Stat()
						c.DataFromReader(http.StatusOK, indexStat.Size(), "text/html", indexFile, nil)
						return
					}
				} else {
					// Serve the file
					c.DataFromReader(http.StatusOK, stat.Size(), getContentType(path), file, nil)
					return
				}
			}

			// Fallback to index.html for SPA routing
			indexFile, err := fileSystem.Open("index.html")
			if err == nil {
				defer indexFile.Close()
				stat, _ := indexFile.Stat()
				c.DataFromReader(http.StatusOK, stat.Size(), "text/html", indexFile, nil)
				return
			}

			// Last resort: 404
			c.JSON(404, gin.H{
				"success": false,
				"error":   "Not found",
			})
		})
	} else {
		// If embedding fails, fallback to 404 handler
		router.NoRoute(func(c *gin.Context) {
			if strings.HasPrefix(c.Request.URL.Path, "/api/") {
				c.JSON(404, gin.H{
					"success": false,
					"error":   fmt.Sprintf("Route not found: %s %s", c.Request.Method, c.Request.URL.Path),
				})
			} else {
				c.JSON(404, gin.H{
					"success": false,
					"error":   "Frontend not available",
				})
			}
		})
	}

	return router
}

// getContentType returns the content type based on file extension
func getContentType(path string) string {
	ext := filepath.Ext(path)
	switch ext {
	case ".html":
		return "text/html"
	case ".css":
		return "text/css"
	case ".js":
		return "application/javascript"
	case ".json":
		return "application/json"
	case ".png":
		return "image/png"
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".gif":
		return "image/gif"
	case ".svg":
		return "image/svg+xml"
	case ".ico":
		return "image/x-icon"
	case ".woff":
		return "font/woff"
	case ".woff2":
		return "font/woff2"
	case ".ttf":
		return "font/ttf"
	case ".eot":
		return "application/vnd.ms-fontobject"
	default:
		return "application/octet-stream"
	}
}
