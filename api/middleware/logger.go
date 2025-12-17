package middleware

import (
	"time"

	"aoi-backend/internal/core/logger"
	"github.com/gin-gonic/gin"
)

func Logger() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path
		method := c.Request.Method

		c.Next()

		latency := time.Since(start)
		status := c.Writer.Status()

		logger.GetLogger().WithFields(map[string]interface{}{
			"method":  method,
			"path":    path,
			"status":  status,
			"latency": latency,
		}).Info("HTTP Request")
	}
}

