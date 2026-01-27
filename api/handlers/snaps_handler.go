package handlers

import (
	"fmt"
	"net/http"

	"aoi-backend/internal/core/cache"
	"aoi-backend/internal/core/logger"
	"aoi-backend/internal/domain"
	"aoi-backend/internal/repository"

	"github.com/gin-gonic/gin"
)

type SnapsHandler struct {
	cache     cache.Cache
	paramRepo repository.ParameterRepository
}

func NewSnapsHandler(cacheInstance cache.Cache) *SnapsHandler {
	return &SnapsHandler{
		cache:     cacheInstance,
		paramRepo: repository.NewParameterRepository(),
	}
}

// GetSnaps handles GET /api/v1/snaps
// Returns snapshot of all parameter values from cache
// This allows frontend to get initial values without waiting for WebSocket
func (h *SnapsHandler) GetSnaps(c *gin.Context) {
	// Get all parameters from database
	params, err := h.paramRepo.GetAll()
	if err != nil {
		logger.GetLogger().Errorf("[SNAPS] Failed to get parameters: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   "Failed to get parameters",
		})
		return
	}

	// Collect snapshots from cache
	snapshots := make([]gin.H, 0)

	for _, param := range params {
		// Only include parameters yang tampil di Dashboard:
		// - direction = READ (READ parameter)
		// - parameter_mode = DIRECT, CLEAN, FORMULA
		// SOURCE dan WRITE parameter TIDAK muncul di dashboard.

		// Skip WRITE parameters (command/on-demand), tidak untuk dashboard
		if param.Direction == "WRITE" {
			continue
		}

		// SOURCE parameters are internal only and not exposed
		if param.ParameterMode == domain.ParameterModeSOURCE {
			continue
		}

		// Try to get clean data from cache
		cleanCacheKey := fmt.Sprintf("parameter:clean:%d", param.ID)
		var cleanData domain.RealtimeData

		if err := h.cache.Get(cleanCacheKey, &cleanData); err == nil {
			// Found clean data, add to snapshots
			snapshot := gin.H{
				"parameter_id":   param.ID,
				"parameter_nama": param.Nama,
				"device_nama":    cleanData.DeviceNama,
				"value":          cleanData.ProcessedValue,
				"status":         cleanData.Status,
				"alarmstatus":    cleanData.AlarmStatus,
				"timestamp":      cleanData.Timestamp,
			}
			
			// Add direction if available
			if cleanData.Direction != "" {
				snapshot["direction"] = cleanData.Direction
			}

			// Add unit if available
			if param.Unit != nil {
				snapshot["unit"] = *param.Unit
			}

			// Add error if available
			if cleanData.Error != nil {
				snapshot["error"] = *cleanData.Error
			}

			snapshots = append(snapshots, snapshot)
		} else {
			// No clean data in cache yet, but still include parameter with null value
			// This allows frontend to know the parameter exists
			deviceNama := param.Device.Nama

			snapshot := gin.H{
				"parameter_id":   param.ID,
				"parameter_nama": param.Nama,
				"device_nama":    deviceNama,
				"value":          nil,
				"status":         "offline",
				"alarmstatus":    "normal",
			}
			
			// Add direction if available
			if param.Direction != "" {
				snapshot["direction"] = param.Direction
			}

			if param.Unit != nil {
				snapshot["unit"] = *param.Unit
			}

			snapshots = append(snapshots, snapshot)
		}
	}

	logger.GetLogger().Infof("[SNAPS] Returning %d parameter snapshots", len(snapshots))

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data":    snapshots,
		"meta": gin.H{
			"total": len(snapshots),
		},
	})
}
