package worker

import (
	"encoding/json"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"aoi-backend/internal/core/logger"
	"aoi-backend/internal/domain"

	"github.com/gorilla/websocket"
)

const (
	// Batch configuration for optimized broadcasting
	batchWindowMs    = 50   // Collect updates for 50ms before sending batch
	maxBatchSize     = 100  // Maximum parameters per batch message
	broadcastBufSize = 1024 // Increased buffer size for high throughput
)

type WebSocketHub struct {
	clients    map[*websocket.Conn]bool
	broadcast  chan []byte
	register   chan *websocket.Conn
	unregister chan *websocket.Conn
	mu         sync.RWMutex

	// Batch broadcast optimization
	batchUpdates chan domain.RealtimeData
	batchMu      sync.Mutex
	batchBuffer  []domain.RealtimeData
	batchTimer   *time.Timer
}

func NewWebSocketHub() *WebSocketHub {
	hub := &WebSocketHub{
		clients:      make(map[*websocket.Conn]bool),
		broadcast:    make(chan []byte, broadcastBufSize),
		register:     make(chan *websocket.Conn),
		unregister:   make(chan *websocket.Conn),
		batchUpdates: make(chan domain.RealtimeData, 1024),
		batchBuffer:  make([]domain.RealtimeData, 0, maxBatchSize),
	}

	// Start batch processor
	go hub.processBatches()

	return hub
}

func (h *WebSocketHub) Run() {
	for {
		select {
		case conn := <-h.register:
			h.mu.Lock()
			h.clients[conn] = true
			h.mu.Unlock()

		case conn := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[conn]; ok {
				delete(h.clients, conn)
				conn.Close()
			}
			h.mu.Unlock()

		case message := <-h.broadcast:
			h.mu.RLock()
			clients := make([]*websocket.Conn, 0, len(h.clients))
			for conn := range h.clients {
				clients = append(clients, conn)
			}
			clientCount := len(clients)
			h.mu.RUnlock()

			if clientCount == 0 {
				continue
			}

			// Log broadcast details (only for single updates, batch logs separately)
			var msgData map[string]interface{}
			if err := json.Unmarshal(message, &msgData); err == nil {
				if data, ok := msgData["data"].(map[string]interface{}); ok {
					// Check if this is a batch update
					if _, isBatch := msgData["batch"]; isBatch {
						if dataArray, ok := data["updates"].([]interface{}); ok {
							logger.GetLogger().Debugf("[BROADCAST] Sending batch to %d clients: %d parameters", clientCount, len(dataArray))
						}
					} else {
						// Single update - only log at debug level to reduce overhead
						logger.GetLogger().Debugf("[BROADCAST] Sending to %d clients: parameter_id=%v, type=%v",
							clientCount, data["parameter_id"], msgData["type"])
					}
				}
			}

			// Send message to all clients in parallel
			h.sendToClientsParallel(clients, message)
		}
	}
}

func (h *WebSocketHub) Register(conn *websocket.Conn) {
	h.register <- conn
}

func (h *WebSocketHub) Unregister(conn *websocket.Conn) {
	h.unregister <- conn
}

// BroadcastDataUpdate broadcasts CLEAN DATA to all connected WebSocket clients
// This method implements the service.WebSocketBroadcaster interface
// CRITICAL: Only DIRECT, CLEAN, FORMULA should be broadcast - SOURCE is INTERNAL ONLY
// This method receives CLEAN data from:
//   - Workers (for DIRECT parameters after scaling)
//   - Formula Engine (for CLEAN and FORMULA parameters)
func (h *WebSocketHub) BroadcastDataUpdate(data domain.RealtimeData) {
	log := logger.GetLogger()

	// GUARD: Ensure RawValue is nil (CLEAN data should not expose raw values)
	if data.RawValue != nil {
		log.Warnf("[BROADCAST] ⚠️  WARNING: Attempting to broadcast data with RawValue set (parameter_id=%d). RawValue will be removed.", data.ParameterID)
		data.RawValue = nil // Force remove raw value
	}

	// Reduced logging overhead - only log at debug level for frequent broadcasts
	log.Debugf("[BROADCAST] Queueing CLEAN data: parameter_id=%d, parameter_nama=%s, clean_value=%v",
		data.ParameterID, data.ParameterNama, data.ProcessedValue)

	// Create DashboardData payload compatible with frontend
	// Frontend expects: parameter_id, device_nama, parameter_nama, value, timestamp, status
	// Convert timestamp to string format for JSON compatibility
	// CRITICAL: When status is offline, value should be null (not 0)
	// CRITICAL: Value must ALWAYS be numeric (never string) for broadcast
	var value interface{} = data.ProcessedValue

	// Convert string to numeric if somehow ProcessedValue is still string
	if strVal, ok := value.(string); ok {
		log.Warnf("[BROADCAST] ProcessedValue is string '%s' for parameter_id=%d, attempting to convert to numeric", strVal, data.ParameterID)
		// Try to parse as number
		if parsedNum, err := strconv.ParseFloat(strVal, 64); err == nil {
			value = parsedNum
			log.Infof("[BROADCAST] Converted string '%s' to numeric %.2f for parameter_id=%d", strVal, parsedNum, data.ParameterID)
		} else {
			// Cannot parse, default to 0
			value = float64(0)
			log.Warnf("[BROADCAST] Cannot parse string '%s' to numeric for parameter_id=%d, defaulting to 0", strVal, data.ParameterID)
		}
	}

	if value == nil && data.Status != "offline" {
		// Only default to 0 if status is not offline
		value = float64(0)
		log.Warnf("[BROADCAST] ProcessedValue is nil for parameter_id=%d (status=%s), defaulting to 0", data.ParameterID, data.Status)
	}
	// If status is offline and value is nil, keep it as nil (null in JSON)

	// CRITICAL: Ensure status is never empty - default to "offline" if empty
	status := data.Status
	if status == "" {
		status = "offline"
		log.Warnf("[BROADCAST] Status is empty for parameter_id=%d, defaulting to 'offline'", data.ParameterID)
	}

	// CRITICAL: Ensure alarmstatus is never empty - default to "normal" if empty
	alarmStatus := data.AlarmStatus
	if alarmStatus == "" {
		alarmStatus = domain.AlarmStatusNormal
		log.Warnf("[BROADCAST] AlarmStatus is empty for parameter_id=%d, defaulting to 'normal'", data.ParameterID)
	}

	// CRITICAL: Final validation - value must be numeric (never string)
	// Double check to ensure no string slips through
	var finalValue interface{} = value
	if strVal, ok := value.(string); ok {
		log.Errorf("[BROADCAST] CRITICAL: Value is still string '%s' for parameter_id=%d after conversion attempt, forcing to 0", strVal, data.ParameterID)
		finalValue = float64(0)
	} else {
		finalValue = value
	}

	// Round value to 2 decimal places if it's a float
	// Convert to float64, round to 2 decimals, then convert back to appropriate type
	var roundedValue interface{} = finalValue
	if finalValue != nil && data.Status != "offline" {
		switch v := finalValue.(type) {
		case float64:
			// Round to 2 decimal places
			roundedValue = math.Round(v*100) / 100
			log.Debugf("[BROADCAST] Parameter %d: Rounded value from %.10f to %.2f", data.ParameterID, v, roundedValue)
		case float32:
			// Round to 2 decimal places
			roundedValue = float32(math.Round(float64(v)*100) / 100)
			log.Debugf("[BROADCAST] Parameter %d: Rounded value from %.10f to %.2f", data.ParameterID, v, roundedValue)
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			// Integer values don't need rounding, keep as is
			roundedValue = finalValue
		default:
			// For other types, try to convert to float64 and round
			if numVal, ok := convertToFloat64(finalValue); ok {
				roundedValue = math.Round(numVal*100) / 100
				log.Debugf("[BROADCAST] Parameter %d: Converted and rounded value from %v to %.2f", data.ParameterID, finalValue, roundedValue)
			} else {
				// Cannot convert, keep original
				roundedValue = finalValue
			}
		}
	} else {
		// Keep nil for offline status
		roundedValue = finalValue
	}

	// Log final value type for debugging
	log.Debugf("[BROADCAST] Parameter %d: Final value type=%T, value=%v (rounded)", data.ParameterID, roundedValue, roundedValue)

	dashboardData := map[string]interface{}{
		"parameter_id":   data.ParameterID,
		"device_nama":    data.DeviceNama,                     // Device name
		"parameter_nama": data.ParameterNama,                  // Logical parameter name (not raw naming)
		"value":          roundedValue,                        // Final value (ALWAYS numeric, never string, rounded to 2 decimals)
		"timestamp":      data.Timestamp.Format(time.RFC3339), // Convert to RFC3339 string
		"status":         status,                              // Status (never empty, defaults to "offline")
		"alarmstatus":    alarmStatus,                         // Alarm status (never empty, defaults to "normal")
	}

	// Format single message: {"data":{...},"type":"data_update"}
	// Urutan field: data, type (sesuai contoh di ws-respon.md)
	message := map[string]interface{}{
		"data": dashboardData,
		"type": "data_update",
	}

	bytes, err := json.Marshal(message)
	if err != nil {
		log.Errorf("[BROADCAST] Failed to marshal broadcast message: %v", err)
		return
	}

	// Send to batch processor instead of direct broadcast
	// This allows batching multiple updates together for better performance
	select {
	case h.batchUpdates <- data:
		// Successfully queued for batch processing
	default:
		// Batch channel full, fallback to immediate broadcast
		log.Warnf("[BROADCAST] Batch channel full, sending immediate broadcast for parameter_id=%d", data.ParameterID)
		h.broadcast <- bytes
	}
}

// processBatches collects parameter updates and sends them in batches
// This significantly reduces WebSocket overhead when broadcasting many parameters
func (h *WebSocketHub) processBatches() {
	for {
		select {
		case update := <-h.batchUpdates:
			h.batchMu.Lock()
			h.batchBuffer = append(h.batchBuffer, update)
			batchSize := len(h.batchBuffer)

			// If batch is full, send immediately
			if batchSize >= maxBatchSize {
				h.batchMu.Unlock()
				h.flushBatch()
			} else {
				// Start timer for first item in batch if not already started
				if h.batchTimer == nil {
					h.batchTimer = time.AfterFunc(batchWindowMs*time.Millisecond, func() {
						h.flushBatch()
					})
				}
				h.batchMu.Unlock()
			}
		}
	}
}

// flushBatch sends all buffered updates as a single batch message
func (h *WebSocketHub) flushBatch() {
	h.batchMu.Lock()
	if len(h.batchBuffer) == 0 {
		if h.batchTimer != nil {
			h.batchTimer.Stop()
			h.batchTimer = nil
		}
		h.batchMu.Unlock()
		return
	}

	// Copy batch buffer and clear it
	batch := make([]domain.RealtimeData, len(h.batchBuffer))
	copy(batch, h.batchBuffer)
	h.batchBuffer = h.batchBuffer[:0]
	if h.batchTimer != nil {
		h.batchTimer.Stop()
		h.batchTimer = nil
	}
	h.batchMu.Unlock()

	// Convert batch to dashboard data format
	updates := make([]map[string]interface{}, 0, len(batch))
	for _, data := range batch {
		dashboardData := h.prepareDashboardData(data)
		if dashboardData != nil {
			updates = append(updates, dashboardData)
		}
	}

	if len(updates) == 0 {
		return
	}

	// Create batch message
	// Format sesuai dengan ws-respon.md:
	// - Batch: {"batch":true,"data":{"count":N,"updates":[...]},"type":"data_update_batch"}
	// - Single: {"data":{...},"type":"data_update"}
	var message map[string]interface{}
	if len(updates) == 1 {
		// Single update - use original format for backward compatibility
		// Format: {"data":{...},"type":"data_update"}
		message = map[string]interface{}{
			"data": updates[0],
			"type": "data_update",
		}
	} else {
		// Batch update - send multiple parameters in one message
		// Format: {"batch":true,"data":{"count":N,"updates":[...]},"type":"data_update_batch"}
		// Urutan field: batch, data, type (sesuai contoh di ws-respon.md)
		message = map[string]interface{}{
			"batch": true,
			"data": map[string]interface{}{
				"count":   len(updates),
				"updates": updates,
			},
			"type": "data_update_batch",
		}
	}

	bytes, err := json.Marshal(message)
	if err != nil {
		logger.GetLogger().Errorf("[BROADCAST] Failed to marshal batch message: %v", err)
		return
	}

	logger.GetLogger().Debugf("[BROADCAST] Flushing batch: %d parameters, %d bytes", len(updates), len(bytes))
	h.broadcast <- bytes
}

// prepareDashboardData converts RealtimeData to dashboard format
// Returns nil if data should be skipped
func (h *WebSocketHub) prepareDashboardData(data domain.RealtimeData) map[string]interface{} {
	log := logger.GetLogger()

	// GUARD: Ensure RawValue is nil
	if data.RawValue != nil {
		data.RawValue = nil
	}

	var value interface{} = data.ProcessedValue

	// Convert string to numeric if needed
	if strVal, ok := value.(string); ok {
		if parsedNum, err := strconv.ParseFloat(strVal, 64); err == nil {
			value = parsedNum
		} else {
			value = float64(0)
		}
	}

	if value == nil && data.Status != "offline" {
		value = float64(0)
	}

	// Ensure status is never empty
	status := data.Status
	if status == "" {
		status = "offline"
	}

	// Ensure alarmstatus is never empty
	alarmStatus := data.AlarmStatus
	if alarmStatus == "" {
		alarmStatus = domain.AlarmStatusNormal
	}

	// Final validation - value must be numeric
	var finalValue interface{} = value
	if strVal, ok := value.(string); ok {
		log.Errorf("[BROADCAST] CRITICAL: Value is still string '%s' for parameter_id=%d, forcing to 0", strVal, data.ParameterID)
		finalValue = float64(0)
	} else {
		finalValue = value
	}

	// Round value to 2 decimal places if it's a float
	var roundedValue interface{} = finalValue
	if finalValue != nil && data.Status != "offline" {
		switch v := finalValue.(type) {
		case float64:
			roundedValue = math.Round(v*100) / 100
		case float32:
			roundedValue = float32(math.Round(float64(v)*100) / 100)
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			roundedValue = finalValue
		default:
			if numVal, ok := convertToFloat64(finalValue); ok {
				roundedValue = math.Round(numVal*100) / 100
			} else {
				roundedValue = finalValue
			}
		}
	} else {
		roundedValue = finalValue
	}

	return map[string]interface{}{
		"parameter_id":   data.ParameterID,
		"device_nama":    data.DeviceNama,
		"parameter_nama": data.ParameterNama,
		"value":          roundedValue,
		"timestamp":      data.Timestamp.Format(time.RFC3339),
		"status":         status,
		"alarmstatus":    alarmStatus,
	}
}

// sendToClientsParallel sends message to all clients in parallel using goroutines
// This significantly improves performance when there are many connected clients
func (h *WebSocketHub) sendToClientsParallel(clients []*websocket.Conn, message []byte) {
	if len(clients) == 0 {
		return
	}

	var wg sync.WaitGroup
	sentCount := int32(0)
	failedCount := int32(0)

	// Send to each client in parallel
	for _, conn := range clients {
		wg.Add(1)
		go func(c *websocket.Conn) {
			defer wg.Done()

			// Use WriteMessage with timeout to prevent blocking
			err := c.WriteMessage(websocket.TextMessage, message)
			if err != nil {
				// Remove failed client
				h.mu.Lock()
				delete(h.clients, c)
				h.mu.Unlock()
				c.Close()
				atomic.AddInt32(&failedCount, 1)
				logger.GetLogger().Debugf("[BROADCAST] Failed to send to client, removed: %v", err)
			} else {
				atomic.AddInt32(&sentCount, 1)
			}
		}(conn)
	}

	wg.Wait()

	if sentCount > 0 {
		logger.GetLogger().Debugf("[BROADCAST] Successfully sent to %d/%d clients", sentCount, len(clients))
	}
}

func (h *WebSocketHub) BroadcastError(data domain.RealtimeData) {
	// Create simplified dashboard data for error
	dashboardData := map[string]interface{}{
		"parameter_id":   data.ParameterID,
		"device_nama":    data.DeviceNama,
		"parameter_nama": data.ParameterNama,
		"value":          nil,                                 // No value on error
		"timestamp":      data.Timestamp.Format(time.RFC3339), // Convert to RFC3339 string
		"status":         data.Status,
	}

	message := map[string]interface{}{
		"type": "error",
		"data": dashboardData,
	}

	bytes, err := json.Marshal(message)
	if err != nil {
		return
	}

	h.broadcast <- bytes
}

func (h *WebSocketHub) BroadcastStatusUpdate(paramID uint, status string) {
	message := map[string]interface{}{
		"type": "status_update",
		"data": map[string]interface{}{
			"parameter_id": paramID,
			"status":       status,
		},
	}

	bytes, err := json.Marshal(message)
	if err != nil {
		return
	}

	h.broadcast <- bytes
}

// BroadcastAlarm broadcasts an alarm notification to all connected WebSocket clients
func (h *WebSocketHub) BroadcastAlarm(workflowID uint, workflowName string, message string) {
	log := logger.GetLogger()
	log.Infof("[BROADCAST] Broadcasting alarm: workflow_id=%d, workflow_name=%s, message=%s", workflowID, workflowName, message)

	alarmData := map[string]interface{}{
		"workflow_id":   workflowID,
		"workflow_name": workflowName,
		"message":       message,
		"timestamp":     time.Now().Format(time.RFC3339),
	}

	wsMessage := map[string]interface{}{
		"type": "alarm",
		"data": alarmData,
	}

	bytes, err := json.Marshal(wsMessage)
	if err != nil {
		log.Errorf("[BROADCAST] Failed to marshal alarm message: %v", err)
		return
	}

	log.Debugf("[BROADCAST] Queuing alarm message: %d bytes, workflow_id=%d", len(bytes), workflowID)
	h.broadcast <- bytes
}

// convertToFloat64 attempts to convert various numeric types to float64
func convertToFloat64(value interface{}) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int8:
		return float64(v), true
	case int16:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint:
		return float64(v), true
	case uint8:
		return float64(v), true
	case uint16:
		return float64(v), true
	case uint32:
		return float64(v), true
	case uint64:
		return float64(v), true
	default:
		return 0, false
	}
}
