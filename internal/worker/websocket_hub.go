package worker

import (
	"encoding/json"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"aoi-backend/internal/domain"

	"github.com/gorilla/websocket"
)

const (
	// Batch configuration for optimized broadcasting (aggressively optimized for speed)
	batchWindowMs    = 0    // Immediate send (no batching delay) for fastest possible broadcast
	maxBatchSize     = 20   // Smaller batch size for faster flush
	broadcastBufSize = 2048 // Increased buffer size for high throughput
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

			// Logging disabled for performance - too frequent for production
			// Uncomment below for debugging if needed
			// var msgData map[string]interface{}
			// if err := json.Unmarshal(message, &msgData); err == nil {
			// 	if data, ok := msgData["data"].(map[string]interface{}); ok {
			// 		if _, isBatch := msgData["batch"]; isBatch {
			// 			if dataArray, ok := data["updates"].([]interface{}); ok {
			// 				logger.GetLogger().Debugf("[BROADCAST] Sending batch to %d clients: %d parameters", clientCount, len(dataArray))
			// 			}
			// 		} else {
			// 			logger.GetLogger().Debugf("[BROADCAST] Sending to %d clients: parameter_id=%v, type=%v",
			// 				clientCount, data["parameter_id"], msgData["type"])
			// 		}
			// 	}
			// }

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
	// GUARD: Ensure RawValue is nil (CLEAN data should not expose raw values)
	if data.RawValue != nil {
		data.RawValue = nil // Force remove raw value
	}

	// Logging disabled for performance - too frequent for production
	// log.Debugf("[BROADCAST] Queueing CLEAN data: parameter_id=%d, parameter_nama=%s, clean_value=%v",
	// 	data.ParameterID, data.ParameterNama, data.ProcessedValue)

	// Create DashboardData payload compatible with frontend
	// Frontend expects: parameter_id, device_nama, parameter_nama, value, timestamp, status
	// Convert timestamp to string format for JSON compatibility
	// CRITICAL: When status is offline, value should be null (not 0)
	// CRITICAL: Value must ALWAYS be numeric (never string) for broadcast
	var value interface{} = data.ProcessedValue

	// Convert string to numeric if somehow ProcessedValue is still string
	if strVal, ok := value.(string); ok {
		// Try to parse as number
		if parsedNum, err := strconv.ParseFloat(strVal, 64); err == nil {
			value = parsedNum
		} else {
			// Cannot parse, default to 0
			value = float64(0)
		}
	}

	if value == nil && data.Status != "offline" {
		// Only default to 0 if status is not offline
		value = float64(0)
	}
	// If status is offline and value is nil, keep it as nil (null in JSON)

	// CRITICAL: Ensure status is never empty - default to "offline" if empty
	status := data.Status
	if status == "" {
		status = "offline"
	}

	// CRITICAL: Ensure alarmstatus is never empty - default to "normal" if empty
	alarmStatus := data.AlarmStatus
	if alarmStatus == "" {
		alarmStatus = domain.AlarmStatusNormal
	}

	// CRITICAL: Final validation - value must be numeric (never string)
	// Double check to ensure no string slips through
	var finalValue interface{} = value
	if _, ok := value.(string); ok {
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
		case float32:
			// Round to 2 decimal places
			roundedValue = float32(math.Round(float64(v)*100) / 100)
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			// Integer values don't need rounding, keep as is
			roundedValue = finalValue
		default:
			// For other types, try to convert to float64 and round
			if numVal, ok := convertToFloat64(finalValue); ok {
				roundedValue = math.Round(numVal*100) / 100
			} else {
				// Cannot convert, keep original
				roundedValue = finalValue
			}
		}
	} else {
		// Keep nil for offline status
		roundedValue = finalValue
	}

	dashboardData := map[string]interface{}{
		"parameter_id":   data.ParameterID,
		"device_nama":    data.DeviceNama,                     // Device name
		"parameter_nama": data.ParameterNama,                  // Logical parameter name (not raw naming)
		"value":          roundedValue,                        // Final value (ALWAYS numeric, never string, rounded to 2 decimals)
		"timestamp":      data.Timestamp.Format(time.RFC3339), // Convert to RFC3339 string
		"status":         status,                              // Status (never empty, defaults to "offline")
		"alarmstatus":    alarmStatus,                         // Alarm status (never empty, defaults to "normal")
	}

	// Add direction if available
	if data.Direction != "" {
		dashboardData["direction"] = data.Direction
	}

	// Format single message: {"data":{...},"type":"data_update"}
	// Urutan field: data, type (sesuai contoh di ws-respon.md)
	message := map[string]interface{}{
		"data": dashboardData,
		"type": "data_update",
	}

	bytes, err := json.Marshal(message)
	if err != nil {
		return
	}

	// Send to batch processor instead of direct broadcast
	// This allows batching multiple updates together for better performance
	select {
	case h.batchUpdates <- data:
		// Successfully queued for batch processing
	default:
		// Batch channel full, fallback to immediate broadcast
		h.broadcast <- bytes
	}
}

// processBatches collects parameter updates and sends them in batches
// This significantly reduces WebSocket overhead when broadcasting many parameters
func (h *WebSocketHub) processBatches() {
	for update := range h.batchUpdates {
		h.batchMu.Lock()
		h.batchBuffer = append(h.batchBuffer, update)
		batchSize := len(h.batchBuffer)

		// If batch is full, send immediately
		if batchSize >= maxBatchSize {
			h.batchMu.Unlock()
			h.flushBatch()
		} else if batchWindowMs == 0 {
			// Immediate send if batchWindowMs is 0 (no batching delay)
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
		return
	}

	// Logging disabled for performance - uncomment for debugging
	// logger.GetLogger().Debugf("[BROADCAST] Flushing batch: %d parameters, %d bytes", len(updates), len(bytes))
	h.broadcast <- bytes
}

// prepareDashboardData converts RealtimeData to dashboard format
// Returns nil if data should be skipped
func (h *WebSocketHub) prepareDashboardData(data domain.RealtimeData) map[string]interface{} {
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
	if _, ok := value.(string); ok {
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

	result := map[string]interface{}{
		"parameter_id":   data.ParameterID,
		"device_nama":    data.DeviceNama,
		"parameter_nama": data.ParameterNama,
		"value":          roundedValue,
		"timestamp":      data.Timestamp.Format(time.RFC3339),
		"status":         status,
		"alarmstatus":    alarmStatus,
	}

	// Add direction if available
	if data.Direction != "" {
		result["direction"] = data.Direction
	}

	return result
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
			} else {
				atomic.AddInt32(&sentCount, 1)
			}
		}(conn)
	}

	wg.Wait()

	// Logging disabled for performance - uncomment for debugging
	// if sentCount > 0 {
	// 	logger.GetLogger().Debugf("[BROADCAST] Successfully sent to %d/%d clients", sentCount, len(clients))
	// }
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
		return
	}

	h.broadcast <- bytes
}

// BroadcastAlarmRemoved broadcasts alarm removal notification to all connected WebSocket clients
func (h *WebSocketHub) BroadcastAlarmRemoved(workflowID uint) {

	alarmRemovedData := map[string]interface{}{
		"workflow_id": workflowID,
		"timestamp":   time.Now().Format(time.RFC3339),
	}

	wsMessage := map[string]interface{}{
		"type": "alarm_removed",
		"data": alarmRemovedData,
	}

	bytes, err := json.Marshal(wsMessage)
	if err != nil {
		return
	}

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
