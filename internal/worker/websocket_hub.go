package worker

import (
	"encoding/json"
	"strconv"
	"sync"
	"time"

	"aoi-backend/internal/core/logger"
	"aoi-backend/internal/domain"

	"github.com/gorilla/websocket"
)

type WebSocketHub struct {
	clients    map[*websocket.Conn]bool
	broadcast  chan []byte
	register   chan *websocket.Conn
	unregister chan *websocket.Conn
	mu         sync.RWMutex
}

func NewWebSocketHub() *WebSocketHub {
	return &WebSocketHub{
		clients:    make(map[*websocket.Conn]bool),
		broadcast:  make(chan []byte, 256),
		register:   make(chan *websocket.Conn),
		unregister: make(chan *websocket.Conn),
	}
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

			// Log broadcast details
			var msgData map[string]interface{}
			if err := json.Unmarshal(message, &msgData); err == nil {
				if data, ok := msgData["data"].(map[string]interface{}); ok {
					paramID, _ := data["parameter_id"].(float64)
					paramName, _ := data["parameter_nama"].(string)
					value := data["value"]
					logger.GetLogger().Infof("[BROADCAST] Sending to %d clients: parameter_id=%v, parameter_nama=%s, value=%v, type=%v",
						clientCount, uint(paramID), paramName, value, msgData["type"])
				}
			}

			// Send message to all clients
			sentCount := 0
			for _, conn := range clients {
				err := conn.WriteMessage(websocket.TextMessage, message)
				if err != nil {
					h.mu.Lock()
					delete(h.clients, conn)
					h.mu.Unlock()
					conn.Close()
					logger.GetLogger().Warnf("[BROADCAST] Failed to send to client, removed: %v", err)
				} else {
					sentCount++
				}
			}
			if sentCount > 0 {
				logger.GetLogger().Debugf("[BROADCAST] Successfully sent to %d/%d clients", sentCount, clientCount)
			}
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

	log.Infof("[BROADCAST] Broadcasting CLEAN data: parameter_id=%d, parameter_nama=%s, parameter_type=%s, clean_value=%v, device_nama=%s",
		data.ParameterID, data.ParameterNama, data.ParameterType, data.ProcessedValue, data.DeviceNama)

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

	// Log final value type for debugging
	log.Debugf("[BROADCAST] Parameter %d: Final value type=%T, value=%v", data.ParameterID, finalValue, finalValue)

	dashboardData := map[string]interface{}{
		"parameter_id":   data.ParameterID,
		"device_nama":    data.DeviceNama,                     // Device name
		"parameter_nama": data.ParameterNama,                  // Logical parameter name (not raw naming)
		"value":          finalValue,                          // Final value (ALWAYS numeric, never string)
		"timestamp":      data.Timestamp.Format(time.RFC3339), // Convert to RFC3339 string
		"status":         status,                              // Status (never empty, defaults to "offline")
		"alarmstatus":    alarmStatus,                         // Alarm status (never empty, defaults to "normal")
	}

	message := map[string]interface{}{
		"type": "data_update",
		"data": dashboardData,
	}

	bytes, err := json.Marshal(message)
	if err != nil {
		log.Errorf("[BROADCAST] Failed to marshal broadcast message: %v", err)
		return
	}

	log.Debugf("[BROADCAST] Queuing broadcast message: %d bytes, parameter_id=%d", len(bytes), data.ParameterID)
	h.broadcast <- bytes
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
