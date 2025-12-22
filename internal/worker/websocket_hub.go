package worker

import (
	"encoding/json"
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
// IMPORTANT: Only formula parameters should call this - raw parameters are internal only
func (h *WebSocketHub) BroadcastDataUpdate(data domain.RealtimeData) {
	log := logger.GetLogger()

	// GUARD: Only broadcast formula parameters (clean data)
	// Raw parameters should never reach here
	// This is a safety check - workers should not call this
	log.Infof("[BROADCAST] BroadcastDataUpdate called: parameter_id=%d, parameter_nama=%s, parameter_type=%s, value=%v, device_nama=%s",
		data.ParameterID, data.ParameterNama, data.ParameterType, data.ProcessedValue, data.DeviceNama)

	// Create DashboardData payload compatible with frontend
	// Frontend expects: parameter_id, device_nama, parameter_nama, value, timestamp, status
	// Convert timestamp to string format for JSON compatibility
	dashboardData := map[string]interface{}{
		"parameter_id":   data.ParameterID,
		"device_nama":    data.DeviceNama,                     // Device name
		"parameter_nama": data.ParameterNama,                  // Logical parameter name (not raw naming)
		"value":          data.ProcessedValue,                 // Final value only (after decode, scale, formula)
		"timestamp":      data.Timestamp.Format(time.RFC3339), // Convert to RFC3339 string
		"status":         data.Status,
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
