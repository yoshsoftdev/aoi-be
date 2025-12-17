package worker

import (
	"encoding/json"
	"sync"

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
			h.mu.RUnlock()

			// Send message to all clients
			for _, conn := range clients {
				err := conn.WriteMessage(websocket.TextMessage, message)
				if err != nil {
					h.mu.Lock()
					delete(h.clients, conn)
					h.mu.Unlock()
					conn.Close()
				}
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

func (h *WebSocketHub) BroadcastDataUpdate(data domain.RealtimeData) {
	// Create simplified dashboard data (only device name, parameter name, and final value)
	dashboardData := domain.DashboardData{
		ParameterID:   data.ParameterID,   // Include parameter ID for edit/delete operations
		DeviceNama:    data.DeviceNama,
		ParameterNama: data.ParameterNama,
		Value:         data.ProcessedValue, // Final processed value
		Timestamp:     data.Timestamp,
		Status:        data.Status,
	}

	message := map[string]interface{}{
		"type": "data_update",
		"data": dashboardData,
	}

	bytes, err := json.Marshal(message)
	if err != nil {
		return
	}

	h.broadcast <- bytes
}

func (h *WebSocketHub) BroadcastError(data domain.RealtimeData) {
	// Create simplified dashboard data for error
	dashboardData := domain.DashboardData{
		ParameterID:   data.ParameterID,   // Include parameter ID for edit/delete operations
		DeviceNama:    data.DeviceNama,
		ParameterNama: data.ParameterNama,
		Value:         nil, // No value on error
		Timestamp:     data.Timestamp,
		Status:        data.Status,
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
