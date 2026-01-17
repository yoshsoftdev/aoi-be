package service

import "aoi-backend/internal/domain"

// WebSocketBroadcaster is an interface for broadcasting data updates via WebSocket
// This interface breaks the circular dependency between service and worker packages
type WebSocketBroadcaster interface {
	BroadcastDataUpdate(data domain.RealtimeData)
	BroadcastAlarm(workflowID uint, workflowName string, message string)
	BroadcastAlarmRemoved(workflowID uint)
}
