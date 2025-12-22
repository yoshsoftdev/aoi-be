package scheduler

import (
	"context"
	"sync"
	"time"

	"aoi-backend/internal/core/logger"
)

// JobType represents the type of job
type JobType string

const (
	JobTypePolling JobType = "polling"
	JobTypeTest    JobType = "test"
	JobTypeWrite   JobType = "write"
)

// Job represents a job in the connection queue
type Job struct {
	Type        JobType
	ConfigID    uint
	ParameterID uint
	SlaveID     int
	ExecuteFn   func(ctx context.Context) error
	Priority    int // Lower number = higher priority (for sorting)
}

// ConnectionQueue manages a queue of jobs for a single connection
type ConnectionQueue struct {
	connectionID   uint
	connectionName string
	queue          chan Job
	workerRunning  bool
	mu             sync.Mutex
	lockCount      int // Number of active locks (test/write operations)
	lockMu         sync.Mutex
}

// NewConnectionQueue creates a new connection queue
func NewConnectionQueue(connectionID uint, connectionName string, queueSize int) *ConnectionQueue {
	return &ConnectionQueue{
		connectionID:   connectionID,
		connectionName: connectionName,
		queue:          make(chan Job, queueSize),
		lockCount:      0,
	}
}

// Enqueue adds a job to the queue
func (cq *ConnectionQueue) Enqueue(job Job) {
	log := logger.GetLogger()
	log.Debugf("[QUEUE] Enqueuing job: connection_id=%d, connection_name=%s, job_type=%s, config_id=%d, parameter_id=%d, slave_id=%d",
		cq.connectionID, cq.connectionName, job.Type, job.ConfigID, job.ParameterID, job.SlaveID)

	select {
	case cq.queue <- job:
		log.Debugf("[QUEUE] Job enqueued successfully: connection_id=%d, queue_length=%d", cq.connectionID, len(cq.queue))
	default:
		log.Warnf("[QUEUE] Queue full, dropping job: connection_id=%d, job_type=%s", cq.connectionID, job.Type)
	}
}

// Lock suspends polling for this connection (used during test/write)
func (cq *ConnectionQueue) Lock(reason string) {
	cq.lockMu.Lock()
	defer cq.lockMu.Unlock()

	cq.lockCount++
	log := logger.GetLogger()
	log.Infof("[QUEUE] Connection locked: connection_id=%d, connection_name=%s, reason=%s, lock_count=%d",
		cq.connectionID, cq.connectionName, reason, cq.lockCount)
}

// Unlock resumes polling for this connection
func (cq *ConnectionQueue) Unlock(reason string) {
	cq.lockMu.Lock()
	defer cq.lockMu.Unlock()

	if cq.lockCount > 0 {
		cq.lockCount--
	}

	log := logger.GetLogger()
	log.Infof("[QUEUE] Connection unlocked: connection_id=%d, connection_name=%s, reason=%s, lock_count=%d",
		cq.connectionID, cq.connectionName, reason, cq.lockCount)
}

// IsLocked checks if the connection is currently locked
func (cq *ConnectionQueue) IsLocked() bool {
	cq.lockMu.Lock()
	defer cq.lockMu.Unlock()
	return cq.lockCount > 0
}

// StartWorker starts the worker goroutine for this connection queue
func (cq *ConnectionQueue) StartWorker() {
	cq.mu.Lock()
	if cq.workerRunning {
		cq.mu.Unlock()
		return
	}
	cq.workerRunning = true
	cq.mu.Unlock()

	log := logger.GetLogger()
	log.Infof("[QUEUE] Starting worker: connection_id=%d, connection_name=%s", cq.connectionID, cq.connectionName)

	go func() {
		for job := range cq.queue {
			// Check if connection is locked (test/write in progress)
			if cq.IsLocked() && job.Type == JobTypePolling {
				log.Debugf("[QUEUE] Skipping polling job (connection locked): connection_id=%d, config_id=%d",
					cq.connectionID, job.ConfigID)
				continue
			}

			// Execute job
			log.Infof("[QUEUE] Executing job: connection_id=%d, connection_name=%s, job_type=%s, config_id=%d, parameter_id=%d, slave_id=%d",
				cq.connectionID, cq.connectionName, job.Type, job.ConfigID, job.ParameterID, job.SlaveID)

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			startTime := time.Now()

			err := job.ExecuteFn(ctx)

			duration := time.Since(startTime)
			cancel()

			if err != nil {
				log.Warnf("[QUEUE] Job execution failed: connection_id=%d, job_type=%s, config_id=%d, error=%v, duration=%v",
					cq.connectionID, job.Type, job.ConfigID, err, duration)
			} else {
				log.Debugf("[QUEUE] Job execution completed: connection_id=%d, job_type=%s, config_id=%d, duration=%v",
					cq.connectionID, job.Type, job.ConfigID, duration)
			}
		}

		cq.mu.Lock()
		cq.workerRunning = false
		cq.mu.Unlock()
		log.Infof("[QUEUE] Worker stopped: connection_id=%d, connection_name=%s", cq.connectionID, cq.connectionName)
	}()
}

// StopWorker stops the worker (closes the queue channel)
func (cq *ConnectionQueue) StopWorker() {
	cq.mu.Lock()
	defer cq.mu.Unlock()

	if !cq.workerRunning {
		return
	}

	close(cq.queue)
	cq.workerRunning = false
	logger.GetLogger().Infof("[QUEUE] Worker stopped: connection_id=%d, connection_name=%s", cq.connectionID, cq.connectionName)
}
