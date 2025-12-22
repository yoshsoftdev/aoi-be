package scheduler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"aoi-backend/internal/core/logger"
)

// ConnectionPollingWorker manages round-robin polling for a single connection
type ConnectionPollingWorker struct {
	connectionID   uint
	connectionName string
	jobs           []PollingJob // Sorted list of all polling jobs for this connection
	currentIndex   int          // Current position in round-robin
	mu             sync.RWMutex
	lockCount      int // Number of active locks (test/write operations)
	lockMu         sync.Mutex
	stopChan       chan struct{}
	running        bool
	workerMu       sync.Mutex
}

// NewConnectionPollingWorker creates a new polling worker for a connection
func NewConnectionPollingWorker(connectionID uint, connectionName string, jobs []PollingJob) *ConnectionPollingWorker {
	return &ConnectionPollingWorker{
		connectionID:   connectionID,
		connectionName: connectionName,
		jobs:           jobs,
		currentIndex:   0,
		lockCount:      0,
		stopChan:       make(chan struct{}),
		running:        false,
	}
}

// Lock suspends polling for this connection (used during test/write)
func (w *ConnectionPollingWorker) Lock(reason string) {
	w.lockMu.Lock()
	defer w.lockMu.Unlock()

	w.lockCount++
	log := logger.GetLogger()
	log.Infof("[POLLING] Connection locked: connection_id=%d, connection_name=%s, reason=%s, lock_count=%d",
		w.connectionID, w.connectionName, reason, w.lockCount)
}

// Unlock resumes polling for this connection
func (w *ConnectionPollingWorker) Unlock(reason string) {
	w.lockMu.Lock()
	defer w.lockMu.Unlock()

	if w.lockCount > 0 {
		w.lockCount--
	}

	log := logger.GetLogger()
	log.Infof("[POLLING] Connection unlocked: connection_id=%d, connection_name=%s, reason=%s, lock_count=%d",
		w.connectionID, w.connectionName, reason, w.lockCount)
}

// IsLocked checks if the connection is currently locked
func (w *ConnectionPollingWorker) IsLocked() bool {
	w.lockMu.Lock()
	defer w.lockMu.Unlock()
	return w.lockCount > 0
}

// Start starts the round-robin polling loop
func (w *ConnectionPollingWorker) Start() {
	w.workerMu.Lock()
	if w.running {
		log := logger.GetLogger()
		log.Warnf("[POLLING] Worker already running: connection_id=%d, skipping start", w.connectionID)
		w.workerMu.Unlock()
		return
	}
	w.running = true
	w.workerMu.Unlock()

	log := logger.GetLogger()
	log.Infof("[POLLING] ===== STARTING WORKER: connection_id=%d, connection_name=%s =====", w.connectionID, w.connectionName)
	log.Infof("[POLLING] Total parameters: %d", len(w.jobs))

	if len(w.jobs) == 0 {
		log.Errorf("[POLLING] ⚠️  CRITICAL: No polling jobs for connection %d, worker will NOT start", w.connectionID)
		w.workerMu.Lock()
		w.running = false
		w.workerMu.Unlock()
		return
	}

	// Log job order
	log.Infof("[POLLING] Polling order for connection %d (%s):", w.connectionID, w.connectionName)
	for i, job := range w.jobs {
		log.Infof("[POLLING]   [%d] parameter_id=%d, slave_id=%d, config_id=%d, polling_ms=%d",
			i+1, job.ParameterID, job.SlaveID, job.ConfigID, job.PollingMs)
	}
	log.Infof("[POLLING] ===== STARTING GOROUTINE: connection_id=%d =====", w.connectionID)

	// Start goroutine
	go w.pollLoop()

	log.Infof("[POLLING] ===== GOROUTINE STARTED: connection_id=%d =====", w.connectionID)
}

// Stop stops the polling worker
func (w *ConnectionPollingWorker) Stop() {
	w.workerMu.Lock()
	if !w.running {
		w.workerMu.Unlock()
		return
	}
	w.running = false
	w.workerMu.Unlock()

	close(w.stopChan)
	logger.GetLogger().Infof("[POLLING] Stopping polling worker: connection_id=%d, connection_name=%s",
		w.connectionID, w.connectionName)
}

// pollLoop is the main polling loop - infinite round-robin
func (w *ConnectionPollingWorker) pollLoop() {
	log := logger.GetLogger()
	cycleCount := 0

	// LANGKAH 4: Log worker lifecycle start - CRITICAL LOG
	log.Infof("[POLLING] ===== WORKER LOOP STARTED: connection_id=%d, total_jobs=%d =====", w.connectionID, len(w.jobs))

	// Validate jobs array immediately
	if len(w.jobs) == 0 {
		log.Errorf("[POLLING] ⚠️  CRITICAL: Worker loop started but jobs array is EMPTY! connection_id=%d", w.connectionID)
		return
	}

	log.Infof("[POLLING] Jobs array validated: connection_id=%d, jobs_count=%d", w.connectionID, len(w.jobs))

	// CRITICAL: Add panic recovery at outer loop level to prevent worker exit
	defer func() {
		if r := recover(); r != nil {
			log.Errorf("[POLLING] CRITICAL: Worker loop panicked: connection_id=%d, panic=%v, cycle_count=%d",
				w.connectionID, r, cycleCount)
			// Don't exit - restart the loop
			log.Warnf("[POLLING] Attempting to restart worker loop: connection_id=%d", w.connectionID)
			go w.pollLoop() // Restart in new goroutine
		} else {
			log.Infof("[POLLING] Worker loop exited normally: connection_id=%d, cycle_count=%d",
				w.connectionID, cycleCount)
		}
	}()

	for {
		// LANGKAH 4: Log worker loop tick untuk memastikan tidak exit
		log.Debugf("[POLLING] Worker loop tick: connection_id=%d, cycle_count=%d", w.connectionID, cycleCount)

		// Check if stopped
		select {
		case <-w.stopChan:
			log.Infof("[POLLING] Polling worker stopped: connection_id=%d", w.connectionID)
			return
		default:
		}

		cycleCount++
		log.Infof("[POLLING] ===== STARTING CYCLE #%d: connection_id=%d, total_parameters=%d =====",
			cycleCount, w.connectionID, len(w.jobs))

		// LANGKAH 2: VERIFIKASI ROUND-ROBIN STATE - Get jobs and currentIndex (dideklarasikan di luar loop)
		w.mu.RLock()
		jobs := w.jobs
		currentIdx := w.currentIndex
		totalJobs := len(jobs)
		w.mu.RUnlock()

		// LANGKAH 2: Log currentIndex state
		log.Debugf("[POLLING] Round-robin state: connection_id=%d, currentIndex=%d, total_jobs=%d",
			w.connectionID, currentIdx, totalJobs)

		// CRITICAL CHECK: Validate jobs array is not empty
		if totalJobs == 0 {
			log.Warnf("[POLLING] ⚠️  No jobs available, worker will wait: connection_id=%d, cycle_count=%d", w.connectionID, cycleCount)
			time.Sleep(1 * time.Second)
			continue
		}

		// CRITICAL CHECK: Validate jobs array is not nil
		if jobs == nil {
			log.Errorf("[POLLING] ⚠️  CRITICAL: Jobs array is nil! connection_id=%d, cycle_count=%d", w.connectionID, cycleCount)
			time.Sleep(1 * time.Second)
			continue
		}

		// Round-robin through all jobs
		log.Infof("[POLLING] Entering inner loop: connection_id=%d, cycle=%d, totalJobs=%d, currentIdx=%d",
			w.connectionID, cycleCount, totalJobs, currentIdx)

		for i := 0; i < totalJobs; i++ {
			// LANGKAH 4: Log worker loop tick dalam inner loop - CRITICAL LOG
			log.Infof("[POLLING] Inner loop iteration: connection_id=%d, cycle=%d, iteration=%d/%d, currentIdx=%d",
				w.connectionID, cycleCount, i+1, totalJobs, currentIdx)

			// Check if stopped
			select {
			case <-w.stopChan:
				log.Infof("[POLLING] Worker stopped during execution: connection_id=%d", w.connectionID)
				return
			default:
			}

			// LANGKAH 3: VERIFIKASI LOCK - Log lock_count BEFORE execute
			lockCountBefore := 0
			w.lockMu.Lock()
			lockCountBefore = w.lockCount
			w.lockMu.Unlock()
			log.Debugf("[POLLING] Lock state BEFORE execute: connection_id=%d, lock_count=%d",
				w.connectionID, lockCountBefore)

			// Wait until not suspended (locked)
			lockWaitCount := 0
			for w.IsLocked() {
				lockWaitCount++
				if lockWaitCount%10 == 0 { // Log every 1 second (10 * 100ms)
					log.Debugf("[POLLING] Polling suspended (connection locked): connection_id=%d, waiting... (waited %d times)",
						w.connectionID, lockWaitCount)
				}
				time.Sleep(100 * time.Millisecond) // Check every 100ms

				select {
				case <-w.stopChan:
					log.Infof("[POLLING] Worker stopped while waiting for lock: connection_id=%d", w.connectionID)
					return
				default:
				}
			}

			// LANGKAH 2: Get current job (round-robin) - Calculate jobIndex
			jobIndex := (currentIdx + i) % totalJobs
			nextIndex := (jobIndex + 1) % totalJobs

			// LANGKAH 2: Log round-robin calculation
			log.Debugf("[POLLING] Round-robin calculation: connection_id=%d, currentIdx=%d, i=%d, jobIndex=%d, nextIndex=%d, totalJobs=%d",
				w.connectionID, currentIdx, i, jobIndex, nextIndex, totalJobs)

			// Validate jobIndex
			if jobIndex < 0 || jobIndex >= totalJobs {
				log.Errorf("[POLLING] Invalid jobIndex: connection_id=%d, jobIndex=%d, totalJobs=%d",
					w.connectionID, jobIndex, totalJobs)
				// LANGKAH 5: Error handling - continue, don't return
				continue
			}

			job := jobs[jobIndex]
			nextJob := jobs[nextIndex]

			log.Infof("[POLLING] ===== POLLING PARAMETER: connection_id=%d, parameter_id=%d, slave_id=%d, config_id=%d (cycle #%d, job %d/%d, currentIndex=%d→%d, next: parameter_id=%d) =====",
				w.connectionID, job.ParameterID, job.SlaveID, job.ConfigID, cycleCount, i+1, totalJobs, currentIdx, nextIndex, nextJob.ParameterID)

			// Execute polling job
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			startTime := time.Now()

			var err error
			func() {
				defer func() {
					if r := recover(); r != nil {
						log.Errorf("[POLLING] Job panicked: connection_id=%d, parameter_id=%d, panic=%v",
							w.connectionID, job.ParameterID, r)
						err = fmt.Errorf("job panicked: %v", r)
					}
				}()
				err = job.ExecuteFn(ctx)
			}()

			duration := time.Since(startTime)
			cancel()

			// LANGKAH 3: VERIFIKASI LOCK - Log lock_count AFTER execute
			lockCountAfter := 0
			w.lockMu.Lock()
			lockCountAfter = w.lockCount
			w.lockMu.Unlock()
			log.Debugf("[POLLING] Lock state AFTER execute: connection_id=%d, lock_count=%d (should be same as before: %d)",
				w.connectionID, lockCountAfter, lockCountBefore)

			// LANGKAH 5: VERIFIKASI ERROR HANDLING - Error TIDAK menghentikan loop
			if err != nil {
				log.Warnf("[POLLING] Polling failed: connection_id=%d, parameter_id=%d, error=%v, duration=%v",
					w.connectionID, job.ParameterID, err, duration)
				// LANGKAH 5: Continue, don't return - Error tidak menghentikan loop
				// currentIndex tetap di-update meskipun error
			} else {
				log.Debugf("[POLLING] Polling completed: connection_id=%d, parameter_id=%d, duration=%v",
					w.connectionID, job.ParameterID, duration)
			}

			// LANGKAH 2: Update current index for next cycle - TIDAK di-reset saat error/lock
			w.mu.Lock()
			oldIndex := w.currentIndex
			w.currentIndex = nextIndex // Update ke nextIndex yang sudah dihitung
			newIndex := w.currentIndex
			w.mu.Unlock()

			// LANGKAH 2: Log index update
			log.Debugf("[POLLING] Index updated: connection_id=%d, oldIndex=%d, newIndex=%d, totalJobs=%d",
				w.connectionID, oldIndex, newIndex, totalJobs)

			// LANGKAH 6: VERIFIKASI SLEEP - Sleep setelah update index, tidak di luar loop
			pollingInterval := time.Duration(job.PollingMs) * time.Millisecond
			if pollingInterval == 0 {
				pollingInterval = 1 * time.Second // Default 1 second
			}

			log.Debugf("[POLLING] Sleeping %v before next parameter (connection_id=%d)", pollingInterval, w.connectionID)

			// LANGKAH 6: Sleep dengan stop check - tidak block selamanya
			select {
			case <-w.stopChan:
				log.Infof("[POLLING] Worker stopped during sleep: connection_id=%d", w.connectionID)
				return
			case <-time.After(pollingInterval):
				// Continue to next parameter
				log.Debugf("[POLLING] Sleep completed, continuing to next parameter: connection_id=%d", w.connectionID)
			}
		}

		// LANGKAH 2: Update currentIndex setelah cycle selesai (untuk cycle berikutnya)
		w.mu.RLock()
		finalIndex := w.currentIndex
		w.mu.RUnlock()

		log.Infof("[POLLING] ===== COMPLETED CYCLE #%d: connection_id=%d, all %d parameters polled, finalIndex=%d =====",
			cycleCount, w.connectionID, totalJobs, finalIndex)

		// CRITICAL: Add small delay between cycles to prevent tight loop
		// This also helps with debugging - we can see cycle boundaries clearly
		select {
		case <-w.stopChan:
			log.Infof("[POLLING] Worker stopped between cycles: connection_id=%d", w.connectionID)
			return
		case <-time.After(100 * time.Millisecond):
			// Continue to next cycle
			log.Debugf("[POLLING] Proceeding to next cycle: connection_id=%d", w.connectionID)
		}
	}
}

// UpdateJobs updates the job list (for reload)
// NOTE: currentIndex TIDAK di-reset ke 0 untuk menjaga round-robin state
func (w *ConnectionPollingWorker) UpdateJobs(jobs []PollingJob) {
	w.mu.Lock()
	defer w.mu.Unlock()

	oldJobsCount := len(w.jobs)
	oldIndex := w.currentIndex

	w.jobs = jobs

	// LANGKAH 2: Validasi currentIndex tidak di-reset ke 0
	// Jika jumlah jobs berubah, adjust index tapi jangan reset ke 0
	if len(jobs) > 0 && oldIndex >= len(jobs) {
		// Jika index lama >= jumlah jobs baru, wrap around
		w.currentIndex = oldIndex % len(jobs)
		logger.GetLogger().Infof("[POLLING] Adjusted currentIndex due to job count change: connection_id=%d, oldIndex=%d, newIndex=%d, oldJobsCount=%d, newJobsCount=%d",
			w.connectionID, oldIndex, w.currentIndex, oldJobsCount, len(jobs))
	} else {
		// Keep currentIndex as is (TIDAK reset ke 0)
		logger.GetLogger().Infof("[POLLING] Updated job list (keeping currentIndex): connection_id=%d, total_jobs=%d, currentIndex=%d (was %d jobs, index %d)",
			w.connectionID, len(jobs), w.currentIndex, oldJobsCount, oldIndex)
	}
}
