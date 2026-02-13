package scheduler

import (
	"context"
	"sync"
	"time"
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
}

// Unlock resumes polling for this connection
func (w *ConnectionPollingWorker) Unlock(reason string) {
	w.lockMu.Lock()
	defer w.lockMu.Unlock()

	if w.lockCount > 0 {
		w.lockCount--
	}
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
		w.workerMu.Unlock()
		return
	}
	w.running = true
	w.workerMu.Unlock()

	if len(w.jobs) == 0 {
		w.workerMu.Lock()
		w.running = false
		w.workerMu.Unlock()
		return
	}

	// Start goroutine
	go w.pollLoop()
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
}

// pollLoop is the main polling loop - infinite round-robin
func (w *ConnectionPollingWorker) pollLoop() {
	cycleCount := 0

	// Validate jobs array immediately
	if len(w.jobs) == 0 {
		return
	}

	// CRITICAL: Add panic recovery at outer loop level to prevent worker exit
	defer func() {
		if r := recover(); r != nil {
			// Don't exit - restart the loop
			go w.pollLoop() // Restart in new goroutine
		}
	}()

	for {
		// Check if stopped
		select {
		case <-w.stopChan:
			return
		default:
		}

		cycleCount++

		// LANGKAH 2: VERIFIKASI ROUND-ROBIN STATE - Get jobs and currentIndex (dideklarasikan di luar loop)
		w.mu.RLock()
		jobs := w.jobs
		currentIdx := w.currentIndex
		totalJobs := len(jobs)
		w.mu.RUnlock()

		// CRITICAL CHECK: Validate jobs array is not empty
		if totalJobs == 0 {
			time.Sleep(1 * time.Second)
			continue
		}

		// CRITICAL CHECK: Validate jobs array is not nil
		if jobs == nil {
			time.Sleep(1 * time.Second)
			continue
		}

		for i := 0; i < totalJobs; i++ {
			// Check if stopped
			select {
			case <-w.stopChan:
				return
			default:
			}

			// Wait until not suspended (locked)
			for w.IsLocked() {
				time.Sleep(100 * time.Millisecond) // Check every 100ms

				select {
				case <-w.stopChan:
					return
				default:
				}
			}

			// Get current job (round-robin) - Calculate jobIndex
			jobIndex := (currentIdx + i) % totalJobs
			nextIndex := (jobIndex + 1) % totalJobs

			// Validate jobIndex
			if jobIndex < 0 || jobIndex >= totalJobs {
				continue
			}

			job := jobs[jobIndex]

			// Execute polling job
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

			func() {
				defer func() {
					if r := recover(); r != nil {
						// Silent panic recovery
					}
				}()
				_ = job.ExecuteFn(ctx)
			}()

			cancel()

			// Update current index for next cycle - TIDAK di-reset saat error/lock
			w.mu.Lock()
			w.currentIndex = nextIndex // Update ke nextIndex yang sudah dihitung
			w.mu.Unlock()

			// LANGKAH 6: VERIFIKASI SLEEP - Sleep setelah update index, tidak di luar loop
			pollingInterval := time.Duration(job.PollingMs) * time.Millisecond
			if pollingInterval == 0 {
				pollingInterval = 1 * time.Second // Default 1 second
			}

			// Sleep dengan stop check - tidak block selamanya
			select {
			case <-w.stopChan:
				return
			case <-time.After(pollingInterval):
				// Continue to next parameter
			}
		}

		// Reduced delay between cycles for faster polling (was 100ms, now 10ms)
		select {
		case <-w.stopChan:
			return
		case <-time.After(10 * time.Millisecond):
			// Continue to next cycle
		}
	}
}

// UpdateJobs updates the job list (for reload)
// NOTE: currentIndex TIDAK di-reset ke 0 untuk menjaga round-robin state
func (w *ConnectionPollingWorker) UpdateJobs(jobs []PollingJob) {
	w.mu.Lock()
	defer w.mu.Unlock()

	oldIndex := w.currentIndex

	w.jobs = jobs

	// Validasi currentIndex tidak di-reset ke 0
	// Jika jumlah jobs berubah, adjust index tapi jangan reset ke 0
	if len(jobs) > 0 && oldIndex >= len(jobs) {
		// Jika index lama >= jumlah jobs baru, wrap around
		w.currentIndex = oldIndex % len(jobs)
	}
}
