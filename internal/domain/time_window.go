package domain

import (
	"fmt"
	"strings"
	"time"
)

// TimeWindowConfig represents configuration for time window condition
type TimeWindowConfig struct {
	StartTime string   `json:"start_time"` // Format: "HH:MM" (e.g., "08:00")
	EndTime   string   `json:"end_time"`   // Format: "HH:MM" (e.g., "17:00")
	Days      []string `json:"days,omitempty"` // Optional: ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
}

// EvaluateTimeWindow evaluates if current time is within the time window
// Returns true if current time is within the window, false otherwise
// Uses time.Local for timezone
func EvaluateTimeWindow(cfg TimeWindowConfig) (bool, error) {
	now := time.Now().Local()

	// Parse start and end times
	startTime, err := parseTime(cfg.StartTime)
	if err != nil {
		return false, fmt.Errorf("invalid start_time format: %w", err)
	}

	endTime, err := parseTime(cfg.EndTime)
	if err != nil {
		return false, fmt.Errorf("invalid end_time format: %w", err)
	}

	// Check if current day is in allowed days (if specified)
	if len(cfg.Days) > 0 {
		currentDay := strings.ToLower(now.Weekday().String()[:3]) // "Mon", "Tue", etc.
		dayAllowed := false
		for _, day := range cfg.Days {
			if strings.ToLower(day) == currentDay {
				dayAllowed = true
				break
			}
		}
		if !dayAllowed {
			return false, nil
		}
	}

	// Get current time (hours and minutes only)
	currentTime := time.Date(0, 1, 1, now.Hour(), now.Minute(), 0, 0, time.Local)

	// Check if range crosses midnight
	if endTime.Before(startTime) || endTime.Equal(startTime) {
		// Range crosses midnight (e.g., 22:00 - 06:00)
		// Current time must be >= startTime OR <= endTime
		return currentTime.After(startTime) || currentTime.Before(endTime) || currentTime.Equal(endTime), nil
	} else {
		// Normal range (e.g., 08:00 - 17:00)
		// Current time must be >= startTime AND <= endTime
		return (currentTime.After(startTime) || currentTime.Equal(startTime)) &&
			(currentTime.Before(endTime) || currentTime.Equal(endTime)), nil
	}
}

// parseTime parses a time string in "HH:MM" format
func parseTime(timeStr string) (time.Time, error) {
	parts := strings.Split(timeStr, ":")
	if len(parts) != 2 {
		return time.Time{}, fmt.Errorf("time must be in HH:MM format")
	}

	var hour, minute int
	_, err := fmt.Sscanf(parts[0], "%d", &hour)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid hour: %w", err)
	}

	_, err = fmt.Sscanf(parts[1], "%d", &minute)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid minute: %w", err)
	}

	if hour < 0 || hour > 23 {
		return time.Time{}, fmt.Errorf("hour must be between 0 and 23")
	}

	if minute < 0 || minute > 59 {
		return time.Time{}, fmt.Errorf("minute must be between 0 and 59")
	}

	return time.Date(0, 1, 1, hour, minute, 0, 0, time.Local), nil
}
