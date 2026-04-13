package service

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"time"
)

// sendAlarmMessageToHTTPEndpoint sends alarm message to external HTTP endpoint.
// This helper is shared by workflow and threshold alarms.
func sendAlarmMessageToHTTPEndpoint(message string) {
	// HTTP endpoint URL (can be moved to config later)
	endpointURL := "http://localhost:8085/send"

	// Prepare request payload
	payload := map[string]string{
		"message": message,
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		// Silent fail on marshal error
		return
	}

	// Create HTTP request with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST", endpointURL, bytes.NewBuffer(jsonData))
	if err != nil {
		// Silent fail on request creation
		return
	}

	req.Header.Set("Content-Type", "application/json")

	// Send request
	client := &http.Client{
		Timeout: 5 * time.Second,
	}

	resp, err := client.Do(req)
	if err != nil {
		// Silent fail on HTTP error
		return
	}
	defer resp.Body.Close()
	// Status code is ignored; failures are silent
}
