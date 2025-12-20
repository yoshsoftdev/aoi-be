package config

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// GetSerialPort returns the appropriate serial port path based on OS and dev mode
// In dev mode (Mac), converts Linux paths like /dev/ttyUSB0 to Mac-compatible paths
func GetSerialPort(portPath string, devMode bool) string {
	if !devMode {
		return portPath
	}

	// Only apply conversion on Mac in dev mode
	if runtime.GOOS != "darwin" {
		return portPath
	}

	// If port path is already a Mac path, return as is
	if strings.HasPrefix(portPath, "/dev/tty.") || strings.HasPrefix(portPath, "/dev/cu.") {
		return portPath
	}

	// Convert Linux USB TTY paths to Mac-compatible paths
	// /dev/ttyUSB0 -> /dev/tty.usbserial-* or /dev/tty.*
	if strings.HasPrefix(portPath, "/dev/ttyUSB") {
		fmt.Printf("Dev mode: Converting Linux USB port %s to Mac-compatible path\n", portPath)

		// Try common Mac USB serial port patterns
		macPorts := []string{
			"/dev/tty.usbserial",
			"/dev/tty.usbserial-A",
			"/dev/tty.usbserial-B",
			"/dev/tty.usbmodem",
			"/dev/tty.usbmodem1",
			"/dev/tty.usbmodem1411", // Common Arduino port
		}

		// Check if any of these ports exist
		for _, macPort := range macPorts {
			if _, err := os.Stat(macPort); err == nil {
				fmt.Printf("Dev mode: Found Mac USB port: %s\n", macPort)
				return macPort
			}
		}

		// If no exact match, try to find any USB serial port
		// List common Mac serial port locations
		devDirs := []string{"/dev"}
		for _, devDir := range devDirs {
			entries, err := os.ReadDir(devDir)
			if err != nil {
				continue
			}

			for _, entry := range entries {
				name := entry.Name()
				// Match Mac USB serial port patterns
				if strings.HasPrefix(name, "tty.usbserial") ||
					strings.HasPrefix(name, "tty.usbmodem") ||
					strings.HasPrefix(name, "cu.usbserial") ||
					strings.HasPrefix(name, "cu.usbmodem") {
					fullPath := filepath.Join(devDir, name)
					if _, err := os.Stat(fullPath); err == nil {
						fmt.Printf("Dev mode: Found Mac USB port: %s\n", fullPath)
						return fullPath
					}
				}
			}
		}

		// Fallback: return first available Mac-style port or original if none found
		fmt.Printf("Dev mode: No Mac USB port found, using fallback: /dev/tty.usbserial\n")
		return "/dev/tty.usbserial"
	}

	// For other paths, return as is
	return portPath
}
