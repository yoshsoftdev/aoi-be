package modbus

import (
	"fmt"
	"strings"
)

// formatHex formats byte array as hex string with spaces between bytes
func formatHex(data []byte) string {
	if len(data) == 0 {
		return ""
	}
	hexParts := make([]string, len(data))
	for i, b := range data {
		hexParts[i] = fmt.Sprintf("%02X", b)
	}
	return strings.Join(hexParts, " ")
}

// FormatHex is the exported version of formatHex for use in other packages
func FormatHex(data []byte) string {
	return formatHex(data)
}

// LogRequest logs Modbus request frame in hex format
func LogRequest(protocol string, functionCode int, address uint16, quantity uint16, request []byte) {
	// Silent logging
}

// LogResponse logs Modbus response frame in hex format
func LogResponse(protocol string, functionCode int, address uint16, quantity uint16, response []byte) {
	// Silent logging
}

// LogResponseData logs Modbus response data bytes (without frame header/CRC) in hex format
func LogResponseData(protocol string, functionCode int, address uint16, quantity uint16, data []byte) {
	// Silent logging
}
