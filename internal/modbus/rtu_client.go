package modbus

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
	"time"

	"aoi-backend/internal/core/config"
	"aoi-backend/internal/core/logger"
	"aoi-backend/internal/domain"

	"github.com/goburrow/modbus"
)

type RTUClient struct {
	handler *modbus.RTUClientHandler
	client  modbus.Client
}

func NewRTUClient(device *domain.Device, slaveID *int) (*RTUClient, error) {
	if device.Port == nil || *device.Port == "" {
		return nil, fmt.Errorf("device port is required for Modbus RTU")
	}
	if device.Baud == nil || *device.Baud == 0 {
		return nil, fmt.Errorf("device baud rate is required for Modbus RTU")
	}

	// Use parameter-level slave_id if provided, otherwise use device-level
	var finalSlaveID int
	if slaveID != nil && *slaveID > 0 {
		finalSlaveID = *slaveID
	} else if device.SlaveID != nil {
		finalSlaveID = *device.SlaveID
	} else {
		return nil, fmt.Errorf("slave ID is required for Modbus RTU (either from device or parameter)")
	}

	// Get serial port path (convert to Mac-compatible if in dev mode)
	portPath := *device.Port
	if cfg := config.Get(); cfg != nil && cfg.App.Dev {
		portPath = config.GetSerialPort(portPath, true)
	}

	handler := modbus.NewRTUClientHandler(portPath)
	handler.BaudRate = *device.Baud
	if device.DataBits != nil {
		handler.DataBits = *device.DataBits
	} else {
		handler.DataBits = 8 // default
	}
	if device.Parity != nil {
		handler.Parity = *device.Parity
	} else {
		handler.Parity = "N" // default
	}
	if device.StopBits != nil {
		handler.StopBits = *device.StopBits
	} else {
		handler.StopBits = 1 // default
	}
	handler.SlaveId = byte(finalSlaveID)

	timeout := 5 * time.Second
	if device.TimeoutMs > 0 {
		timeout = time.Duration(device.TimeoutMs) * time.Millisecond
	}
	handler.Timeout = timeout

	if err := handler.Connect(); err != nil {
		return nil, fmt.Errorf("failed to connect to Modbus RTU: %w", err)
	}

	client := modbus.NewClient(handler)

	return &RTUClient{
		handler: handler,
		client:  client,
	}, nil
}

func (c *RTUClient) Read(ctx context.Context, functionCode int, address uint16, quantity uint16) ([]byte, error) {
	log := logger.GetLogger()

	// For multiple registers, use standard library directly
	// Custom implementation disabled due to reflection issues with unexported fields
	// The standard library should handle multiple registers correctly
	if (functionCode == 3 || functionCode == 4) && quantity > 1 {
		log.Debugf("RTUClient.Read: Reading multiple registers using standard library: FC=%d, addr=%d, qty=%d", functionCode, address, quantity)

		var result []byte
		var err error

		if functionCode == 3 {
			result, err = c.client.ReadHoldingRegisters(address, quantity)
		} else if functionCode == 4 {
			result, err = c.client.ReadInputRegisters(address, quantity)
		}

		if err != nil {
			log.Errorf("RTUClient.Read: Standard library read failed: %v", err)
			return nil, fmt.Errorf("failed to read Modbus: %w", err)
		}

		// Log response (data bytes only, not full frame for standard library)
		LogResponseData("RTU", functionCode, address, quantity, result)

		log.Debugf("RTUClient.Read: Standard library read succeeded: %d bytes", len(result))
		return result, nil
	}

	var result []byte
	var err error

	switch functionCode {
	case 1: // Read Coils
		result, err = c.client.ReadCoils(address, quantity)
	case 2: // Read Discrete Inputs
		result, err = c.client.ReadDiscreteInputs(address, quantity)
	case 3: // Read Holding Registers
		result, err = c.client.ReadHoldingRegisters(address, quantity)
	case 4: // Read Input Registers
		result, err = c.client.ReadInputRegisters(address, quantity)
	default:
		return nil, fmt.Errorf("unsupported function code: %d", functionCode)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to read Modbus: %w", err)
	}

	// Log response (data bytes only, not full frame for standard library)
	LogResponseData("RTU", functionCode, address, quantity, result)

	return result, nil
}

// readMultipleRegisters implements custom read for multiple registers following the specified steps
func (c *RTUClient) readMultipleRegisters(ctx context.Context, functionCode int, address uint16, quantity uint16) ([]byte, error) {
	log := logger.GetLogger()

	// Get serial port reader/writer from handler using reflection
	handlerValue := reflect.ValueOf(c.handler).Elem()

	// Try different possible field names for serial port (including unexported fields)
	var serialPort interface{}
	var found bool

	// Try exported fields first
	possibleFieldNames := []string{"serialPort", "port", "conn", "connection", "SerialPort", "Port", "Conn"}
	for _, fieldName := range possibleFieldNames {
		field := handlerValue.FieldByName(fieldName)
		if field.IsValid() && !field.IsZero() {
			serialPort = field.Interface()
			found = true
			log.Debugf("Found serial port field: %s", fieldName)
			break
		}
	}

	// If not found, try to find field by iterating through all fields
	if !found {
		handlerType := handlerValue.Type()
		log.Debugf("Searching through %d fields in RTUClientHandler", handlerType.NumField())
		for i := 0; i < handlerType.NumField(); i++ {
			fieldType := handlerType.Field(i)
			fieldValue := handlerValue.Field(i)

			// Check if field implements io.Reader and io.Writer
			if fieldValue.CanInterface() && !fieldValue.IsZero() {
				iface := fieldValue.Interface()
				if reader, ok := iface.(io.Reader); ok {
					if _, ok := iface.(io.Writer); ok {
						// Found a field that implements both Reader and Writer
						serialPort = reader
						found = true
						log.Debugf("Found serial port field by interface check: %s", fieldType.Name)
						break
					}
				}
			}
		}
	}

	if !found {
		log.Warnf("Cannot access serial port via reflection, will fallback to standard library")
		// Return error to trigger fallback in Read() method
		return nil, fmt.Errorf("cannot access serial port via reflection")
	}

	reader, ok := serialPort.(io.Reader)
	if !ok {
		log.Warnf("Serial port does not implement io.Reader, will fallback to standard library")
		return nil, fmt.Errorf("serial port is not a reader")
	}

	writer, ok := serialPort.(io.Writer)
	if !ok {
		log.Warnf("Serial port does not implement io.Writer, will fallback to standard library")
		return nil, fmt.Errorf("serial port is not a writer")
	}

	log.Debugf("Successfully accessed serial port via reflection")

	// Build request frame
	slaveID := byte(c.handler.SlaveId)
	request := make([]byte, 8)
	request[0] = slaveID
	request[1] = byte(functionCode)
	binary.BigEndian.PutUint16(request[2:4], address)
	binary.BigEndian.PutUint16(request[4:6], quantity)

	// Calculate CRC
	crc := calculateCRC16(request[:6])
	request[6] = byte(crc & 0xFF)
	request[7] = byte(crc >> 8)

	// Log request frame (hex)
	LogRequest("RTU", functionCode, address, quantity, request)

	// Send request
	if _, err := writer.Write(request); err != nil {
		return nil, fmt.Errorf("failed to write request: %w", err)
	}

	// Step 1: Read minimal header (3 bytes: slave_id + function_code + byte_count)
	header := make([]byte, 3)
	if err := readBytesWithTimeout(reader, header, c.handler.Timeout); err != nil {
		// Fallback to standard library on read error
		if functionCode == 3 {
			return c.client.ReadHoldingRegisters(address, quantity)
		} else if functionCode == 4 {
			return c.client.ReadInputRegisters(address, quantity)
		}
		return nil, fmt.Errorf("failed to read header: %w", err)
	}

	// Validate slave ID and function code
	if header[0] != slaveID {
		return nil, fmt.Errorf("invalid slave ID in response: expected %d, got %d", slaveID, header[0])
	}

	// Check for error response (function code with MSB set)
	if header[1]&0x80 != 0 {
		errorCode := header[1] & 0x7F
		return nil, fmt.Errorf("modbus error response: function code 0x%02X, error code 0x%02X", functionCode, errorCode)
	}

	if header[1] != byte(functionCode) {
		return nil, fmt.Errorf("invalid function code in response: expected %d, got %d", functionCode, header[1])
	}

	// Step 2: Get byte count
	byteCount := header[2]
	if byteCount == 0 {
		return nil, fmt.Errorf("invalid byte count: 0")
	}

	// Validate byte count matches expected quantity
	expectedByteCount := quantity * 2
	if byteCount != byte(expectedByteCount) {
		return nil, fmt.Errorf("byte count mismatch: expected %d (quantity %d * 2), got %d", expectedByteCount, quantity, byteCount)
	}

	// Step 3: Read data + CRC (byteCount bytes of data + 2 bytes CRC)
	dataAndCRC := make([]byte, byteCount+2)
	if err := readBytesWithTimeout(reader, dataAndCRC, c.handler.Timeout); err != nil {
		return nil, fmt.Errorf("failed to read data and CRC: %w", err)
	}

	// Step 4: Combine all buffers
	fullResponse := make([]byte, 0, 3+byteCount+2)
	fullResponse = append(fullResponse, header...)
	fullResponse = append(fullResponse, dataAndCRC...)

	// Step 5: Validate CRC
	receivedCRC := uint16(fullResponse[len(fullResponse)-2]) | (uint16(fullResponse[len(fullResponse)-1]) << 8)
	calculatedCRC := calculateCRC16(fullResponse[:len(fullResponse)-2])
	if receivedCRC != calculatedCRC {
		return nil, fmt.Errorf("CRC mismatch: received 0x%04X, calculated 0x%04X", receivedCRC, calculatedCRC)
	}

	// Extract data bytes (skip header and CRC)
	data := fullResponse[3 : 3+byteCount]

	// Log response frame (hex)
	LogResponse("RTU", functionCode, address, quantity, fullResponse)

	return data, nil
}

// readBytesWithTimeout reads exactly len(buf) bytes from reader with timeout
func readBytesWithTimeout(reader io.Reader, buf []byte, timeout time.Duration) error {
	done := make(chan error, 1)
	go func() {
		_, err := io.ReadFull(reader, buf)
		done <- err
	}()

	select {
	case err := <-done:
		return err
	case <-time.After(timeout):
		return fmt.Errorf("read timeout after %v", timeout)
	}
}

// calculateCRC16 calculates Modbus RTU CRC16
func calculateCRC16(data []byte) uint16 {
	crc := uint16(0xFFFF)
	for _, b := range data {
		crc ^= uint16(b)
		for i := 0; i < 8; i++ {
			if crc&0x0001 != 0 {
				crc = (crc >> 1) ^ 0xA001
			} else {
				crc >>= 1
			}
		}
	}
	return crc
}

func (c *RTUClient) Write(ctx context.Context, functionCode int, address uint16, value uint16) error {
	var err error

	switch functionCode {
	case 5: // Write Single Coil
		_, err = c.client.WriteSingleCoil(address, value)
	case 6: // Write Single Register
		_, err = c.client.WriteSingleRegister(address, value)
	case 15: // Write Multiple Coils
		// Not implemented for single value
		return fmt.Errorf("function code 15 not supported for single write")
	case 16: // Write Multiple Registers
		// Not implemented for single value
		return fmt.Errorf("function code 16 not supported for single write")
	default:
		return fmt.Errorf("unsupported function code for write: %d", functionCode)
	}

	if err != nil {
		return fmt.Errorf("failed to write Modbus: %w", err)
	}

	return nil
}

// WriteMultipleRegisters writes multiple registers using function code 16
func (c *RTUClient) WriteMultipleRegisters(ctx context.Context, address uint16, values []uint16) error {
	if len(values) == 0 {
		return fmt.Errorf("values array cannot be empty")
	}
	if len(values) > 123 {
		return fmt.Errorf("maximum 123 registers can be written at once")
	}

	// Convert []uint16 to []byte (2 bytes per register, Big Endian)
	bytes := make([]byte, len(values)*2)
	for i, v := range values {
		bytes[i*2] = byte(v >> 8)     // High byte
		bytes[i*2+1] = byte(v & 0xFF) // Low byte
	}

	// Log write request (data bytes)
	LogRequest("RTU", 16, address, uint16(len(values)), bytes)

	_, err := c.client.WriteMultipleRegisters(address, uint16(len(values)), bytes)
	if err != nil {
		return fmt.Errorf("failed to write multiple registers: %w", err)
	}

	return nil
}

func (c *RTUClient) Close() error {
	if c.handler != nil {
		return c.handler.Close()
	}
	return nil
}
