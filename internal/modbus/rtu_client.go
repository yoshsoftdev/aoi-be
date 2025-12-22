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
// Format: Same as modpoll - writes multiple values in sequence
// Example: modpoll -m rtu -b 9600 -p none -a 4 -r 0 -c 8 COM5 1 1 1 1 1 1 1 1
// This will write 8 registers starting at address 0, each with value 1
func (c *RTUClient) WriteMultipleRegisters(ctx context.Context, address uint16, values []uint16) error {
	log := logger.GetLogger()

	if len(values) == 0 {
		return fmt.Errorf("values array cannot be empty")
	}
	if len(values) > 123 {
		return fmt.Errorf("maximum 123 registers can be written at once")
	}

	log.Debugf("RTUClient.WriteMultipleRegisters: address=%d, quantity=%d, values=%v", address, len(values), values)

	// Convert []uint16 to []byte (2 bytes per register, Big Endian)
	// Format: [high_byte_0, low_byte_0, high_byte_1, low_byte_1, ...]
	// This matches modpoll's format for Write Multiple Registers (FC16)
	bytes := make([]byte, len(values)*2)
	for i, v := range values {
		bytes[i*2] = byte(v >> 8)     // High byte (MSB)
		bytes[i*2+1] = byte(v & 0xFF) // Low byte (LSB)
		log.Debugf("RTUClient.WriteMultipleRegisters: Register[%d] = %d (0x%04X) -> bytes[%d:%d] = [0x%02X, 0x%02X]",
			i, v, v, i*2, i*2+2, bytes[i*2], bytes[i*2+1])
	}

	// Log write request (data bytes) - shows hex format like modpoll
	LogRequest("RTU", 16, address, uint16(len(values)), bytes)
	log.Infof("RTUClient.WriteMultipleRegisters: Writing %d registers starting at address %d", len(values), address)

	// Call library WriteMultipleRegisters
	// Signature: WriteMultipleRegisters(address, quantity uint16, value []byte)
	_, err := c.client.WriteMultipleRegisters(address, uint16(len(values)), bytes)
	if err != nil {
		log.Errorf("RTUClient.WriteMultipleRegisters: Failed to write: %v", err)
		return fmt.Errorf("failed to write multiple registers: %w", err)
	}

	log.Infof("RTUClient.WriteMultipleRegisters: Successfully wrote %d registers", len(values))
	return nil
}

// WriteMultipleCoils writes multiple coils using function code 15
// Format: Coils are bit-packed (1 bit per coil)
// Example: 8 coils with values [1,1,1,1,1,1,1,1] = 1 byte: 0xFF
func (c *RTUClient) WriteMultipleCoils(ctx context.Context, address uint16, values []uint16) error {
	log := logger.GetLogger()

	if len(values) == 0 {
		return fmt.Errorf("values array cannot be empty")
	}
	if len(values) > 1968 {
		return fmt.Errorf("maximum 1968 coils can be written at once")
	}

	log.Debugf("RTUClient.WriteMultipleCoils: address=%d, quantity=%d, values=%v", address, len(values), values)

	// Convert []uint16 to []byte (bit-packed, 1 bit per coil)
	// Format: [byte_count, coil_bytes...]
	// Each coil value should be 0 or 1 (OFF or ON)
	byteCount := (len(values) + 7) / 8 // Round up to nearest byte
	coilBytes := make([]byte, byteCount)

	for i, v := range values {
		// Ensure value is 0 or 1
		bitValue := uint8(0)
		if v != 0 {
			bitValue = 1
		}

		// Pack bits into bytes
		byteIndex := i / 8
		bitIndex := i % 8
		if bitValue == 1 {
			coilBytes[byteIndex] |= (1 << bitIndex)
		}
	}

	log.Debugf("RTUClient.WriteMultipleCoils: Packed %d coils into %d bytes: %v", len(values), byteCount, coilBytes)

	// Log write request
	LogRequest("RTU", 15, address, uint16(len(values)), coilBytes)
	log.Infof("RTUClient.WriteMultipleCoils: Writing %d coils starting at address %d", len(values), address)

	// Call library WriteMultipleCoils
	// Signature: WriteMultipleCoils(address, quantity uint16, value []byte)
	_, err := c.client.WriteMultipleCoils(address, uint16(len(values)), coilBytes)
	if err != nil {
		log.Errorf("RTUClient.WriteMultipleCoils: Failed to write: %v", err)
		return fmt.Errorf("failed to write multiple coils: %w", err)
	}

	log.Infof("RTUClient.WriteMultipleCoils: Successfully wrote %d coils", len(values))
	return nil
}

// WriteSingleCoil writes a single coil using function code 5
func (c *RTUClient) WriteSingleCoil(ctx context.Context, address uint16, value uint16) error {
	log := logger.GetLogger()

	// Ensure value is 0 or 1
	coilValue := uint16(0)
	if value != 0 {
		coilValue = 0xFF00 // Modbus convention: 0xFF00 = ON, 0x0000 = OFF
	}

	log.Debugf("RTUClient.WriteSingleCoil: address=%d, value=%d", address, coilValue)

	// Log write request
	LogRequest("RTU", 5, address, 1, []byte{byte(coilValue >> 8), byte(coilValue & 0xFF)})
	log.Infof("RTUClient.WriteSingleCoil: Writing coil at address %d", address)

	// Call library WriteSingleCoil
	_, err := c.client.WriteSingleCoil(address, coilValue)
	if err != nil {
		log.Errorf("RTUClient.WriteSingleCoil: Failed to write: %v", err)
		return fmt.Errorf("failed to write single coil: %w", err)
	}

	log.Infof("RTUClient.WriteSingleCoil: Successfully wrote coil")
	return nil
}

func (c *RTUClient) Close() error {
	if c.handler != nil {
		return c.handler.Close()
	}
	return nil
}
