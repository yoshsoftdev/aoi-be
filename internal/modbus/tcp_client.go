package modbus

import (
	"context"
	"fmt"
	"time"

	"aoi-backend/internal/domain"

	"github.com/goburrow/modbus"
)

type TCPClient struct {
	handler *modbus.TCPClientHandler
	client  modbus.Client
}

func NewTCPClient(device *domain.Device, slaveID *int) (*TCPClient, error) {
	if device.IPAddress == nil || *device.IPAddress == "" {
		return nil, fmt.Errorf("device IP address is required for Modbus TCP")
	}

	// Use parameter-level slave_id if provided, otherwise use device-level
	var finalSlaveID int
	if slaveID != nil && *slaveID > 0 {
		finalSlaveID = *slaveID
	} else if device.SlaveID != nil {
		finalSlaveID = *device.SlaveID
	} else {
		return nil, fmt.Errorf("slave ID is required for Modbus TCP (either from device or parameter)")
	}

	port := 502 // default Modbus TCP port
	if device.TCPPort != nil {
		port = *device.TCPPort
	}

	address := fmt.Sprintf("%s:%d", *device.IPAddress, port)
	handler := modbus.NewTCPClientHandler(address)
	handler.SlaveId = byte(finalSlaveID)

	timeout := 5 * time.Second
	if device.TimeoutMs > 0 {
		timeout = time.Duration(device.TimeoutMs) * time.Millisecond
	}
	handler.Timeout = timeout

	if err := handler.Connect(); err != nil {
		return nil, fmt.Errorf("failed to connect to Modbus TCP: %w", err)
	}

	client := modbus.NewClient(handler)

	return &TCPClient{
		handler: handler,
		client:  client,
	}, nil
}

func (c *TCPClient) Read(ctx context.Context, functionCode int, address uint16, quantity uint16) ([]byte, error) {
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
	LogResponseData("TCP", functionCode, address, quantity, result)

	return result, nil
}

func (c *TCPClient) Write(ctx context.Context, functionCode int, address uint16, value uint16) error {
	var err error

	switch functionCode {
	case 5: // Write Single Coil
		_, err = c.client.WriteSingleCoil(address, value)
	case 6: // Write Single Register
		_, err = c.client.WriteSingleRegister(address, value)
	case 15: // Write Multiple Coils
		return fmt.Errorf("function code 15 not supported for single write")
	case 16: // Write Multiple Registers
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
func (c *TCPClient) WriteMultipleRegisters(ctx context.Context, address uint16, values []uint16) error {
	if len(values) == 0 {
		return fmt.Errorf("values array cannot be empty")
	}
	if len(values) > 123 {
		return fmt.Errorf("maximum 123 registers can be written at once")
	}

	// Convert []uint16 to []byte (2 bytes per register, Big Endian)
	// Format: [high_byte_0, low_byte_0, high_byte_1, low_byte_1, ...]
	// This matches modpoll's format for Write Multiple Registers (FC16)
	bytes := make([]byte, len(values)*2)
	for i, v := range values {
		bytes[i*2] = byte(v >> 8)     // High byte (MSB)
		bytes[i*2+1] = byte(v & 0xFF) // Low byte (LSB)
	}

	// Log write request (data bytes)
	LogRequest("TCP", 16, address, uint16(len(values)), bytes)

	// Call library WriteMultipleRegisters
	// Signature: WriteMultipleRegisters(address, quantity uint16, value []byte)
	_, err := c.client.WriteMultipleRegisters(address, uint16(len(values)), bytes)
	if err != nil {
		return fmt.Errorf("failed to write multiple registers: %w", err)
	}

	return nil
}

// WriteMultipleCoils writes multiple coils using function code 15
// Format: Coils are bit-packed (1 bit per coil)
// Example: 8 coils with values [1,1,1,1,1,1,1,1] = 1 byte: 0xFF
func (c *TCPClient) WriteMultipleCoils(ctx context.Context, address uint16, values []uint16) error {
	if len(values) == 0 {
		return fmt.Errorf("values array cannot be empty")
	}
	if len(values) > 1968 {
		return fmt.Errorf("maximum 1968 coils can be written at once")
	}

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

	// Log write request
	LogRequest("TCP", 15, address, uint16(len(values)), coilBytes)

	// Call library WriteMultipleCoils
	// Signature: WriteMultipleCoils(address, quantity uint16, value []byte)
	_, err := c.client.WriteMultipleCoils(address, uint16(len(values)), coilBytes)
	if err != nil {
		return fmt.Errorf("failed to write multiple coils: %w", err)
	}

	return nil
}

// WriteSingleCoil writes a single coil using function code 5
func (c *TCPClient) WriteSingleCoil(ctx context.Context, address uint16, value uint16) error {
	// Ensure value is 0 or 1
	coilValue := uint16(0)
	if value != 0 {
		coilValue = 0xFF00 // Modbus convention: 0xFF00 = ON, 0x0000 = OFF
	}

	// Log write request
	LogRequest("TCP", 5, address, 1, []byte{byte(coilValue >> 8), byte(coilValue & 0xFF)})

	// Call library WriteSingleCoil
	_, err := c.client.WriteSingleCoil(address, coilValue)
	if err != nil {
		return fmt.Errorf("failed to write single coil: %w", err)
	}

	return nil
}

func (c *TCPClient) Close() error {
	if c.handler != nil {
		return c.handler.Close()
	}
	return nil
}
