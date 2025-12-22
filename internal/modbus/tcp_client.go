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
func (c *TCPClient) WriteMultipleRegisters(ctx context.Context, address uint16, values []uint16) error {
	if len(values) == 0 {
		return fmt.Errorf("values array cannot be empty")
	}
	if len(values) > 123 {
		return fmt.Errorf("maximum 123 registers can be written at once")
	}

	// Convert []uint16 to []byte (2 bytes per register)
	bytes := make([]byte, len(values)*2)
	for i, v := range values {
		bytes[i*2] = byte(v >> 8)     // High byte
		bytes[i*2+1] = byte(v & 0xFF) // Low byte
	}

	// Log write request (data bytes)
	LogRequest("TCP", 16, address, uint16(len(values)), bytes)

	_, err := c.client.WriteMultipleRegisters(address, uint16(len(values)), bytes)
	if err != nil {
		return fmt.Errorf("failed to write multiple registers: %w", err)
	}

	return nil
}

func (c *TCPClient) Close() error {
	if c.handler != nil {
		return c.handler.Close()
	}
	return nil
}
