package modbus

import (
	"context"
	"fmt"
	"time"

	"github.com/goburrow/modbus"
	"aoi-backend/internal/domain"
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

	handler := modbus.NewRTUClientHandler(*device.Port)
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

	return result, nil
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

func (c *RTUClient) Close() error {
	if c.handler != nil {
		return c.handler.Close()
	}
	return nil
}

