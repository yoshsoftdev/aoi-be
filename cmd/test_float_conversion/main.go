package main

import (
	"context"
	"fmt"
	"log"

	"aoi-backend/internal/core/config"
	"aoi-backend/internal/core/database"
	"aoi-backend/internal/modbus"
	"aoi-backend/internal/repository"
)

func main() {
	// Load configuration
	_, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Initialize database
	if err := database.Init(); err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}

	// Get modbus_tcp config with id = 2
	tcpRepo := repository.NewModbusTCPRepository()
	config, err := tcpRepo.GetByID(2)
	if err != nil {
		log.Fatalf("Failed to get modbus_tcp config id=2: %v", err)
	}

	fmt.Printf("=== Modbus TCP Config ID 2 ===\n")
	fmt.Printf("ID Parameter: %d\n", config.IDParameter)
	fmt.Printf("Function Code: %d\n", config.FunctionCode)
	fmt.Printf("Register Address: %d\n", config.RegisterAddress)
	fmt.Printf("Quantity: %d\n", config.Quantity)
	fmt.Printf("Byte Order: %s\n", config.ByteOrder)
	fmt.Printf("Scaling: %f\n", config.Scaling)
	fmt.Printf("Slave ID: %d\n", config.SlaveID)
	fmt.Printf("Data Type: %s\n", config.DataType)
	fmt.Printf("\n")

	// Get parameter to get device_id
	paramRepo := repository.NewParameterRepository()
	param, err := paramRepo.GetByID(config.IDParameter)
	if err != nil {
		log.Fatalf("Failed to get parameter: %v", err)
	}

	// Get device
	deviceRepo := repository.NewDeviceRepository()
	device, err := deviceRepo.GetByID(param.DeviceID)
	if err != nil {
		log.Fatalf("Failed to get device: %v", err)
	}

	fmt.Printf("=== Device Info ===\n")
	fmt.Printf("Device ID: %d\n", device.ID)
	fmt.Printf("Device Name: %s\n", device.Nama)
	if device.IPAddress != nil {
		fmt.Printf("IP Address: %s\n", *device.IPAddress)
	}
	if device.TCPPort != nil {
		fmt.Printf("TCP Port: %d\n", *device.TCPPort)
	}
	if device.SlaveID != nil {
		fmt.Printf("Device Slave ID: %d\n", *device.SlaveID)
	}
	fmt.Printf("\n")

	// Create Modbus TCP client
	slaveID := &config.SlaveID
	client, err := modbus.NewTCPClient(device, slaveID)
	if err != nil {
		log.Fatalf("Failed to create Modbus TCP client: %v", err)
	}
	defer client.Close()

	fmt.Printf("=== Reading Registers ===\n")
	fmt.Printf("Reading 2 registers starting from address %d (for float32 test)...\n", config.RegisterAddress)

	ctx := context.Background()
	
	// Read 2 registers for float32 test
	data, err := client.Read(ctx, config.FunctionCode, uint16(config.RegisterAddress), 2)
	if err != nil {
		log.Fatalf("Failed to read registers: %v", err)
	}

	fmt.Printf("Raw bytes received: %v\n", data)
	fmt.Printf("Hex: %02x %02x %02x %02x\n", data[0], data[1], data[2], data[3])
	fmt.Printf("Number of bytes: %d\n", len(data))
	fmt.Printf("\n")

	// Test conversion as uint32 (current method)
	fmt.Printf("=== Current Conversion (uint32, quantity=1) ===\n")
	rawValue, err := modbus.ConvertBytesToValue(data[:2], config.ByteOrder, 1)
	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
	} else {
		scaledValue := float64(rawValue) * config.Scaling
		fmt.Printf("Raw Value (uint32, 1 register): %d\n", rawValue)
		fmt.Printf("Scaled Value: %f\n", scaledValue)
	}
	fmt.Printf("\n")

	// Test conversion as float32 with different byte orders
	fmt.Printf("=== Test Conversion (float32) with Different Byte Orders ===\n")
	byteOrders := []string{"ABCD", "DCBA", "BADC", "CDAB"}
	
	for _, byteOrder := range byteOrders {
		rawFloat32, err := modbus.ConvertBytesToFloat32(data, byteOrder, 2)
		if err != nil {
			fmt.Printf("  %s: ERROR - %v\n", byteOrder, err)
		} else {
			scaledFloat32 := float64(rawFloat32) * config.Scaling
			fmt.Printf("  %s: Raw=%f, Scaled=%f\n", byteOrder, rawFloat32, scaledFloat32)
		}
	}
	fmt.Printf("\n")

	// Test conversion as int32
	fmt.Printf("=== Test Conversion (int32) ===\n")
	for _, byteOrder := range byteOrders {
		rawInt32, err := modbus.ConvertBytesToSignedInt32(data, byteOrder, 2)
		if err != nil {
			fmt.Printf("  %s: ERROR - %v\n", byteOrder, err)
		} else {
			scaledInt32 := float64(rawInt32) * config.Scaling
			fmt.Printf("  %s: Raw=%d, Scaled=%f\n", byteOrder, rawInt32, scaledInt32)
		}
	}
	fmt.Printf("\n")

	fmt.Printf("=== Recommendation ===\n")
	fmt.Printf("Current: quantity=%d, data_type=%s, byte_order=%s\n", config.Quantity, config.DataType, config.ByteOrder)
	fmt.Printf("✅ Data can be converted to float32\n")
	fmt.Printf("   Please check which byte order gives the correct float value\n")
	fmt.Printf("   Then update: quantity=2, data_type='float32', byte_order=<correct_order>\n")
}
