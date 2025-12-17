package modbus

import (
	"encoding/binary"
	"fmt"
	"math"
)

// ConvertBytesToValue converts Modbus register bytes to value based on byte order
// Returns uint32 for integer values
func ConvertBytesToValue(data []byte, byteOrder string, quantity int) (uint32, error) {
	if len(data) < quantity*2 {
		return 0, fmt.Errorf("insufficient data: got %d bytes, need %d", len(data), quantity*2)
	}

	var result uint32

	switch byteOrder {
	case "ABCD": // Big Endian (most significant byte first)
		if quantity == 1 {
			result = uint32(binary.BigEndian.Uint16(data[0:2]))
		} else if quantity == 2 {
			result = binary.BigEndian.Uint32(data[0:4])
		}
	case "DCBA": // Little Endian (least significant byte first)
		if quantity == 1 {
			result = uint32(binary.LittleEndian.Uint16(data[0:2]))
		} else if quantity == 2 {
			result = binary.LittleEndian.Uint32(data[0:4])
		}
	case "BADC": // Byte swap
		if quantity == 1 {
			result = uint32(binary.BigEndian.Uint16([]byte{data[1], data[0]}))
		} else if quantity == 2 {
			swapped := []byte{data[1], data[0], data[3], data[2]}
			result = binary.BigEndian.Uint32(swapped)
		}
	case "CDAB": // Word swap
		if quantity == 2 {
			result = binary.BigEndian.Uint32([]byte{data[2], data[3], data[0], data[1]})
		} else {
			return 0, fmt.Errorf("CDAB byte order requires quantity=2")
		}
	default:
		return 0, fmt.Errorf("unsupported byte order: %s", byteOrder)
	}

	return result, nil
}

// ConvertBytesToFloat32 converts Modbus register bytes to float32 based on byte order
// Supports float32 values (requires quantity=2, 4 bytes)
// Note: float32 is 32 bits = 4 bytes = 2 Modbus registers (each register is 16 bits)
func ConvertBytesToFloat32(data []byte, byteOrder string, quantity int) (float32, error) {
	if quantity < 2 {
		return 0, fmt.Errorf("float32 requires at least 2 registers (quantity >= 2), got quantity=%d. Please set quantity to 2 or more when using float32 data type", quantity)
	}
	if len(data) < 4 {
		return 0, fmt.Errorf("insufficient data: got %d bytes, need 4 bytes (2 registers) for float32", len(data))
	}

	var bits uint32

	switch byteOrder {
	case "ABCD": // Big Endian
		bits = binary.BigEndian.Uint32(data[0:4])
	case "DCBA": // Little Endian
		bits = binary.LittleEndian.Uint32(data[0:4])
	case "BADC": // Byte swap
		swapped := []byte{data[1], data[0], data[3], data[2]}
		bits = binary.BigEndian.Uint32(swapped)
	case "CDAB": // Word swap
		bits = binary.BigEndian.Uint32([]byte{data[2], data[3], data[0], data[1]})
	default:
		return 0, fmt.Errorf("unsupported byte order: %s", byteOrder)
	}

	return math.Float32frombits(bits), nil
}

// ConvertBytesToFloat64 converts Modbus register bytes to float64 based on byte order
// Supports float64 values (requires quantity=4, 8 bytes)
// Note: float64 is 64 bits = 8 bytes = 4 Modbus registers (each register is 16 bits)
func ConvertBytesToFloat64(data []byte, byteOrder string, quantity int) (float64, error) {
	if quantity < 4 {
		return 0, fmt.Errorf("float64 requires at least 4 registers (quantity >= 4), got quantity=%d. Please set quantity to 4 or more when using float64 data type", quantity)
	}
	if len(data) < 8 {
		return 0, fmt.Errorf("insufficient data: got %d bytes, need 8 bytes (4 registers) for float64", len(data))
	}

	var bits uint64

	switch byteOrder {
	case "ABCD": // Big Endian
		bits = binary.BigEndian.Uint64(data[0:8])
	case "DCBA": // Little Endian
		bits = binary.LittleEndian.Uint64(data[0:8])
	case "BADC": // Byte swap (for float64, swap bytes within each 32-bit word)
		// Swap bytes: [1,0,3,2,5,4,7,6]
		swapped := []byte{data[1], data[0], data[3], data[2], data[5], data[4], data[7], data[6]}
		bits = binary.BigEndian.Uint64(swapped)
	case "CDAB": // Word swap (for float64, swap 32-bit words)
		// Swap words: [4,5,6,7,0,1,2,3]
		bits = binary.BigEndian.Uint64([]byte{data[4], data[5], data[6], data[7], data[0], data[1], data[2], data[3]})
	default:
		return 0, fmt.Errorf("unsupported byte order: %s", byteOrder)
	}

	return math.Float64frombits(bits), nil
}

// ConvertBytesToSignedInt32 converts Modbus register bytes to signed int32 based on byte order
// Supports negative integer values (requires quantity=2, 4 bytes)
// Note: int32 is 32 bits = 4 bytes = 2 Modbus registers (each register is 16 bits)
func ConvertBytesToSignedInt32(data []byte, byteOrder string, quantity int) (int32, error) {
	if quantity < 2 {
		return 0, fmt.Errorf("int32 requires at least 2 registers (quantity >= 2), got quantity=%d. Please set quantity to 2 or more when using int32 data type", quantity)
	}
	if len(data) < 4 {
		return 0, fmt.Errorf("insufficient data: got %d bytes, need 4 bytes (2 registers) for int32", len(data))
	}

	var result int32

	switch byteOrder {
	case "ABCD": // Big Endian
		result = int32(binary.BigEndian.Uint32(data[0:4]))
	case "DCBA": // Little Endian
		result = int32(binary.LittleEndian.Uint32(data[0:4]))
	case "BADC": // Byte swap
		swapped := []byte{data[1], data[0], data[3], data[2]}
		result = int32(binary.BigEndian.Uint32(swapped))
	case "CDAB": // Word swap
		result = int32(binary.BigEndian.Uint32([]byte{data[2], data[3], data[0], data[1]}))
	default:
		return 0, fmt.Errorf("unsupported byte order: %s", byteOrder)
	}

	return result, nil
}

// ConvertValueToBytes converts value to Modbus register bytes based on byte order
func ConvertValueToBytes(value uint32, byteOrder string, quantity int) ([]byte, error) {
	var result []byte

	switch byteOrder {
	case "ABCD": // Big Endian
		if quantity == 1 {
			result = make([]byte, 2)
			binary.BigEndian.PutUint16(result, uint16(value))
		} else if quantity == 2 {
			result = make([]byte, 4)
			binary.BigEndian.PutUint32(result, value)
		}
	case "DCBA": // Little Endian
		if quantity == 1 {
			result = make([]byte, 2)
			binary.LittleEndian.PutUint16(result, uint16(value))
		} else if quantity == 2 {
			result = make([]byte, 4)
			binary.LittleEndian.PutUint32(result, value)
		}
	case "BADC": // Byte swap
		if quantity == 1 {
			temp := make([]byte, 2)
			binary.BigEndian.PutUint16(temp, uint16(value))
			result = []byte{temp[1], temp[0]}
		} else if quantity == 2 {
			temp := make([]byte, 4)
			binary.BigEndian.PutUint32(temp, value)
			result = []byte{temp[1], temp[0], temp[3], temp[2]}
		}
	case "CDAB": // Word swap
		if quantity == 2 {
			temp := make([]byte, 4)
			binary.BigEndian.PutUint32(temp, value)
			result = []byte{temp[2], temp[3], temp[0], temp[1]}
		} else {
			return nil, fmt.Errorf("CDAB byte order requires quantity=2")
		}
	default:
		return nil, fmt.Errorf("unsupported byte order: %s", byteOrder)
	}

	return result, nil
}

