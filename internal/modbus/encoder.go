package modbus

import (
	"encoding/binary"
	"fmt"
	"math"
)

// EncodeValueToRegisters encodes a scalar value to Modbus registers based on data type and byte order
// Returns array of uint16 values ready for WriteMultipleRegisters
// If quantity > required registers for data type, fills remaining registers with zeros
func EncodeValueToRegisters(value float64, dataType string, byteOrder string, quantity int) ([]uint16, error) {
	var registers []uint16
	var err error

	switch dataType {
	case "uint16":
		if quantity < 1 {
			return nil, fmt.Errorf("uint16 requires at least 1 register")
		}
		if value < 0 || value > 65535 {
			return nil, fmt.Errorf("uint16 value out of range: %f", value)
		}
		registers = []uint16{uint16(value)}

	case "int32":
		if quantity < 2 {
			return nil, fmt.Errorf("int32 requires at least 2 registers")
		}
		if value < math.MinInt32 || value > math.MaxInt32 {
			return nil, fmt.Errorf("int32 value out of range: %f", value)
		}
		registers = encodeInt32ToRegisters(int32(value), byteOrder)

	case "float32", "float":
		if quantity < 2 {
			return nil, fmt.Errorf("float32 requires at least 2 registers")
		}
		registers = encodeFloat32ToRegisters(float32(value), byteOrder)

	case "float64":
		if quantity < 4 {
			return nil, fmt.Errorf("float64 requires at least 4 registers")
		}
		registers = encodeFloat64ToRegisters(value, byteOrder)

	case "uint32":
		if quantity < 2 {
			return nil, fmt.Errorf("uint32 requires at least 2 registers")
		}
		if value < 0 || value > math.MaxUint32 {
			return nil, fmt.Errorf("uint32 value out of range: %f", value)
		}
		registers = encodeUint32ToRegisters(uint32(value), byteOrder)

	default:
		return nil, fmt.Errorf("unsupported data type: %s", dataType)
	}

	// If quantity > required registers, pad with zeros
	if len(registers) < quantity {
		padding := make([]uint16, quantity-len(registers))
		registers = append(registers, padding...)
	}

	// If quantity < required registers, truncate (should not happen due to validation above)
	if len(registers) > quantity {
		registers = registers[:quantity]
	}

	return registers, err
}

// encodeUint32ToRegisters encodes uint32 to 2 registers based on byte order
func encodeUint32ToRegisters(value uint32, byteOrder string) []uint16 {
	bytes := make([]byte, 4)
	binary.BigEndian.PutUint32(bytes, value)

	switch byteOrder {
	case "ABCD": // Big Endian
		return []uint16{
			binary.BigEndian.Uint16(bytes[0:2]),
			binary.BigEndian.Uint16(bytes[2:4]),
		}
	case "DCBA": // Little Endian
		return []uint16{
			binary.LittleEndian.Uint16(bytes[0:2]),
			binary.LittleEndian.Uint16(bytes[2:4]),
		}
	case "BADC": // Byte swap
		return []uint16{
			binary.BigEndian.Uint16([]byte{bytes[1], bytes[0]}),
			binary.BigEndian.Uint16([]byte{bytes[3], bytes[2]}),
		}
	case "CDAB": // Word swap
		return []uint16{
			binary.BigEndian.Uint16(bytes[2:4]),
			binary.BigEndian.Uint16(bytes[0:2]),
		}
	default:
		// Default to ABCD
		return []uint16{
			binary.BigEndian.Uint16(bytes[0:2]),
			binary.BigEndian.Uint16(bytes[2:4]),
		}
	}
}

// encodeInt32ToRegisters encodes int32 to 2 registers based on byte order
func encodeInt32ToRegisters(value int32, byteOrder string) []uint16 {
	return encodeUint32ToRegisters(uint32(value), byteOrder)
}

// encodeFloat32ToRegisters encodes float32 to 2 registers based on byte order
func encodeFloat32ToRegisters(value float32, byteOrder string) []uint16 {
	bits := math.Float32bits(value)
	bytes := make([]byte, 4)
	binary.BigEndian.PutUint32(bytes, bits)

	switch byteOrder {
	case "ABCD": // Big Endian
		return []uint16{
			binary.BigEndian.Uint16(bytes[0:2]),
			binary.BigEndian.Uint16(bytes[2:4]),
		}
	case "DCBA": // Little Endian
		return []uint16{
			binary.LittleEndian.Uint16(bytes[0:2]),
			binary.LittleEndian.Uint16(bytes[2:4]),
		}
	case "BADC": // Byte swap
		return []uint16{
			binary.BigEndian.Uint16([]byte{bytes[1], bytes[0]}),
			binary.BigEndian.Uint16([]byte{bytes[3], bytes[2]}),
		}
	case "CDAB": // Word swap
		return []uint16{
			binary.BigEndian.Uint16(bytes[2:4]),
			binary.BigEndian.Uint16(bytes[0:2]),
		}
	default:
		// Default to ABCD
		return []uint16{
			binary.BigEndian.Uint16(bytes[0:2]),
			binary.BigEndian.Uint16(bytes[2:4]),
		}
	}
}

// encodeFloat64ToRegisters encodes float64 to 4 registers based on byte order
func encodeFloat64ToRegisters(value float64, byteOrder string) []uint16 {
	bits := math.Float64bits(value)
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, bits)

	switch byteOrder {
	case "ABCD": // Big Endian
		return []uint16{
			binary.BigEndian.Uint16(bytes[0:2]),
			binary.BigEndian.Uint16(bytes[2:4]),
			binary.BigEndian.Uint16(bytes[4:6]),
			binary.BigEndian.Uint16(bytes[6:8]),
		}
	case "DCBA": // Little Endian
		return []uint16{
			binary.LittleEndian.Uint16(bytes[0:2]),
			binary.LittleEndian.Uint16(bytes[2:4]),
			binary.LittleEndian.Uint16(bytes[4:6]),
			binary.LittleEndian.Uint16(bytes[6:8]),
		}
	case "BADC": // Byte swap
		return []uint16{
			binary.BigEndian.Uint16([]byte{bytes[1], bytes[0]}),
			binary.BigEndian.Uint16([]byte{bytes[3], bytes[2]}),
			binary.BigEndian.Uint16([]byte{bytes[5], bytes[4]}),
			binary.BigEndian.Uint16([]byte{bytes[7], bytes[6]}),
		}
	case "CDAB": // Word swap
		return []uint16{
			binary.BigEndian.Uint16(bytes[4:6]),
			binary.BigEndian.Uint16(bytes[6:8]),
			binary.BigEndian.Uint16(bytes[0:2]),
			binary.BigEndian.Uint16(bytes[2:4]),
		}
	default:
		// Default to ABCD
		return []uint16{
			binary.BigEndian.Uint16(bytes[0:2]),
			binary.BigEndian.Uint16(bytes[2:4]),
			binary.BigEndian.Uint16(bytes[4:6]),
			binary.BigEndian.Uint16(bytes[6:8]),
		}
	}
}

// EncodeRawRegisters validates and converts raw register values to uint16 array
func EncodeRawRegisters(registerValues []int, quantity int) ([]uint16, error) {
	if len(registerValues) != quantity {
		return nil, fmt.Errorf("register_values length (%d) must match quantity (%d)", len(registerValues), quantity)
	}

	result := make([]uint16, quantity)
	for i, v := range registerValues {
		if v < 0 || v > 65535 {
			return nil, fmt.Errorf("register value %d at index %d is out of range (0-65535)", v, i)
		}
		result[i] = uint16(v)
	}

	return result, nil
}
