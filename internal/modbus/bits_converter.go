package modbus

// ConvertBitsToArray unpacks packed bits from Modbus FC1/FC2 response
// Each byte contains 8 bits, with LSB first
// Returns array of 0/1 values for each bit position
func ConvertBitsToArray(data []byte, quantity int) []int {
	result := make([]int, 0, quantity)
	bitIndex := 0

	for _, b := range data {
		// Extract 8 bits from this byte (LSB first)
		for i := 0; i < 8 && bitIndex < quantity; i++ {
			if (b & (1 << i)) != 0 {
				result = append(result, 1)
			} else {
				result = append(result, 0)
			}
			bitIndex++
		}
	}

	return result
}

