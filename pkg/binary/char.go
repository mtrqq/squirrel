package binary

import (
	"fmt"
	"math"
)

func ParseCharArray(data *[]byte, buffer []byte) (int, error) {
	var arrayLength int32
	lengthBytes, err := ParseInt32(&arrayLength, buffer)
	if err != nil {
		return 0, fmt.Errorf("unable to decode char array: failed to parse length: %w", err)
	}

	*data = make([]byte, arrayLength)
	arrayBytes := copy(*data, buffer[lengthBytes:])
	return lengthBytes + arrayBytes, nil
}

func EncodeCharArray(data []byte) ([]byte, error) {
	buffer := make([]byte, Int32ByteSize+len(data))
	lengthBytes, err := PutInt32(buffer, int32(len(data)))
	if err != nil {
		return nil, fmt.Errorf("unable to encode char array: failed to write length: %w", err)
	}

	copy(buffer[lengthBytes:], data)
	return buffer, nil
}

func PutCharArray(buffer []byte, data []byte) (int, error) {
	if len(buffer) < len(data) {
		return 0, fmt.Errorf("insufficient buffer size to put data, got %d, want %d", len(buffer), len(data))
	}

	if len(buffer) > math.MaxInt32 {
		return 0, fmt.Errorf("unable to serialize char array with length exceeding %d bytes", math.MaxInt32)
	}

	lengthBytes, err := PutInt32(buffer, int32(len(buffer)))
	if err != nil {
		return 0, fmt.Errorf("unable to put char array: failed to write length: %w", err)
	}

	arrayBytes := copy(buffer[lengthBytes:], data)
	return lengthBytes + arrayBytes, nil
}

func GetCharArraySize(data []byte) int64 {
	return int64(Int32ByteSize) + int64(len(data))
}
