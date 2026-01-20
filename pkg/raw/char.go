package raw

import (
	"fmt"
	"math"
)

const (
	VarCharHeaderSize = Int32ByteSize
)

func ParseVarChar(source []byte, output []byte) (int, error) {
	varCharLength, err := GetVarCharSize(source)
	if err != nil {
		return 0, fmt.Errorf("unable to decode char array: failed to get size: %w", err)
	}

	if len(output) < int(varCharLength) {
		return 0, fmt.Errorf("insufficient buffer size to hold char array, got %d, want %d", len(output), varCharLength)
	}

	// truncate source to only the bytes of the char array
	source = source[Int32ByteSize : Int32ByteSize+int(varCharLength)]
	readBytes := copy(output, source)
	return Int32ByteSize + readBytes, nil
}

func PutVarChar(output []byte, data []byte) (int, error) {
	if len(output) < len(data) {
		return 0, fmt.Errorf("insufficient buffer size to put data, got %d, want %d", len(output), len(data))
	}

	if len(output) > math.MaxInt32 {
		return 0, fmt.Errorf("unable to serialize char array with length exceeding %d bytes", math.MaxInt32)
	}

	writtenTotal := 0
	written, err := PutInt(output, int32(len(data)))
	if err != nil {
		return 0, fmt.Errorf("unable to put char array: failed to write length: %w", err)
	}
	writtenTotal += written

	written = copy(output[written:], data)
	writtenTotal += written

	return writtenTotal, nil
}

// GetVarCharSize returns the size of the VarChar data in bytes,
// assumes that provided buffer is a valid VarChar binary representation.
func GetVarCharSize(source []byte) (int32, error) {
	var arrayLength int32
	_, err := ParseInt(&arrayLength, source)
	if err != nil {
		return 0, fmt.Errorf("unable to decode char array: failed to parse length: %w", err)
	}

	return arrayLength, nil
}

type CharData interface {
	[]byte | string
}

// VarCharSizeFor returns the size in bytes required to store the given data as VarChar.
func VarCharSizeFor[T CharData](source T) int {
	return Int32ByteSize + len(source)
}

func VarCharSizeInBuffer(buffer []byte) (int32, error) {
	length, err := GetVarCharSize(buffer)
	if err != nil {
		return 0, err
	}
	return length + int32(Int32ByteSize), nil
}
