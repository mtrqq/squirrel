package binary

import (
	"encoding/binary"
	"fmt"
	"log"
)

var (
	binaryEncodingOrder = binary.BigEndian
)

const (
	numbersBase = 10

	Int64ByteSize = 8
	Int32ByteSize = 4
)

func ParseInt64(value *int64, buffer []byte) (int, error) {
	return parseInt(value, buffer, Int64ByteSize)
}

func ParseInt32(value *int32, buffer []byte) (int, error) {
	return parseInt(value, buffer, Int32ByteSize)
}

func parseInt[T fixedSizeInt](value *T, buffer []byte, byteSize int) (int, error) {
	if len(buffer) < byteSize {
		return 0, fmt.Errorf("unable to decode number: too small buffer size (at least %d bytes required)", byteSize)
	}

	buffer = buffer[:byteSize]
	_, err := binary.Decode(buffer, binaryEncodingOrder, value)
	if err != nil {
		return 0, fmt.Errorf("unable to decode number (%v): %v", buffer, err)
	}

	return byteSize, nil
}

type fixedSizeInt interface {
	int32 | int64
}

func EncodeInt64(value int64) []byte {
	return encodeInt(value, Int64ByteSize)

}

func EncodeInt32(value int32) []byte {
	return encodeInt(value, Int32ByteSize)
}

func encodeInt[T fixedSizeInt](value T, byteSize int) []byte {
	buffer := make([]byte, byteSize)
	written, err := binary.Encode(buffer, binaryEncodingOrder, value)
	if err != nil {
		log.Fatalf("failed to encode number (%v): %v", value, err)
		// return nil, fmt.Errorf("unable to encode number (%v): %v", value, err)
	}

	if written != byteSize {
		log.Fatalf("written incorrect number of bytes while encoding, got %d, want %d", written, byteSize)
		// return nil, fmt.Errorf("written incorrect number of bytes while encoding, got %d, want %d", written, byteSize)
	}

	return buffer
}

func PutInt64(buffer []byte, value int64) (int, error) {
	if len(buffer) < Int64ByteSize {
		return 0, fmt.Errorf("insufficient buffer size to put data, got %d, want %d", len(buffer), Int64ByteSize)
	}

	written := binary.PutVarint(buffer, value)
	if written != Int64ByteSize {
		return 0, fmt.Errorf("written incorrect number of bytes while encoding, got %d, want %d", written, Int64ByteSize)
	}

	return written, nil
}

func PutInt32(buffer []byte, value int32) (int, error) {
	if len(buffer) < Int32ByteSize {
		return 0, fmt.Errorf("insufficient buffer size to put data, got %d, want %d", len(buffer), Int64ByteSize)
	}

	data := EncodeInt32(value)
	return copy(buffer, data), nil
}

func GetInt32Size() int64 {
	return Int32ByteSize
}

func GetInt64Size() int64 {
	return Int64ByteSize
}
