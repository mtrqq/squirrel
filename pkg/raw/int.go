package raw

import (
	"encoding/binary"
	"fmt"
	"log"
	"unsafe"
)

var (
	binaryEncodingOrder = binary.BigEndian
)

const (
	numbersBase = 10

	Int64ByteSize = int(unsafe.Sizeof(int64(0)))
	Int32ByteSize = int(unsafe.Sizeof(int32(0)))
	Int16ByteSize = int(unsafe.Sizeof(int16(0)))
	Int8ByteSize  = int(unsafe.Sizeof(int8(0)))
)

type fixedSizeInt interface {
	int32 | int64 | int16 | int8 | uint32 | uint64 | uint16 | uint8
}

func ParseInt[T fixedSizeInt](value *T, buffer []byte) (int, error) {
	valueByteSize := int(unsafe.Sizeof(*value))
	if len(buffer) < valueByteSize {
		return 0, fmt.Errorf("unable to decode number: too small buffer size (at least %d bytes required)", valueByteSize)
	}

	buffer = buffer[:valueByteSize]
	_, err := binary.Decode(buffer, binaryEncodingOrder, value)
	if err != nil {
		return 0, fmt.Errorf("unable to decode number (%v): %v", buffer, err)
	}

	return valueByteSize, nil
}

func ParseInt8(value *int8, buffer []byte) (int, error) {
	return ParseInt[int8](value, buffer)
}

func ParseInt16(value *int16, buffer []byte) (int, error) {
	return ParseInt[int16](value, buffer)
}

func ParseInt32(value *int32, buffer []byte) (int, error) {
	return ParseInt[int32](value, buffer)
}

func ParseInt64(value *int64, buffer []byte) (int, error) {
	return ParseInt[int64](value, buffer)
}

func ParseUint8(value *uint8, buffer []byte) (int, error) {
	return ParseInt[uint8](value, buffer)
}

func ParseUint16(value *uint16, buffer []byte) (int, error) {
	return ParseInt[uint16](value, buffer)
}

func ParseUint32(value *uint32, buffer []byte) (int, error) {
	return ParseInt[uint32](value, buffer)
}

func ParseUint64(value *uint64, buffer []byte) (int, error) {
	return ParseInt[uint64](value, buffer)
}

func EncodeInt[T fixedSizeInt](value T) []byte {
	valueByteSize := int(unsafe.Sizeof(value))
	buffer := make([]byte, valueByteSize)
	written, err := PutInt(buffer, value)

	if err != nil {
		log.Fatalf("failed to encode integer value %v: %v", value, err)
	}
	if written != valueByteSize {
		log.Fatalf("written incorrect number of bytes while encoding %T, got %d, want %d", value, written, valueByteSize)
	}

	return buffer
}

func EncodeInt8(value int8) []byte {
	return EncodeInt[int8](value)
}

func EncodeInt16(value int16) []byte {
	return EncodeInt[int16](value)
}

func EncodeInt32(value int32) []byte {
	return EncodeInt[int32](value)
}

func EncodeInt64(value int64) []byte {
	return EncodeInt[int64](value)
}

func EncodeUint8(value uint8) []byte {
	return EncodeInt[uint8](value)
}

func EncodeUint16(value uint16) []byte {
	return EncodeInt[uint16](value)
}

func EncodeUint32(value uint32) []byte {
	return EncodeInt[uint32](value)
}

func EncodeUint64(value uint64) []byte {
	return EncodeInt[uint64](value)
}

func PutInt[T fixedSizeInt](buffer []byte, value T) (int, error) {
	valueByteSize := int(unsafe.Sizeof(value))
	if len(buffer) < valueByteSize {
		return 0, fmt.Errorf("insufficient buffer size to put data for %T, got %d, want %d", value, len(buffer), valueByteSize)
	}

	written, err := binary.Encode(buffer, binaryEncodingOrder, value)
	if err != nil {
		return 0, fmt.Errorf("failed to encode value: %v", err)
	}
	if written != valueByteSize {
		return 0, fmt.Errorf("written incorrect number of bytes while encoding %T, got %d, want %d", value, written, valueByteSize)
	}

	return written, nil
}

func PutInt8(buffer []byte, value int8) (int, error) {
	return PutInt[int8](buffer, value)
}

func PutInt16(buffer []byte, value int16) (int, error) {
	return PutInt[int16](buffer, value)
}

func PutInt32(buffer []byte, value int32) (int, error) {
	return PutInt[int32](buffer, value)
}

func PutInt64(buffer []byte, value int64) (int, error) {
	return PutInt[int64](buffer, value)
}

func PutUint8(buffer []byte, value uint8) (int, error) {
	return PutInt[uint8](buffer, value)
}

func PutUint16(buffer []byte, value uint16) (int, error) {
	return PutInt[uint16](buffer, value)
}

func PutUint32(buffer []byte, value uint32) (int, error) {
	return PutInt[uint32](buffer, value)
}

func PutUint64(buffer []byte, value uint64) (int, error) {
	return PutInt[uint64](buffer, value)
}
