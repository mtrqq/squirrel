package field

import (
	"fmt"

	"github.com/mtrqq/squirrel/pkg/binary"
)

const CharTypeName string = "CharType"

type CharsValue struct {
	data []byte
}

func NewChars(val string) CharsValue {
	return CharsValue{data: []byte(val)}
}

func NewCharsFromBytes(val []byte) CharsValue {
	return CharsValue{data: val}
}

func (v CharsValue) String() string {
	return fmt.Sprintf("CharsValue[data=%q]", v.data)
}

func (v *CharsValue) ParseBinary(buffer []byte) (int, error) {
	return binary.ParseCharArray(&v.data, buffer)
}

func (v CharsValue) EncodeBinary() ([]byte, error) {
	return binary.EncodeCharArray(v.data)
}

func (v CharsValue) PutBinary(buffer []byte) (int, error) {
	return binary.PutCharArray(buffer, v.data)
}

func (v CharsValue) ByteSizeBinary() int64 {
	return binary.GetCharArraySize(v.data)
}

type CharsType struct{}

func (t CharsType) Validate(value FieldValue) error {
	_, ok := value.(*CharsValue)
	if !ok {
		return fmt.Errorf("%w: want %T, got %T", ErrInvalidValueType, &CharsValue{}, value)
	}

	return nil
}

func (t CharsType) Params() []CharsType {
	return nil
}

func (t CharsType) String() string {
	return "CharType[]"
}

func (v *CharsValue) TypeName() string {
	return CharTypeName
}
