package field

import (
	"fmt"

	"github.com/mtrqq/squirrel/pkg/binary"
)

const (
	IntTypeName = "IntType"
)

type IntValue struct {
	value int64
}

func NewInt(val int) IntValue {
	return IntValue{value: int64(val)}
}

func NewInt64(val int64) IntValue {
	return IntValue{value: val}
}

func (v IntValue) String() string {
	return fmt.Sprintf("IntValue[value=%d]", v.value)
}

func (v *IntValue) ParseBinary(buffer []byte) (int, error) {
	return binary.ParseInt64(&v.value, buffer)
}

func (v IntValue) EncodeBinary() ([]byte, error) {
	return binary.EncodeInt64(v.value), nil
}

func (v IntValue) PutBinary(buffer []byte) (int, error) {
	return binary.PutInt64(buffer, v.value)
}

func (v IntValue) ByteSizeBinary() int64 {
	return binary.GetInt64Size()
}

type IntType struct{}

func (t IntType) Validate(value FieldValue) error {
	_, ok := value.(*IntValue)
	if !ok {
		return fmt.Errorf("%w: got %T, want %T", ErrInvalidValueType, value, &IntValue{})
	}

	return nil
}

func (t IntType) Params() []interface{} {
	return nil
}

func (t IntType) String() string {
	return "IntType[]"
}

func (t IntType) TypeName() string {
	return IntTypeName
}
