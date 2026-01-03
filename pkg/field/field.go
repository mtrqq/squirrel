package field

import (
	"errors"
	"fmt"

	"github.com/mtrqq/squirrel/pkg/binary"
)

var (
	ErrInvalidValueType = errors.New("field value has invalid type")
)

type FieldValue interface {
	fmt.Stringer
	binary.BinarySerializable
}

type FieldType interface {
	fmt.Stringer

	TypeName() string
	Params() []interface{}
	Validate(value FieldValue) error
}
