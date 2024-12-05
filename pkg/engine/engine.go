package engine

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/mtrqq/squirrel/pkg/field"
)

type Table struct {
	Name   string
	Fields []field.FieldType
}

// assume that tables are just inmemory construct
type BinaryEngine struct {
	pg    *pager
	table Table
}

func InitBinaryEngine(path string, table Table) (BinaryEngine, error) {
	pg, err := newPager(path)
	if err != nil {
		return BinaryEngine{}, err
	}

	return BinaryEngine{
		pg:    pg,
		table: table,
	}, nil
}

func (eng *BinaryEngine) Insert(values []field.FieldValue) (uuid.UUID, error) {
	if len(values) != len(eng.table.Fields) {
		return uuid.UUID{}, fmt.Errorf("Invalid number of fields supplied, got %d, want %d", len(values), len(eng.table.Fields))
	}

	for fieldIndex, field := range eng.table.Fields {
		value := values[fieldIndex]
		if err := field.Validate(value); err != nil {
			return uuid.UUID{}, fmt.Errorf("validation failed for %s/#%d field: %v", eng.table.Name, fieldIndex, err)
		}
	}
	panic("TODO")
}
