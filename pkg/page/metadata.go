package page

import (
	"fmt"

	"github.com/mtrqq/squirrel/pkg/item"
	"github.com/mtrqq/squirrel/pkg/raw"
	"github.com/mtrqq/squirrel/pkg/utils"
)

const (
	maxTableNameLength  = 64
	maxColumnNameLength = 64
)

var (
	ErrTableNotFound = fmt.Errorf("table not found")
)

type ColumnDescriptor struct {
	Type item.ItemType
	Name string
}

func (c *ColumnDescriptor) ParseBinary(data []byte) (int, error) {
	readTotal := 0

	read, err := c.Type.ParseBinary(data)
	if err != nil {
		return 0, err
	}
	readTotal += read

	nameSize, err := raw.GetVarCharSize(data[readTotal:])
	if err != nil {
		return 0, fmt.Errorf("unable to parse column name: %w", err)
	}
	if nameSize > maxColumnNameLength {
		return 0, fmt.Errorf("unable to parse column name: name size %d exceeds maximum %d", nameSize, maxColumnNameLength)
	}
	if nameSize+int32(readTotal)+int32(raw.VarCharHeaderSize) > int32(len(data)) {
		return 0, fmt.Errorf("unable to parse column name: insufficient data, got %d, want %d", len(data)-readTotal, nameSize)
	}

	nameBuffer := make([]byte, nameSize)
	read, err = raw.ParseVarChar(data[readTotal:], nameBuffer)
	if err != nil {
		return 0, fmt.Errorf("unable to parse column name: %w", err)
	}
	readTotal += read
	c.Name = utils.StringTakeOverByteArray(nameBuffer)

	return readTotal, nil
}

func (c *ColumnDescriptor) PutBinary(data []byte) (int, error) {
	writtenTotal := 0

	written, err := c.Type.PutBinary(data)
	writtenTotal += written
	if err != nil {
		return 0, err
	}

	if len(c.Name) > maxColumnNameLength {
		return writtenTotal, fmt.Errorf("unable to put column name: name size %d exceeds maximum %d", len(c.Name), maxColumnNameLength)
	}

	written, err = raw.PutVarChar(data[writtenTotal:], utils.ByteArrayFromString(c.Name))
	writtenTotal += written
	if err != nil {
		return 0, fmt.Errorf("unable to put column name: %w", err)
	}

	return writtenTotal, nil
}

func (c *ColumnDescriptor) ByteSize() int {
	return raw.Int8ByteSize + raw.Int32ByteSize + len(c.Name)
}

type TableDescriptor struct {
	Name      string
	Columns   []ColumnDescriptor
	DataPages []uint32
}

func (t *TableDescriptor) ByteSize() int {
	size := raw.Int16ByteSize
	for i := range t.Columns {
		size += t.Columns[i].ByteSize()
	}
	size += raw.Int16ByteSize + raw.Int32ByteSize*len(t.DataPages)
	size += raw.Int32ByteSize + len(t.Name)
	return size
}

func (t TableDescriptor) PutBinary(data []byte) (int, error) {
	if len(data) < t.ByteSize() {
		return 0, fmt.Errorf("insufficient buffer size to put table descriptor, got %d, want %d", len(data), t.ByteSize())
	}

	writtenTotal := 0

	written, err := raw.PutUint16(data, uint16(len(t.Columns)))
	writtenTotal += written
	if err != nil {
		return 0, err
	}

	for i := range t.Columns {
		written, err := t.Columns[i].PutBinary(data[writtenTotal:])
		writtenTotal += written
		if err != nil {
			return writtenTotal, err
		}
	}

	written, err = raw.PutUint16(data[writtenTotal:], uint16(len(t.DataPages)))
	writtenTotal += written
	if err != nil {
		return writtenTotal, err
	}

	for i := range t.DataPages {
		written, err := raw.PutUint32(data[writtenTotal:], t.DataPages[i])
		writtenTotal += written
		if err != nil {
			return writtenTotal, err
		}
	}

	if len(t.Name) > maxTableNameLength {
		return writtenTotal, fmt.Errorf("unable to put table name: name size %d exceeds maximum %d", len(t.Name), maxTableNameLength)
	}

	written, err = raw.PutVarChar(data[writtenTotal:], utils.ByteArrayFromString(t.Name))
	writtenTotal += written
	if err != nil {
		return writtenTotal, fmt.Errorf("unable to put table name: %w", err)
	}

	return writtenTotal, nil
}

func (t *TableDescriptor) ParseBinary(data []byte) (int, error) {
	readTotal := 0

	var columnCount uint16
	read, err := raw.ParseUint16(&columnCount, data)
	if err != nil {
		return 0, err
	}
	readTotal += read

	if columnCount > 0 {
		t.Columns = make([]ColumnDescriptor, columnCount)
		for i := uint16(0); i < columnCount; i++ {
			read, err := t.Columns[i].ParseBinary(data[readTotal:])
			if err != nil {
				return 0, err
			}
			readTotal += read
		}
	}

	var dataPageCount uint16
	read, err = raw.ParseUint16(&dataPageCount, data[readTotal:])
	if err != nil {
		return 0, err
	}
	readTotal += read

	if dataPageCount > 0 {
		t.DataPages = make([]uint32, dataPageCount)
		for i := uint16(0); i < dataPageCount; i++ {
			read, err := raw.ParseUint32(&t.DataPages[i], data[readTotal:])
			if err != nil {
				return 0, err
			}
			readTotal += read
		}
	}

	nameSize, err := raw.GetVarCharSize(data[readTotal:])
	if err != nil {
		return 0, fmt.Errorf("unable to parse table name: %w", err)
	}
	if nameSize > maxTableNameLength {
		return 0, fmt.Errorf("unable to parse table name: name size %d exceeds maximum %d", nameSize, maxTableNameLength)
	}
	if nameSize+int32(readTotal)+int32(raw.VarCharHeaderSize) > int32(len(data)) {
		return 0, fmt.Errorf("unable to parse table name: insufficient data, got %d, want %d", len(data)-readTotal, nameSize)
	}

	nameBuffer := make([]byte, nameSize)
	read, err = raw.ParseVarChar(data[readTotal:], nameBuffer)
	if err != nil {
		return 0, fmt.Errorf("unable to parse table name: %w", err)
	}
	readTotal += read
	t.Name = utils.StringTakeOverByteArray(nameBuffer)

	return readTotal, nil
}

func (t *TableDescriptor) AddDataPage(pageID uint32) {
	t.DataPages = append(t.DataPages, pageID)
}

func (t *TableDescriptor) RemoveDataPage(pageID uint32) {
	for i, id := range t.DataPages {
		if id == pageID {
			t.DataPages = utils.RemoteItemAt(t.DataPages, i)
			return
		}
	}
}

func (t *TableDescriptor) RowSchema() RowSchema {
	schema := RowSchema{
		Columns: make([]item.ItemType, len(t.Columns)),
	}

	for i := range t.Columns {
		schema.Columns[i] = t.Columns[i].Type
	}

	return schema
}

type metadata struct {
	pagesCount uint32
	tables     []TableDescriptor
}

func (m *metadata) ByteSize() int {
	size := raw.Int32ByteSize + raw.Int16ByteSize
	for i := range m.tables {
		size += m.tables[i].ByteSize()
	}
	return size
}

func (m *metadata) PutBinary(data []byte) (int, error) {
	if len(data) < m.ByteSize() {
		return 0, fmt.Errorf("insufficient buffer size to put metadata, got %d, want %d", len(data), m.ByteSize())
	}

	writtenTotal := 0
	written, err := raw.PutUint32(data, m.pagesCount)
	if err != nil {
		return 0, err
	}
	writtenTotal += written

	written, err = raw.PutUint16(data[writtenTotal:], uint16(len(m.tables)))
	if err != nil {
		return writtenTotal, err
	}
	writtenTotal += written

	for i := range m.tables {
		written, err := m.tables[i].PutBinary(data[writtenTotal:])
		if err != nil {
			return writtenTotal, err
		}
		writtenTotal += written
	}

	return writtenTotal, nil
}

func (m *metadata) ParseBinary(data []byte) (int, error) {
	readTotal := 0

	read, err := raw.ParseUint32(&m.pagesCount, data)
	if err != nil {
		return 0, err
	}
	readTotal += read

	var tableCount uint16
	read, err = raw.ParseUint16(&tableCount, data[readTotal:])
	if err != nil {
		return 0, err
	}
	readTotal += read

	if tableCount > 0 {
		m.tables = make([]TableDescriptor, tableCount)
		for i := uint16(0); i < tableCount; i++ {
			read, err := m.tables[i].ParseBinary(data[readTotal:])
			if err != nil {
				return 0, err
			}
			readTotal += read
		}
	}

	return readTotal, nil
}

type MetadataPage struct {
	bp       *BufferPage
	metadata metadata
}

func NewMetadataPage(bp *BufferPage) (MetadataPage, error) {
	if bp.PageType() != PageTypeMetadata {
		return MetadataPage{}, fmt.Errorf("unable to create metadata page#%d: invalid page type %v", bp.Id(), bp.PageType())
	}

	page := MetadataPage{bp: bp}
	_, err := page.metadata.ParseBinary(bp.Data())
	if err != nil {
		return MetadataPage{}, fmt.Errorf("unable to create metadata page#%d: failed to parse metadata: %w", bp.Id(), err)
	}

	return page, nil
}

func (mp *MetadataPage) sync() error {
	_, err := mp.metadata.PutBinary(mp.bp.Data())
	if err != nil {
		return fmt.Errorf("unable to sync metadata page#%d: %w", mp.bp.Id(), err)
	}

	mp.bp.markDirty()
	return nil
}

func (mp *MetadataPage) TableByName(name string) (TableDescriptor, error) {
	table, _, exists := mp.findTableByName(name)
	if !exists {
		return TableDescriptor{}, fmt.Errorf("%w: %s", ErrTableNotFound, name)
	}
	return table, nil
}

func (mp *MetadataPage) findTableByName(name string) (TableDescriptor, int, bool) {
	for i := range mp.metadata.tables {
		if mp.metadata.tables[i].Name == name {
			return mp.metadata.tables[i], i, true
		}
	}
	return TableDescriptor{}, -1, false
}

func (mp *MetadataPage) AddTable(table TableDescriptor) error {
	if _, _, exists := mp.findTableByName(table.Name); exists {
		return fmt.Errorf("unable to add table %s: table already exists", table.Name)
	}

	mp.metadata.tables = append(mp.metadata.tables, table)
	if err := mp.sync(); err != nil {
		return fmt.Errorf("unable to add table %s: %w", table.Name, err)
	}

	return nil
}

// UpdateTable updates an existing table descriptor in the metadata page
// It's extremely dumb and just replaces the old descriptor with the new one
// No data migration or validation is performed
func (mp *MetadataPage) UpdateTable(table TableDescriptor) error {
	_, index, exists := mp.findTableByName(table.Name)
	if !exists {
		return fmt.Errorf("unable to update table %s: table does not exist", table.Name)
	}

	mp.metadata.tables[index] = table
	if err := mp.sync(); err != nil {
		return fmt.Errorf("unable to update table %s: %w", table.Name, err)
	}

	return nil
}

func (mp *MetadataPage) RemoveTableByName(name string) error {
	_, index, exists := mp.findTableByName(name)
	if !exists {
		return fmt.Errorf("unable to remove table %s: table does not exist", name)
	}

	mp.metadata.tables = utils.RemoteItemAt(mp.metadata.tables, index)
	if err := mp.sync(); err != nil {
		return fmt.Errorf("unable to remove table %s: %w", name, err)
	}

	return nil
}

func (mp *MetadataPage) TableCount() int {
	return len(mp.metadata.tables)
}

func (mp *MetadataPage) Tables() []TableDescriptor {
	return mp.metadata.tables
}

func (mp *MetadataPage) PagesCount() uint32 {
	return mp.metadata.pagesCount
}

func (mp *MetadataPage) SetPagesCount(count uint32) error {
	mp.metadata.pagesCount = count
	if err := mp.sync(); err != nil {
		return fmt.Errorf("unable to set pages count to %d: %w", count, err)
	}
	return nil
}
