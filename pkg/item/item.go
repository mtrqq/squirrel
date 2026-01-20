package item

import (
	"fmt"

	"github.com/mtrqq/squirrel/pkg/raw"
	"github.com/mtrqq/squirrel/pkg/utils"
	"github.com/rs/zerolog/log"
)

type ItemType uint8

const (
	ItemTypeInteger ItemType = 1
	ItemTypeString  ItemType = 2
	ItemTypeBytes   ItemType = 3
)

func (it *ItemType) ParseBinary(data []byte) (int, error) {
	return raw.ParseUint8((*uint8)(it), data)
}

func (it ItemType) PutBinary(data []byte) (int, error) {
	return raw.PutUint8(data, uint8(it))
}

func (it ItemType) ItemByteSize(data []byte) int {
	switch it {
	case ItemTypeInteger:
		return raw.Int64ByteSize
	case ItemTypeString, ItemTypeBytes:
		size, err := raw.VarCharSizeInBuffer(data)
		if err != nil {
			log.Error().Err(err).Msgf("unable to determine item byte size for item type %v", it)
			return -1
		}
		return int(size)
	}

	log.Error().Msgf("unable to determine item byte size: unsupported item type %v", it)
	return -1
}

type Item struct {
	stringValue string
	bytesValue  []byte
	itemType    ItemType
	intValue    int64
}

func Bytes(data []byte) Item {
	return Item{
		itemType:   ItemTypeBytes,
		bytesValue: data,
	}
}

func String(data string) Item {
	return Item{
		itemType:    ItemTypeString,
		stringValue: data,
	}
}

func Int64(data int64) Item {
	return Item{
		itemType: ItemTypeInteger,
		intValue: data,
	}
}

func (i *Item) Type() ItemType {
	return i.itemType
}

func (i *Item) IntValue() int64 {
	return i.intValue
}

func (i *Item) BytesValue() []byte {
	return i.bytesValue
}

func (i *Item) StringValue() string {
	return i.stringValue
}

func (i *Item) ByteSize() int {
	switch i.itemType {
	case ItemTypeInteger:
		return raw.Int64ByteSize
	case ItemTypeString:
		return raw.VarCharSizeFor(i.stringValue)
	case ItemTypeBytes:
		return raw.VarCharSizeFor(i.bytesValue)
	default:
		return -1
	}
}

func (i *Item) PutBinary(buffer []byte) (int, error) {
	switch i.itemType {
	case ItemTypeInteger:
		return raw.PutInt64(buffer, i.intValue)
	case ItemTypeString:
		return raw.PutVarChar(buffer, []byte(i.stringValue))
	case ItemTypeBytes:
		return raw.PutVarChar(buffer, i.bytesValue)
	default:
		return 0, fmt.Errorf("unable to serialize item: unsupported item type %v", i.itemType)
	}
}

func ItemsSize(items []Item) int {
	totalSize := 0
	for i := range items {
		totalSize += items[i].ByteSize()
	}
	return totalSize
}

func ItemsPutBinary(items []Item, buffer []byte) (int, error) {
	writtenTotal := 0
	for i := range items {
		written, err := items[i].PutBinary(buffer[writtenTotal:])
		if err != nil {
			return 0, fmt.Errorf("unable to serialize item at index %d: %w", i, err)
		}
		writtenTotal += written
	}
	return writtenTotal, nil
}

type ItemView struct {
	data     []byte
	itemType ItemType
}

func NewItemView(data []byte, it ItemType) ItemView {
	return ItemView{
		data:     data,
		itemType: it,
	}
}

func (iv ItemView) ensureType(t ItemType) error {
	if iv.itemType != t {
		return fmt.Errorf("type mismatch when interpreting item view: want %v, available: %v", iv.itemType, t)
	}
	return nil
}

func (iv ItemView) Type() ItemType {
	return iv.itemType
}

func (iv ItemView) Int64() (int64, error) {
	if err := iv.ensureType(ItemTypeInteger); err != nil {
		return 0, err
	}

	var value int64
	_, err := raw.ParseInt64(&value, iv.data)
	if err != nil {
		return 0, fmt.Errorf("failed to parse int64 from item view data: %w", err)
	}

	return value, nil
}

func (iv ItemView) Int64OrDie() int64 {
	value, err := iv.Int64()
	if err != nil {
		panic(err)
	}
	return value
}

func (iv ItemView) Bytes() ([]byte, error) {
	if err := iv.ensureType(ItemTypeBytes); err != nil {
		return nil, err
	}

	length, err := raw.GetVarCharSize(iv.data)
	if err != nil {
		return nil, fmt.Errorf("failed to get varchar size from item view data: %w", err)
	}

	copybuffer := make([]byte, length)
	_, err = raw.ParseVarChar(iv.data, copybuffer)
	if err != nil {
		return nil, fmt.Errorf("failed to parse bytes from item view data: %w", err)
	}
	return copybuffer, nil
}

func (iv ItemView) BytesOrDie() []byte {
	data, err := iv.Bytes()
	if err != nil {
		panic(err)
	}
	return data
}

func (iv ItemView) String() (string, error) {
	if err := iv.ensureType(ItemTypeString); err != nil {
		return "", err
	}

	length, err := raw.GetVarCharSize(iv.data)
	if err != nil {
		return "", fmt.Errorf("failed to get varchar size from item view data: %w", err)
	}

	strBytes := make([]byte, length)
	_, err = raw.ParseVarChar(iv.data, strBytes)
	if err != nil {
		return "", fmt.Errorf("failed to parse string from item view data: %w", err)
	}

	return utils.StringTakeOverByteArray(strBytes), nil
}

func (iv ItemView) StringOrDie() string {
	str, err := iv.String()
	if err != nil {
		panic(err)
	}
	return str
}
