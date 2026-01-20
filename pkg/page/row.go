package page

import (
	"fmt"
	"math"
	"sync"

	"github.com/mtrqq/squirrel/pkg/allocator"
	"github.com/mtrqq/squirrel/pkg/item"
	"github.com/rs/zerolog/log"
)

type SlotID uint16

type RowSchema struct {
	Columns []item.ItemType
}

type RowPage struct {
	bp        *BufferPage
	lock      sync.RWMutex
	allocator *allocator.SlotAllocator
	schema    RowSchema
}

func NewRowPage(bp *BufferPage, schema RowSchema) (RowPage, error) {
	alloc := allocator.NewSlotAllocator(bp.Data())
	return RowPage{
		bp:        bp,
		allocator: alloc,
		schema:    schema,
	}, nil
}

// InsertRow inserts a new row into the RowPage and returns its SlotID
// we assume that the caller has already checked if the row can fit
// and page doesn't care about the internal item types or validity
func (rp *RowPage) InsertRow(items []item.Item) (SlotID, error) {
	rp.lock.Lock()
	defer rp.lock.Unlock()

	itemsSize := item.ItemsSize(items)
	slot, err := rp.allocator.Allocate(uint32(itemsSize))
	if err != nil {
		return 0, err
	}

	written, err := item.ItemsPutBinary(items, slot.Buffer)
	if err != nil {
		return 0, err
	}

	if written != itemsSize {
		return 0, fmt.Errorf("row size mismatch: expected %d bytes, wrote %d bytes", itemsSize, written)
	}

	return SlotID(slot.Index), nil
}

func (rp *RowPage) DeleteRow(slot SlotID) error {
	rp.lock.Lock()
	defer rp.lock.Unlock()

	return rp.allocator.Deallocate(allocator.Allocation{
		Index: uint16(slot),
	})
}

func (rp *RowPage) UpdateRow(slot SlotID, items []item.Item) error {
	rp.lock.Lock()
	defer rp.lock.Unlock()

	allocation, err := rp.allocator.GetAllocation(uint16(slot))
	if err != nil {
		return fmt.Errorf("unable to update slot %d: %w", slot, err)
	}

	itemsSize := item.ItemsSize(items)
	// if the new row size matches the existing allocation, we can update in place
	if itemsSize == len(allocation.Buffer) {
		written, err := item.ItemsPutBinary(items, allocation.Buffer)
		if err != nil {
			return fmt.Errorf("unable to update slot %d: %w", slot, err)
		}
		if written != itemsSize {
			return fmt.Errorf("row size mismatch during update: expected %d bytes, wrote %d bytes", itemsSize, written)
		}
		return nil
	}

	if err := rp.allocator.Deallocate(allocation); err != nil {
		return fmt.Errorf("unable to update slot %d: %w", slot, err)
	}

	newAllocation, err := rp.allocator.Allocate(uint32(itemsSize))
	if err != nil {
		return fmt.Errorf("unable to update slot %d: %w", slot, err)
	}

	written, err := item.ItemsPutBinary(items, newAllocation.Buffer)
	if err != nil {
		return fmt.Errorf("unable to update slot %d: %w", slot, err)
	}

	if written != itemsSize {
		return fmt.Errorf("row size mismatch during update: expected %d bytes, wrote %d bytes", itemsSize, written)
	}

	return nil
}

func (rp *RowPage) itemsInBuffer(buffer []byte) ([]item.ItemView, error) {
	items := make([]item.ItemView, len(rp.schema.Columns))
	offset := 0
	for i, itemType := range rp.schema.Columns {
		if offset >= len(buffer) {
			return nil, fmt.Errorf("unable to read item at index %d: buffer too small", i)
		}
		itemSize := itemType.ItemByteSize(buffer[offset:])

		if offset+itemSize > len(buffer) {
			return nil, fmt.Errorf("unable to read item at index %d: item size exceeds buffer size", i)
		}
		items[i] = item.NewItemView(buffer[offset:offset+itemSize], itemType)

		offset += itemSize
	}

	return items, nil
}

func (rp *RowPage) FetchRow(slot SlotID) ([]item.ItemView, error) {
	rp.lock.RLock()
	defer rp.lock.RUnlock()

	allocation, err := rp.allocator.GetAllocation(uint16(slot))
	if err != nil {
		return nil, fmt.Errorf("unable to fetch slot %d: %w", slot, err)
	}

	return rp.itemsInBuffer(allocation.Buffer)
}

func (rp *RowPage) IterRows(yield func(SlotID, []item.ItemView) bool) {
	rp.lock.RLock()
	defer rp.lock.RUnlock()

	rp.allocator.VisitAllocations(func(allocation allocator.Allocation) bool {
		items, err := rp.itemsInBuffer(allocation.Buffer)
		if err != nil {
			log.Error().Err(err).Msgf("failed to read row at slot %d", allocation.Index)
			return true
		}

		return yield(SlotID(allocation.Index), items)
	})
}

func (rp *RowPage) CanFit(size uint32) bool {
	rp.lock.RLock()
	defer rp.lock.RUnlock()

	return rp.allocator.CanFit(size)
}

func (rp *RowPage) CanFitItems(items []item.Item) bool {
	size := item.ItemsSize(items)
	if size > math.MaxUint32 {
		log.Error().Msgf("row size %d exceeds maximum uint32 size", size)
		return false
	}
	return rp.CanFit(uint32(size))
}

func (rp *RowPage) FreeBytes() uint32 {
	rp.lock.RLock()
	defer rp.lock.RUnlock()

	return rp.allocator.FreeBytes()
}

func (rp *RowPage) LargestAllocable() uint32 {
	rp.lock.RLock()
	defer rp.lock.RUnlock()

	return rp.allocator.LargestAllocatableSize()
}

func (rp *RowPage) SlotsCount() uint16 {
	rp.lock.RLock()
	defer rp.lock.RUnlock()

	return rp.allocator.SlotsAllocated()
}

func (rp *RowPage) Id() uint32 {
	return rp.bp.Id()
}
