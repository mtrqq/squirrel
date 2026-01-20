package allocator

import (
	"errors"
	"fmt"
	"math"

	"github.com/mtrqq/squirrel/pkg/raw"
	"github.com/rs/zerolog/log"
)

const (
	allocatorHeaderSize     = raw.Int16ByteSize
	allocatorSlotHeaderSize = raw.Int32ByteSize*2 + raw.Int8ByteSize
	slotsCountOffset        = 0
)

var (
	noFreeSlotsErr = fmt.Errorf("no free slots available for requested size")
)

type Allocation struct {
	Buffer []byte
	Index  uint16
}

// SlotAllocator is an allocator that allocates memory slots from a pre-allocated buffer
// it operates in sandwich mode, meaning that it allocates memory from both ends of the buffer
// towards the center. From left side it allocates fixed-size slots, usually for metadata,
// and from the right side it allocates variable-size slots for data itself.
// Data restrictions of the allocator are made to improve the performance for 4096-byte pages.
//
// Limitations:
// - does not support resizing of slots
// - 65535 slots is hard limit due to uint16 slot count
// - allocator is not stable to external buffer modifications
// - does not provide safety guarantees for concurrent access
type SlotAllocator struct {
	// freeList is a list of free slot headers, used to optimize allocation
	// when searching for free slots
	freeList freeList
	// buffer is the pre-allocated buffer used for allocation
	buffer []byte
	// slotsCount is the number of slots allocated, lazily loaded from the buffer header
	slotsCount uint16
}

// NewSlotAllocator creates a new SlotAllocator with the given buffer
// the buffer should be pre-allocated and have zeroed memory
// allocator would be only managing the memory within the slice
// provided, capacity of the buffer is not taken into account
func NewSlotAllocator(buffer []byte) *SlotAllocator {
	if len(buffer) > math.MaxInt32 {
		log.Warn().Int("buffer_length", len(buffer)).Msg("allocator buffer length exceeds MaxInt32, truncating to MaxInt32")
		buffer = buffer[:math.MaxInt32]
	}

	allocator := &SlotAllocator{
		buffer:     buffer,
		slotsCount: math.MaxUint16,
		freeList:   newFreeList(),
	}

	allocator.loadFreeList()
	return allocator
}

func (a *SlotAllocator) SlotsAllocated() uint16 {
	if a.slotsCount != math.MaxUint16 {
		return a.slotsCount
	}

	_, err := raw.ParseUint16(&a.slotsCount, a.buffer[slotsCountOffset:])
	if err != nil {
		log.Error().Err(err).Msg("failed to parse slots count from allocator header")
		return 0
	}

	return a.slotsCount
}

func (a *SlotAllocator) writeSlotsAllocated(count uint16) error {
	_, err := raw.PutUint16(a.buffer[slotsCountOffset:], count)
	if err != nil {
		return fmt.Errorf("failed to write slots count to allocator header: %w", err)
	}

	a.slotsCount = count

	return nil
}

func (a *SlotAllocator) slotHeaderOffset(index uint16) uint32 {
	return uint32(allocatorHeaderSize) + uint32(index)*uint32(allocatorSlotHeaderSize)
}

func (a *SlotAllocator) slotHeaderAt(index uint16) (slotHeader, error) {
	if index >= a.SlotsAllocated() {
		return slotHeader{}, fmt.Errorf("invalid slot index %d, exceeds allocated slots count %d", index, a.SlotsAllocated())
	}

	offset := a.slotHeaderOffset(index)
	var header slotHeader
	_, err := header.ParseBinary(a.buffer[offset:])
	if err != nil {
		return slotHeader{}, fmt.Errorf("failed to parse slot header at index %d: %w", index, err)
	}

	return header, nil
}

func (a *SlotAllocator) iterSlotHeaders(yield func(slotHeader) bool) {
	slotsCount := a.SlotsAllocated()
	offset := allocatorHeaderSize
	var slot slotHeader
	for i := uint16(0); i < slotsCount; i++ {
		read, err := slot.ParseBinary(a.buffer[offset:])
		if err != nil {
			log.Error().Uint16("index", i).Err(err).Msg("failed to parse slot header")
			return
		}
		offset += read

		if !yield(slot) {
			return
		}
	}
}

func (a *SlotAllocator) loadFreeList() {
	var slotIndex uint16 = 0
	for header := range a.iterSlotHeaders {
		if header.status == slotStatusFree {
			a.addToFreeList(slotIndex, header.size)
		}
		slotIndex++
	}
}

func (a *SlotAllocator) addToFreeList(index uint16, headerSize uint32) {
	added := a.freeList.AddHeader(index, headerSize)
	if !added {
		log.Warn().Uint16("index", index).Msg("duplicate free slot header reference found during add to free list")
	}
}

func (a *SlotAllocator) popFromFreeList(index uint16) {
	removed := a.freeList.MarkHeaderUsed(index)
	if !removed {
		log.Warn().Uint16("index", index).Msg("attempted to remove non-existing free slot header reference from free list")
	}
}

// effectiveAllocatableSizeFrom calculates the effective allocatable size from the given slot index
// it takes into account the space occupied by the next slot header created during the allocation
func (a *SlotAllocator) effectiveAllocatableSizeFrom(index uint16, header slotHeader) uint32 {
	// We need to have an offset on N+2 to get the offset for the end of the next slot
	// which will be available for allocation
	//
	// N = current slot index
	// N+1 = next slot header
	// N+2 = first byte of the user data after the next slot gets allocated
	headerOffset := a.slotHeaderOffset(index + 2)
	if header.dataOffset < headerOffset {
		return 0
	}

	return header.dataOffset - uint32(headerOffset)
}

// effectiveAllocatableSizeEmpty calculates the effective allocatable predending that
// there are no slots allocated yet
func (a *SlotAllocator) effectiveAllocatableSizeEmpty() uint32 {
	return uint32(len(a.buffer)) - uint32(allocatorHeaderSize)
}

func (a *SlotAllocator) allocateNewSlotOfSize(size uint32) (slotHeader, uint16, error) {
	slotsCount := a.SlotsAllocated()

	var dataOffset uint32
	var allocatable uint32
	if slotsCount == 0 {
		dataOffset = uint32(len(a.buffer)) - size
		allocatable = a.effectiveAllocatableSizeEmpty()
	} else {
		lastOffset := a.slotHeaderOffset(slotsCount - 1)
		lastHeader := slotHeader{}
		_, err := lastHeader.ParseBinary(a.buffer[lastOffset:])
		if err != nil {
			return slotHeader{}, 0, err
		}

		dataOffset = lastHeader.dataOffset - size
		allocatable = a.effectiveAllocatableSizeFrom(slotsCount-1, lastHeader)
	}

	if size > allocatable {
		return slotHeader{}, 0, fmt.Errorf("insufficient space to allocate slot of size %d, allocatable %d", size, allocatable)
	}

	header := slotHeader{
		dataOffset: uint32(dataOffset),
		status:     slotStatusAllocated,
		size:       size,
	}

	headerOffset := a.slotHeaderOffset((slotsCount))
	_, err := header.PutBinary(a.buffer[headerOffset:])
	if err != nil {
		return slotHeader{}, 0, err
	}

	err = a.writeSlotsAllocated(slotsCount + 1)
	if err != nil {
		return slotHeader{}, 0, err
	}

	return header, slotsCount, nil
}

func (a *SlotAllocator) allocateFreeSlotOfSize(size uint32) (slotHeader, uint16, error) {
	index, found := a.freeList.HeaderWithCapacity(size)
	if !found {
		return slotHeader{}, 0, noFreeSlotsErr
	}

	headerOffset := a.slotHeaderOffset(index)
	header := slotHeader{}
	_, err := header.ParseBinary(a.buffer[headerOffset:])
	if err != nil {
		return slotHeader{}, 0, err
	}

	if header.status != slotStatusFree || header.size < size {
		log.Warn().Uint16("index", index).Msg("free list contains invalid slot header reference, removing from free list")
		a.popFromFreeList(index)
		return slotHeader{}, 0, noFreeSlotsErr
	}

	header.status = slotStatusAllocated
	_, err = header.PutBinary(a.buffer[headerOffset:])
	if err != nil {
		return slotHeader{}, 0, err
	}

	a.popFromFreeList(index)
	return header, index, nil
}

func (a *SlotAllocator) findSlotOrAllocate(size uint32) (slotHeader, uint16, error) {
	header, index, err := a.allocateFreeSlotOfSize(size)
	if err == nil {
		return header, index, nil
	}

	// We don't want to error out if there are no free slots,
	// only if there was a different error
	if !errors.Is(err, noFreeSlotsErr) {
		return slotHeader{}, 0, err
	}

	header, index, err = a.allocateNewSlotOfSize(size)
	if err != nil {
		return slotHeader{}, 0, err
	}

	return header, index, nil
}

func (a *SlotAllocator) CanFit(size uint32) bool {
	_, _, err := a.allocateFreeSlotOfSize(size)
	if err == nil {
		return true
	}

	if !errors.Is(err, noFreeSlotsErr) {
		log.Error().Err(err).Msg("failed to find free slot: unexpected error")
		return false
	}

	slotsCount := a.SlotsAllocated()
	lastHeader, err := a.slotHeaderAt(slotsCount - 1)
	if err != nil {
		log.Error().Err(err).Msg("failed to parse last slot header")
		return false
	}

	freeSpace := a.effectiveAllocatableSizeFrom(slotsCount-1, lastHeader)
	return size <= freeSpace
}

func (a *SlotAllocator) Allocate(size uint32) (Allocation, error) {
	header, index, err := a.findSlotOrAllocate(size)
	if err != nil {
		return Allocation{}, err
	}

	return Allocation{
		Buffer: a.buffer[header.dataOffset : header.dataOffset+header.size],
		Index:  index,
	}, nil
}

func (a *SlotAllocator) AllocateOrDie(size uint32) Allocation {
	allocation, err := a.Allocate(size)
	if err != nil {
		log.Fatal().Err(err).Uint32("size", size).Msg("failed to allocate slot")
	}

	return allocation
}

func (a *SlotAllocator) Deallocate(allocation Allocation) error {
	headerIndex := allocation.Index
	if headerIndex >= a.SlotsAllocated() {
		return fmt.Errorf("invalid slot index %d, exceeds allocated slots count %d", headerIndex, a.SlotsAllocated())
	}

	headerOffset := a.slotHeaderOffset(headerIndex)
	header := slotHeader{}
	_, err := header.ParseBinary(a.buffer[headerOffset:])
	if err != nil {
		return fmt.Errorf("failed to parse slot header at index %d: %w", headerIndex, err)
	}

	if header.status != slotStatusAllocated {
		return fmt.Errorf("slot at index %d is not allocated", headerIndex)
	}

	header.status = slotStatusFree
	_, err = header.PutBinary(a.buffer[headerOffset:])
	if err != nil {
		return fmt.Errorf("failed to update slot header at index %d: %w", headerIndex, err)
	}

	a.addToFreeList(headerIndex, header.size)
	// zero-out the data for safety and reusability
	clear(a.buffer[header.dataOffset : header.dataOffset+header.size])

	return nil
}

func (a *SlotAllocator) DeallocateOrDie(allocation Allocation) {
	err := a.Deallocate(allocation)
	if err != nil {
		log.Fatal().Err(err).Uint16("index", allocation.Index).Msg("failed to deallocate slot")
	}
}

func (a *SlotAllocator) GetAllocation(index uint16) (Allocation, error) {
	allocated := a.SlotsAllocated()
	if index >= allocated {
		return Allocation{}, fmt.Errorf("invalid slot index %d, exceeds allocated slots count %d", index, allocated)
	}

	headerOffset := a.slotHeaderOffset(index)
	header := slotHeader{}
	_, err := header.ParseBinary(a.buffer[headerOffset:])
	if err != nil {
		return Allocation{}, fmt.Errorf("failed to parse slot header at index %d: %w", index, err)
	}

	if header.status != slotStatusAllocated {
		return Allocation{}, fmt.Errorf("slot at index %d is not allocated", index)
	}

	return Allocation{
		Buffer: a.buffer[header.dataOffset : header.dataOffset+header.size],
		Index:  index,
	}, nil
}

func (a *SlotAllocator) VisitAllocations(visitor func(Allocation) bool) {
	slotIndex := uint16(0)
	for header := range a.iterSlotHeaders {
		if header.status == slotStatusAllocated {
			allocation := Allocation{
				Buffer: a.buffer[header.dataOffset : header.dataOffset+header.size],
				Index:  slotIndex,
			}
			if !visitor(allocation) {
				return
			}
		}
		slotIndex++
	}
}

func (a *SlotAllocator) FreeBytes() uint32 {
	slotsCount := a.SlotsAllocated()
	if slotsCount == 0 {
		return a.effectiveAllocatableSizeEmpty()
	}

	lastHeader, err := a.slotHeaderAt(slotsCount - 1)
	if err != nil {
		log.Error().Err(err).Msg("failed to parse last slot header")
		return 0
	}

	totalFree := a.effectiveAllocatableSizeFrom(slotsCount-1, lastHeader)
	a.freeList.Visit(func(ref freeHeaderRef) bool {
		totalFree += ref.capacity
		return true
	})

	return totalFree

}

func (a *SlotAllocator) LargestAllocatableSize() uint32 {
	slotsCount := a.SlotsAllocated()
	if slotsCount == 0 {
		return a.effectiveAllocatableSizeEmpty()
	}

	lastHeader, err := a.slotHeaderAt(slotsCount - 1)
	if err != nil {
		log.Error().Err(err).Msg("failed to parse last slot header")
		return 0
	}

	largestFree := a.effectiveAllocatableSizeFrom(slotsCount-1, lastHeader)
	a.freeList.Visit(func(ref freeHeaderRef) bool {
		if ref.capacity > largestFree {
			largestFree = ref.capacity
		}
		return true
	})

	return largestFree
}
