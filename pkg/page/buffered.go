package page

import (
	"fmt"
	"sync/atomic"

	"github.com/mtrqq/squirrel/pkg/raw"
	"github.com/rs/zerolog/log"
)

const (
	// pageSize is the fixed size of a page in bytes, includes header and data sizes
	pageSize = 4096
	// pageDataSize is the size of the data portion of the page in bytes
	pageDataSize = pageSize - pageHeaderSize
	// pageVersion is the current version of the page structure
	pageVersion = 1

	// Offsets within the page header, these are used for binary serialization/deserialization
	// and assume specific sizes for each field.
	pageIdSize        = raw.Int32ByteSize
	pageVersionSize   = raw.Int8ByteSize
	pageTypeSize      = raw.Int8ByteSize
	pageIdOffset      = 0
	pageVersionOffset = pageIdOffset + pageIdSize
	pageTypeOffset    = pageVersionOffset + pageVersionSize
	pageHeaderSize    = pageTypeOffset + pageTypeSize
)

type PageType uint8

const (
	PageTypeRow      PageType = 1
	PageTypeMetadata PageType = 2
)

type BufferPage struct {
	// flushCallback is a callback function to be called when the page needs to
	// be flushed to disk
	flushCallback func(p *BufferPage) error
	// pins indicates how many pins are on the page
	pins atomic.Int32
	// referenceBit indicates if the page has been recently accessed
	referenceBit atomic.Bool
	// isDirty indicates if the page has been modified
	isDirty atomic.Bool
	// initializedBit signals whether the page has been initialized
	// and whether its ready for use
	initializedBit atomic.Bool
	// pageBlock a full snapshot of the page including header and payload itself
	pageBlock [pageSize]byte
	// data is a slice pointing to the data portion of the page, does not include header
	data []byte
}

func (p *BufferPage) Id() uint32 {
	var id uint32
	_, err := raw.ParseUint32(&id, p.pageBlock[pageIdOffset:pageIdOffset+pageIdSize])
	if err != nil {
		log.Error().Err(err).Msg("failed to parse page id from page data")
		return 0
	}
	return id
}

func (p *BufferPage) SetId(id uint32) {
	_, err := raw.PutUint32(p.pageBlock[pageIdOffset:], id)
	if err != nil {
		log.Error().Err(err).Msg("failed to set page id in data")
	}

	p.markDirty()
}

func (p *BufferPage) Version() uint8 {
	var version uint8
	_, err := raw.ParseUint8(&version, p.pageBlock[pageVersionOffset:pageVersionOffset+pageVersionSize])
	if err != nil {
		log.Error().Uint32("id", p.Id()).Err(err).Msg("failed to parse page version from page data")
		return 0
	}
	return version
}

func (p *BufferPage) SetVersion() {
	_, err := raw.PutUint8(p.pageBlock[pageVersionOffset:], pageVersion)
	if err != nil {
		log.Error().Uint32("id", p.Id()).Err(err).Msg("failed to set page version in data")
	}

	p.markDirty()
}

func (p *BufferPage) PageType() PageType {
	var pt uint8
	_, err := raw.ParseUint8(&pt, p.pageBlock[pageTypeOffset:pageTypeOffset+pageTypeSize])
	if err != nil {
		log.Error().Uint32("id", p.Id()).Err(err).Msg("failed to parse page type from page data")
		return 0
	}
	return PageType(pt)
}

func (p *BufferPage) SetPageType(pt PageType) {
	_, err := raw.PutUint8(p.pageBlock[pageTypeOffset:], uint8(pt))
	if err != nil {
		log.Error().Uint32("id", p.Id()).Err(err).Msg("failed to set page type in data")
	}

	p.markDirty()
}

func (p *BufferPage) Data() []byte {
	if p.data == nil {
		p.data = p.pageBlock[pageHeaderSize:]
	}

	return p.data
}

func (p *BufferPage) IsPinned() bool {
	return p.pins.Load() > 0
}

func (p *BufferPage) Pin() {
	p.pins.Add(1)
	p.setReferenceBit()
}

func (p *BufferPage) Unpin() {
	currentPins := p.pins.Load()
	if currentPins == 0 {
		log.Error().Uint32("id", p.Id()).Msg("Attempted to unpin not pinned page")
	}

	currentPins = p.pins.Add(-1)
	if currentPins < 0 {
		log.Error().Uint32("id", p.Id()).Msg("Page pins went negative")
		p.pins.Store(0)
	}
	p.setReferenceBit()
}

func (p *BufferPage) validateVersion() error {
	version := p.Version()
	if version != pageVersion {
		return fmt.Errorf("invalid page version, got %d, want %d", version, pageVersion)
	}

	return nil
}

// bind resets the metadata of the page to its initial state and assigns it the given id.
func (p *BufferPage) bind(id uint32, flushCallback func(p *BufferPage) error) error {
	if p.isDirty.Load() && p.flushCallback != nil {
		// Call the eviction callback before rebinding itself
		err := p.flushCallback(p)
		if err != nil {
			return err
		}
		p.clearDirty()
	}

	if p.IsPinned() {
		log.Error().Uint32("id", p.Id()).Msg("Binding a pinned page")
	}

	p.markInitialized()
	p.setReferenceBit()
	p.pins.Store(0)
	p.flushCallback = flushCallback
	// To avoid data corruption we clear the data buffer
	// when binding it to the new id.
	clear(p.pageBlock[:])
	p.SetVersion()
	p.SetId(id)
	// Clearing dirty flag should be performed after
	// all the mutations are done.
	p.clearDirty()
	return nil
}

func (p *BufferPage) getReferenceBit() bool {
	return p.referenceBit.Load()
}

func (p *BufferPage) clearReferenceBit() {
	p.referenceBit.Store(false)
}

func (p *BufferPage) setReferenceBit() {
	p.referenceBit.Store(true)
}

func (p *BufferPage) markDirty() {
	p.isDirty.Store(true)
}

func (p *BufferPage) getIsDirty() bool {
	return p.isDirty.Load()
}

func (p *BufferPage) clearDirty() {
	p.isDirty.Store(false)
}

func (p *BufferPage) markInitialized() {
	p.initializedBit.Store(true)
}

func (p *BufferPage) getIsInitialized() bool {
	return p.initializedBit.Load()
}
