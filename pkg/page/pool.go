package page

import (
	"errors"
	"fmt"
	"sync"

	"github.com/rs/zerolog/log"
)

func nextHandIndex(current, capacity int) int {
	if current+1 >= capacity {
		return 0
	}

	return current + 1
}

// clockPagePool implements a simple clock-based page replacement algorithm.
// It maintains a circular list of pages and a reference bit for each page to track usage.
// When a page needs to be replaced, it checks the reference bit of the pages in a circular manner.
//
// Ideally, page pool would be operating on the bare buffers instead of page objects,
// but for simplicity and ease of implementation we are using page objects directly.
type clockPagePool struct {
	addresses map[uint32]*BufferPage
	pages     []BufferPage
	hand      int
	lock      sync.RWMutex
}

func newClockPagePool(bufferSize int) *clockPagePool {
	return &clockPagePool{
		addresses: make(map[uint32]*BufferPage, bufferSize),
		pages:     make([]BufferPage, bufferSize),
		hand:      0,
		lock:      sync.RWMutex{},
	}
}

// getHandPage returns the page at the current hand position and advances the hand.
func (ca *clockPagePool) getHandPage() *BufferPage {
	p := &ca.pages[ca.hand]
	ca.hand = nextHandIndex(ca.hand, len(ca.pages))
	return p
}

func (ca *clockPagePool) AllocatePage(id uint32, flushCallback func(p *BufferPage) error) (*BufferPage, error) {
	ca.lock.Lock()
	defer ca.lock.Unlock()

	if _, exists := ca.addresses[id]; exists {
		return nil, fmt.Errorf("attempted to allocate page that is already allocated, page id: %d", id)
	}

	victim, err := ca.evictPage()
	if err != nil {
		return nil, err
	}
	// We need to perform the deletion only for the non-initialized pages,
	// as un-initialized pages are not tracked in the addresses map and this
	// may lead to accidental deletion of other pages bound to zero id.
	if victim.getIsInitialized() {
		delete(ca.addresses, victim.Id())
	}
	err = victim.bind(id, flushCallback)
	if err != nil {
		return nil, err
	}

	ca.addresses[id] = victim
	return victim, nil
}

func (ca *clockPagePool) GetPage(id uint32) (*BufferPage, bool) {
	ca.lock.RLock()
	defer ca.lock.RUnlock()

	p, exists := ca.addresses[id]
	if !exists {
		return nil, false
	}

	p.setReferenceBit()
	return p, true
}

func (ca *clockPagePool) VisitPages(f func(p *BufferPage) error) error {
	ca.lock.RLock()
	defer ca.lock.RUnlock()

	for _, p := range ca.addresses {
		if err := f(p); err != nil {
			return err
		}
	}

	return nil
}

// evictPage selects a page to evict using the clock algorithm, does not perform any mutations
// to the page or the page pool itself.
func (ca *clockPagePool) evictPage() (*BufferPage, error) {
	for i := 0; i < len(ca.pages)*2; i++ {
		p := ca.getHandPage()
		if p == nil {
			log.Error().Msg("encountered nil page in clock hand")
			continue
		}

		if p.IsPinned() {
			continue
		}

		if p.getReferenceBit() {
			p.clearReferenceBit()
			continue
		}

		return p, nil
	}

	return nil, errors.New("unable to evict any page, allocation buffer is full")
}
