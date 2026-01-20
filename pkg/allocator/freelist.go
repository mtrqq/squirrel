package allocator

type freeHeaderRef struct {
	next     *freeHeaderRef
	prev     *freeHeaderRef
	capacity uint32
	index    uint16
}

type freeList struct {
	head  *freeHeaderRef
	index map[uint16]*freeHeaderRef
}

func newFreeList() freeList {
	return freeList{
		head:  nil,
		index: make(map[uint16]*freeHeaderRef),
	}
}

func (f freeList) Visit(visitor func(ref freeHeaderRef) bool) {
	current := f.head
	for current != nil {
		if !visitor(*current) {
			return
		}
		current = current.next
	}
}

func (f freeList) HeaderWithCapacity(minCapacity uint32) (uint16, bool) {
	current := f.head
	for current != nil {
		if current.capacity >= minCapacity {
			return current.index, true
		}
		current = current.next
	}

	return 0, false
}

func (f freeList) MarkHeaderUsed(index uint16) bool {
	ref, exists := f.index[index]
	if !exists {
		return false
	}

	if ref.prev != nil {
		ref.prev.next = ref.next
	}

	if ref.next != nil {
		ref.next.prev = ref.prev
	}

	if f.head == ref {
		f.head = ref.next
	}

	delete(f.index, index)

	return true
}

func (f freeList) AddHeader(index uint16, capacity uint32) bool {
	ref := &freeHeaderRef{
		index:    index,
		capacity: capacity,
		next:     nil,
		prev:     nil,
	}

	if _, exists := f.index[index]; exists {
		return false
	} else {
		f.index[index] = ref
	}

	if f.head == nil {
		f.head = ref
		return true
	}

	if f.head.capacity >= capacity {
		ref.next = f.head
		f.head.prev = ref
		f.head = ref
		return true
	}

	var current *freeHeaderRef
	var prev *freeHeaderRef
	for current = f.head; current != nil; current = current.next {
		if current.capacity >= capacity {
			ref.next = current
			ref.prev = current.prev
			if current.prev != nil {
				current.prev.next = ref
			}
			current.prev = ref
			return true
		}

		prev = current
	}

	prev.next = ref
	return true
}
