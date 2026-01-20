package ctrl

type TID struct {
	PageID uint32
	SlotID uint16
}

func (t TID) AsNumber() uint64 {
	return (uint64(t.PageID) << 16) | uint64(t.SlotID)
}

func TIDFromNumber(num uint64) TID {
	return TID{
		PageID: uint32(num >> 16),
		SlotID: uint16(num & 0xFFFF),
	}
}
