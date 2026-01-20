package allocator

import "github.com/mtrqq/squirrel/pkg/raw"

type slotStatus uint8

const (
	slotStatusFree      slotStatus = 0
	slotStatusAllocated slotStatus = 1
)

type slotHeader struct {
	dataOffset uint32
	size       uint32
	status     slotStatus
}

func (s *slotHeader) ParseBinary(data []byte) (int, error) {
	readTotal := 0
	read, err := raw.ParseUint32(&s.dataOffset, data)
	if err != nil {
		return 0, err
	}
	readTotal += read

	read, err = raw.ParseUint32(&s.size, data[readTotal:])
	if err != nil {
		return 0, err
	}
	readTotal += read

	read, err = raw.ParseUint8((*uint8)(&s.status), data[readTotal:])
	if err != nil {
		return 0, err
	}
	readTotal += read

	return readTotal, nil
}

func (s slotHeader) PutBinary(data []byte) (int, error) {
	writtenTotal := 0
	written, err := raw.PutUint32(data, s.dataOffset)
	if err != nil {
		return 0, err
	}
	writtenTotal += written

	written, err = raw.PutUint32(data[writtenTotal:], s.size)
	if err != nil {
		return writtenTotal, err
	}
	writtenTotal += written

	written, err = raw.PutUint8(data[writtenTotal:], uint8(s.status))
	if err != nil {
		return writtenTotal, err
	}
	writtenTotal += written

	return writtenTotal, nil
}
