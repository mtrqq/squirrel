package engine

import (
	"errors"
	"fmt"
	"os"

	"github.com/mtrqq/squirrel/pkg/binary"
)

const (
	dataOffset     = binary.Int32ByteSize
	pageHeaderSize = 4 * binary.Int32ByteSize
	pageSize       = 8092
	pageDataSize   = pageSize - pageHeaderSize
)

type page struct {
	id              int32
	size            int32
	allocated       int32
	userSpaceOffset int32
	data            []byte
}

func (p *page) ParseBinary(buffer []byte) (int, error) {
	read, err := binary.ParseInt32(&p.id, buffer)
	if err != nil {
		return 0, err
	}

	offsetBuffer := buffer[read:]
	read, err = binary.ParseInt32(&p.size, offsetBuffer)
	if err != nil {
		return 0, err
	}

	if p.size != pageSize {
		return 0, fmt.Errorf("Page size is not matching (%d/%d)", p.size, pageSize)
	}

	offsetBuffer = buffer[read:]
	read, err = binary.ParseInt32(&p.allocated, offsetBuffer)
	if err != nil {
		return 0, err
	}

	offsetBuffer = buffer[read:]
	read, err = binary.ParseInt32(&p.userSpaceOffset, offsetBuffer)
	if err != nil {
		return 0, err
	}

	// TODO: make a buffer copy?
	p.data = buffer[p.userSpaceOffset : p.userSpaceOffset+p.size]
	return int(4*binary.Int32ByteSize + p.size), nil
}

func (p page) EncodeBinary() ([]byte, error) {
	buffer := make([]byte, p.size)
	written, err := p.PutBinary(buffer)
	if err != nil {
		return nil, err
	}

	if int32(written) != p.size {
		return nil, fmt.Errorf("written unexpected count of bytes, got %d, want %d", written, p.size)
	}

	return buffer, nil
}

func (p page) PutBinary(buffer []byte) (int, error) {
	writtenTotal := 0

	written, err := binary.PutInt32(buffer, p.id)
	writtenTotal += written
	if err != nil {
		return writtenTotal, err
	}

	written, err = binary.PutInt32(buffer, p.size)
	writtenTotal += written
	if err != nil {
		return writtenTotal, err
	}

	written, err = binary.PutInt32(buffer, p.allocated)
	writtenTotal += written
	if err != nil {
		return writtenTotal, err
	}

	written, err = binary.PutInt32(buffer, p.userSpaceOffset)
	writtenTotal += written
	if err != nil {
		return writtenTotal, err
	}

	written, err = binary.PutBytes(buffer, p.data)
	writtenTotal += written
	if err != nil {
		return writtenTotal, err
	}

	return writtenTotal, nil
}

func (p page) ByteSizeBinary() int64 {
	return int64(p.size)
}

func (p page) DataSize() int64 {
	return int64(p.size - p.userSpaceOffset)
}

func (p page) Allocatable() int32 {
	return p.size - p.allocated
}

func zeroAllocatedPage() page {
	return page{
		id:              -1,
		allocated:       0,
		size:            pageSize,
		userSpaceOffset: pageHeaderSize,
		data:            make([]byte, pageDataSize),
	}
}

type pager struct {
	fd         *os.File
	pagesCount int32
}

func fileExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}

	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}

	return false, err
}

func initPagingFile(path string) (*os.File, int32, error) {
	fd, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, 0, err
	}

	// TODO: use full header object instead of plain int
	_, err = fd.WriteAt(binary.EncodeInt32(0), 0)
	if err != nil {
		return nil, 0, err
	}

	return fd, 0, nil
}

func loadExistingPagingFile(path string) (*os.File, int32, error) {
	fd, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return nil, 0, err
	}

	// TODO: use full header object instead of plain int
	buffer := make([]byte, binary.Int32ByteSize)
	_, err = fd.ReadAt(buffer, 0)
	if err != nil {
		return nil, 0, err
	}

	var pagesCount int32
	_, err = binary.ParseInt32(&pagesCount, buffer)
	if err != nil {
		return nil, 0, err
	}

	return fd, pagesCount, nil
}

func newPager(path string) (*pager, error) {
	exists, err := fileExists(path)
	if err != nil {
		return nil, err
	}

	var fd *os.File
	var pagesCount int32
	if exists {
		fd, pagesCount, err = loadExistingPagingFile(path)
	} else {
		fd, pagesCount, err = initPagingFile(path)
	}

	if err != nil {
		return nil, err
	}

	return &pager{fd: fd, pagesCount: pagesCount}, nil
}

func (pg *pager) pageOffset(n int32) int64 {
	return int64(dataOffset + n*pageSize)
}

func (pg *pager) FetchPage(n int32) (page, error) {
	if n >= pg.pagesCount {
		return page{}, fmt.Errorf("page with id %d not found", n)
	}

	buffer := make([]byte, pageSize)
	read, err := pg.fd.ReadAt(buffer, pg.pageOffset(n))
	if err != nil {
		return page{}, err
	}

	if read != len(buffer) {
		return page{}, fmt.Errorf("invalid number of bytes read for page, got %d, want %d", read, len(buffer))
	}

	var p page
	read, err = p.ParseBinary(buffer)
	if err != nil {
		return page{}, err
	}

	if read != len(buffer) {
		return page{}, fmt.Errorf("invalid number of bytes read for page, got %d, want %d", read, len(buffer))
	}

	return p, nil
}

func (pg *pager) AppendPage() (page, error) {
	newPage := zeroAllocatedPage()
	buffer := make([]byte, newPage.size)
	written, err := newPage.PutBinary(buffer)
	if err != nil {
		return page{}, err
	}

	if written != len(buffer) {
		return page{}, fmt.Errorf("invalid number of bytes written to buffer, got %d, want %d", written, len(buffer))
	}

	offset := pg.pageOffset(pg.pagesCount)
	written, err = pg.fd.WriteAt(buffer, offset)
	if err != nil {
		return page{}, err
	}

	if written != len(buffer) {
		return page{}, fmt.Errorf("invalid number of bytes written for page, got %d, want %d", written, len(buffer))
	}

	pg.pagesCount += 1
	return newPage, nil
}

func (pg *pager) UpdatePage(n int32, data []byte, newAllocated int32) (page, error) {
	if n >= pg.pagesCount {
		return page{}, fmt.Errorf("page with id %d not found", n)
	}

	p, err := pg.FetchPage(n)
	if err != nil {
		return page{}, err
	}

	if len(p.data) > int(p.DataSize()) {
		return page{}, fmt.Errorf("unable to fit buffer of size %d into a page", len(p.data))
	}

	if len(p.data) < int(p.DataSize()) {
		paddedData := make([]byte, p.DataSize())
		copy(paddedData, data)
		data = paddedData
	}

	p.data = data
	p.allocated = newAllocated

	return p, nil
}
