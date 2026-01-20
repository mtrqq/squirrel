package page

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/rs/zerolog/log"
)

const (
	metadataPageId = 0
)

type Pager struct {
	fd   *os.File
	pool *clockPagePool
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

func initPagingFile(path string) (*os.File, error) {
	fd, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	return fd, nil
}

func loadExistingPagingFile(path string) (*os.File, error) {
	fd, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	return fd, nil
}

func NewPager(path string) (*Pager, error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}

	exists, err := fileExists(path)
	if err != nil {
		return nil, err
	}

	if exists {
		fd, err := loadExistingPagingFile(path)
		if err != nil {
			return nil, err
		}
		pager := &Pager{fd: fd, pool: newClockPagePool(16)}
		return pager, nil
	}

	fd, err := initPagingFile(path)
	if err != nil {
		return nil, err
	}
	pager := &Pager{fd: fd, pool: newClockPagePool(16)}

	_, err = pager.appendMetadataPage()
	if err != nil {
		return nil, err
	}

	return pager, nil
}

func (pg *Pager) pageOffset(n uint32) int64 {
	return int64(n) * int64(pageSize)
}

func (pg *Pager) flushPageToDisk(p *BufferPage) error {
	offset := pg.pageOffset(p.Id())
	_, err := pg.fd.WriteAt(p.pageBlock[:], offset)
	if err != nil {
		return fmt.Errorf("failed to flush page#%d to file: %w", p.Id(), err)
	}
	p.clearDirty()
	return nil
}

func (pg *Pager) FetchPage(n uint32) (*BufferPage, error) {
	page, found := pg.pool.GetPage(n)
	if found {
		return page, nil
	}

	page, err := pg.pool.AllocatePage(n, pg.flushPageToDisk)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate page: %w", err)
	}

	read, err := pg.fd.ReadAt(page.pageBlock[:], pg.pageOffset(n))
	if err != nil {
		return nil, fmt.Errorf("failed to read from pager file: %w", err)
	}

	if read != len(page.pageBlock) {
		return nil, fmt.Errorf("invalid number of bytes read for page, got %d, want %d", read, len(page.pageBlock))
	}

	err = page.validateVersion()
	if err != nil {
		return nil, fmt.Errorf("failed to validate page version: %w", err)
	}

	return page, nil
}

// appendPageNoMetadata appends a new page without updating the metadata page
// this matters on the first page creation when the metadata page itself is being created
func (pg *Pager) appendPageNoMetadata(id uint32) (*BufferPage, error) {
	page, err := pg.pool.AllocatePage(id, pg.flushPageToDisk)
	if err != nil {
		return nil, err
	}

	offset := pg.pageOffset(id)
	written, err := pg.fd.WriteAt(page.pageBlock[:], offset)
	if err != nil {
		return nil, fmt.Errorf("failed to write new page data to the file: %w", err)
	}

	if written != len(page.pageBlock) {
		return nil, fmt.Errorf("invalid number of bytes written for page, got %d, want %d", written, len(page.pageBlock))
	}

	return page, nil
}

// appendMetadataPage appends a new metadata page and initializes it
// we assume that this page is being created on an empty pager file
// and there can be only one metadata page at index 0
func (pg *Pager) appendMetadataPage() (MetadataPage, error) {
	page, err := pg.appendPageNoMetadata(metadataPageId)
	if err != nil {
		return MetadataPage{}, err
	}

	page.SetPageType(PageTypeMetadata)
	metadataPage, err := NewMetadataPage(page)
	if err != nil {
		return MetadataPage{}, fmt.Errorf("unable to create metadata page#%d: %w", page.Id(), err)
	}
	metadataPage.SetPagesCount(1)
	return metadataPage, nil
}

// AppendPage appends a new page and updates the metadata page accordingly
func (pg *Pager) AppendPage(pageType PageType) (*BufferPage, error) {
	metadataPage, err := pg.MetadataPage()
	if err != nil {
		return nil, err
	}

	page, err := pg.appendPageNoMetadata(metadataPage.PagesCount())
	if err != nil {
		return nil, err
	}

	pg.setPagesCount(metadataPage.PagesCount() + 1)
	page.SetPageType(pageType)
	return page, nil
}

func (pg *Pager) Close() error {
	if err := pg.Sync(); err != nil {
		return fmt.Errorf("failed to sync before close: %w", err)
	}

	return pg.fd.Close()
}

func (pg *Pager) PagesCount() uint32 {
	metadataPage, err := pg.MetadataPage()
	if err != nil {
		log.Error().Err(err).Msg("failed to fetch metadata page to get pages count")
		return 0
	}
	return metadataPage.PagesCount()
}

func (pg *Pager) setPagesCount(count uint32) {
	metadataPage, err := pg.MetadataPage()
	if err != nil {
		log.Error().Err(err).Msg("failed to fetch metadata page to set pages count")
		return
	}
	metadataPage.SetPagesCount(count)
}

func (pg *Pager) Sync() error {
	err := pg.pool.VisitPages(func(p *BufferPage) error {
		if !p.getIsDirty() {
			return nil
		}

		return pg.flushPageToDisk(p)
	})

	if err != nil {
		return fmt.Errorf("failed to flush dirty pages: %w", err)
	}

	return pg.fd.Sync()
}

func (pg *Pager) MetadataPage() (MetadataPage, error) {
	page, err := pg.FetchPage(metadataPageId)
	if err != nil {
		return MetadataPage{}, fmt.Errorf("unable to fetch metadata page: %w", err)
	}

	metadataPage, err := NewMetadataPage(page)
	if err != nil {
		return MetadataPage{}, fmt.Errorf("unable to create metadata page: %w", err)
	}

	return metadataPage, nil
}
