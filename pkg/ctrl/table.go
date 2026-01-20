package ctrl

import (
	"fmt"

	"github.com/mtrqq/squirrel/pkg/item"
	"github.com/mtrqq/squirrel/pkg/page"
)

var (
	errNoSpaceInExistingPages = fmt.Errorf("no space in existing pages")
)

type TableContext struct {
	name       string
	descriptor page.TableDescriptor
	db         Database
}

func (tc TableContext) Name() string {
	return tc.name
}

func (tc TableContext) insertIntoExisting(values ...item.Item) (TID, error) {
	for _, pageId := range tc.descriptor.DataPages {
		pg, err := tc.db.pager.FetchPage(pageId)
		if err != nil {
			return TID{}, fmt.Errorf("unable to load row page #%d for table %s: %w", pageId, tc.name, err)
		}

		rowPage, err := page.NewRowPage(pg, tc.descriptor.RowSchema())
		if err != nil {
			return TID{}, fmt.Errorf("unable to initialize row page #%d for table %s: %w", pageId, tc.name, err)
		}

		if rowPage.CanFitItems(values) {
			slot, err := rowPage.InsertRow(values)
			if err != nil {
				return TID{}, fmt.Errorf("unable to insert row into page #%d for table %s: %w", pageId, tc.name, err)
			}

			return TID{
				PageID: pageId,
				SlotID: uint16(slot),
			}, nil
		}
	}
	return TID{}, errNoSpaceInExistingPages
}

func (tc TableContext) insertIntoNewPage(values ...item.Item) (TID, error) {
	pg, err := tc.db.pager.AppendPage(page.PageTypeRow)
	if err != nil {
		return TID{}, fmt.Errorf("unable to append new row page for table %s: %w", tc.name, err)
	}

	rowPage, err := page.NewRowPage(pg, tc.descriptor.RowSchema())
	if err != nil {
		return TID{}, fmt.Errorf("unable to initialize new row page for table %s: %w", tc.name, err)
	}

	slot, err := rowPage.InsertRow(values)
	if err != nil {
		return TID{}, fmt.Errorf("unable to insert row into new page for table %s: %w", tc.name, err)
	}

	// Update table descriptor to include the new data page
	tc.descriptor.DataPages = append(tc.descriptor.DataPages, pg.Id())
	metadata, err := tc.db.pager.MetadataPage()
	if err != nil {
		return TID{}, fmt.Errorf("unable to load metadata page to update table %s: %w", tc.name, err)
	}

	if err := metadata.UpdateTable(tc.descriptor); err != nil {
		return TID{}, fmt.Errorf("unable to update table %s in metadata page: %w", tc.name, err)
	}

	return TID{
		PageID: pg.Id(),
		SlotID: uint16(slot),
	}, nil
}

func (tc TableContext) Insert(values ...item.Item) (TID, error) {
	if len(values) != len(tc.descriptor.Columns) {
		return TID{}, fmt.Errorf("invalid number of items provided for insert: want %d, got %d", len(tc.descriptor.Columns), len(values))
	}

	tid, err := tc.insertIntoExisting(values...)
	if err == nil {
		return tid, nil
	}

	if err != errNoSpaceInExistingPages {
		return TID{}, err
	}

	return tc.insertIntoNewPage(values...)
}

// SelectAll retrieves all rows from the table, this is extremely inefficient
// and is only meant for testing and debugging purposes during the early stages
func (tc TableContext) SelectAll() ([][]item.ItemView, error) {
	var result [][]item.ItemView
	for _, pageId := range tc.descriptor.DataPages {
		pg, err := tc.db.pager.FetchPage(pageId)
		if err != nil {
			return nil, fmt.Errorf("unable to load row page #%d for table %s: %w", pageId, tc.name, err)
		}

		rowPage, err := page.NewRowPage(pg, tc.descriptor.RowSchema())
		if err != nil {
			return nil, fmt.Errorf("unable to initialize row page #%d for table %s: %w", pageId, tc.name, err)
		}

		for _, items := range rowPage.IterRows {
			result = append(result, items)
		}
	}

	return result, nil
}
