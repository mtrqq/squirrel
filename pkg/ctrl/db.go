package ctrl

import (
	"errors"
	"fmt"

	"github.com/mtrqq/squirrel/pkg/page"
)

type Database struct {
	pager *page.Pager
}

func NewDatabaseFromPath(path string) (Database, error) {
	pager, err := page.NewPager(path)
	if err != nil {
		return Database{}, fmt.Errorf("failure when initializing db: %w", err)
	}

	return Database{pager: pager}, nil
}

func (db Database) AddTable(table page.TableDescriptor) error {
	metadata, err := db.pager.MetadataPage()
	if err != nil {
		return fmt.Errorf("unable to add table %s: failed to load metadata page: %w", table.Name, err)
	}

	if err := metadata.AddTable(table); err != nil {
		return fmt.Errorf("unable to add table %s: %w", table.Name, err)
	}

	return nil
}

func (db Database) TableExists(name string) (bool, error) {
	metadata, err := db.pager.MetadataPage()
	if err != nil {
		return false, fmt.Errorf("unable to check table %s existence: failed to load metadata page: %w", name, err)
	}

	_, err = metadata.TableByName(name)
	if err != nil {
		if errors.Is(err, page.ErrTableNotFound) {
			return false, nil
		}

		return false, fmt.Errorf("failed to load table descriptor: %w")
	}

	return true, nil
}

func (db Database) Table(name string) (TableContext, error) {
	metadata, err := db.pager.MetadataPage()
	if err != nil {
		return TableContext{}, fmt.Errorf("unable to fetch table %s: failed to load metadata page: %w", name, err)
	}

	table, err := metadata.TableByName(name)
	if err != nil {
		return TableContext{}, fmt.Errorf("unable to fetch table %s: %w", name, err)
	}

	return TableContext{
		name:       name,
		descriptor: table,
		db:         db,
	}, nil
}

func (db Database) Close() error {
	return db.pager.Close()
}
