package main

import (
	"fmt"
	"math/rand"

	"github.com/mtrqq/squirrel/pkg/ctrl"
	"github.com/mtrqq/squirrel/pkg/item"
	"github.com/mtrqq/squirrel/pkg/page"
	"github.com/rs/zerolog/log"
)

const (
	users = "users"
)

func firstLaunchSetup() error {
	db, err := ctrl.NewDatabaseFromPath("./test.db")
	if err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}
	defer db.Close()

	exists, err := db.TableExists(users)
	if err != nil {
		return fmt.Errorf("failed to check if table exists: %w", err)
	}

	if !exists {
		log.Info().Msg("Table users does not exist, creating a new one...")
		table := page.TableDescriptor{
			Name: "users",
			Columns: []page.ColumnDescriptor{
				{
					Type: item.ItemTypeInteger,
					Name: "id",
				},
				{
					Type: item.ItemTypeString,
					Name: "name",
				},
			},
		}
		err = db.AddTable(table)
		if err != nil {
			return fmt.Errorf("failed to add table: %w", err)
		}
		log.Info().Msg("Table users created successfully.")
	} else {
		log.Info().Msg("Tables already exist in the database.")
	}

	log.Info().Msg("First launch setup completed.")

	table, err := db.Table(users)
	if err != nil {
		return fmt.Errorf("failed to get table: %w", err)
	}

	id := rand.Int63n(10000)
	name := fmt.Sprintf("User%d", id)
	tid, err := table.Insert(item.Int64(id), item.String(name))
	if err != nil {
		return fmt.Errorf("failed to insert row: %w", err)
	}

	log.Info().Uint32("tid.pageid", tid.PageID).Uint16("tid.slotid", tid.SlotID).Msg("Inserted row successfully")

	// I'm a bit embarrassed to admit that table context requires an explicit update
	// in order to have properly set data pages after an insert.
	table, err = db.Table(users)
	if err != nil {
		return fmt.Errorf("failed to get table: %w", err)
	}

	items, err := table.SelectAll()
	if err != nil {
		return fmt.Errorf("failed to select all rows: %w", err)
	}

	log.Info().Int("count", len(items)).Msg("Selected all rows successfully")
	for idx, row := range items {
		log.Info().Msgf("Row: %v", row)
		id := row[0].Int64OrDie()
		name := row[1].StringOrDie()
		log.Info().Int64("id", id).Str("name", name).Msgf("User#%d", idx+1)
	}

	log.Info().Msg("It worked?!11")
	return nil
}

func main() {

	err := firstLaunchSetup()
	if err != nil {
		log.Fatal().Err(err).Msg("First launch setup failed")
	}
}
