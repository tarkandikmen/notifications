// Package migrate wraps golang-migrate as a library. The migrate subcommand
// in cmd/notifications/main.go invokes Up and Down directly.
package migrate

import (
	"errors"
	"fmt"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

// Up applies every pending migration found at sourceURL against databaseURL.
// A no-op (migrate.ErrNoChange) is treated as success.
func Up(databaseURL, sourceURL string) error {
	m, err := migrate.New(sourceURL, databaseURL)
	if err != nil {
		return fmt.Errorf("migrate: new: %w", err)
	}
	defer closeMigrator(m)

	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("migrate: up: %w", err)
	}
	return nil
}

// Down reverts every applied migration. As with Up, ErrNoChange is success.
func Down(databaseURL, sourceURL string) error {
	m, err := migrate.New(sourceURL, databaseURL)
	if err != nil {
		return fmt.Errorf("migrate: new: %w", err)
	}
	defer closeMigrator(m)

	if err := m.Down(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("migrate: down: %w", err)
	}
	return nil
}

// closeMigrator drops the migrator's own connection. migrate.New opens its
// own pool; failing to close it leaves an orphaned connection at process
// exit. The errors here are non-actionable for the caller.
func closeMigrator(m *migrate.Migrate) {
	srcErr, dbErr := m.Close()
	_ = srcErr
	_ = dbErr
}
