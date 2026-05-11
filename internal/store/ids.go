package store

import "github.com/google/uuid"

// NewID returns a fresh UUIDv7. The api package mints the notification id
// before INSERT per docs/design/01-schema.md §1 ("Generated app-side"); no
// other component generates notification IDs.
//
// docs/phases/02-walking-skeleton.md §2.
func NewID() (uuid.UUID, error) {
	return uuid.NewV7()
}
