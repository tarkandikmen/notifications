package store

import "github.com/google/uuid"

// NewID returns a fresh UUIDv7. The api package mints the notification id
// before INSERT; no other component generates notification IDs.
func NewID() (uuid.UUID, error) {
	return uuid.NewV7()
}
