//go:build tools
// +build tools

// This file exists solely to keep dependencies in go.mod even when they are
// not yet imported by production code or active tests. Phase 1 ships the
// dependency pins for Phase 2+ per docs/phases/01-foundation.md §2; the
// testcontainers integration tests land in Phase 2.

package tools

import (
	_ "github.com/redis/go-redis/v9"
	_ "github.com/testcontainers/testcontainers-go"
	_ "github.com/twmb/franz-go/pkg/kgo"
)
