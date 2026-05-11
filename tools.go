//go:build tools
// +build tools

// This file exists solely to keep dependencies in go.mod even when they are
// not yet imported by production code or active tests. Phase 2 brought
// franz-go/kgo (relay + worker) and testcontainers-go (integration tests
// in internal/{store,dispatcher,relay,worker,reaper,itest}) into real use,
// so their blank imports drop here. go-redis/v9 stays — Phase 3 wires the
// rate limiter against it.
//
// docs/phases/02-walking-skeleton.md §Repo layout (tools.go row) +
// §Chunk 7.

package tools

import (
	_ "github.com/redis/go-redis/v9"
)
