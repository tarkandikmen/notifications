// Package config loads runtime configuration from environment variables.
//
// The binary calls godotenv.Load() at startup (in cmd/notifications/main.go),
// after which os.Getenv returns either the value from .env or, when present,
// the pre-existing OS-level environment value. See docs/phases/00-phases.md
// §Cross-cutting decisions.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
)

// Config holds every value the binary needs at startup. Sourced from the
// environment by Load. The shape is locked by docs/phases/01-foundation.md §4.
type Config struct {
	HTTPAddr     string
	DatabaseURL  string
	RedisURL     string
	KafkaBrokers []string
	LogLevel     slog.Level
	OTelEndpoint string
	WebhookURL   string
}

// ErrMissingRequired is returned by Load when one or more required env vars
// are unset or empty. The wrapped error names each missing variable.
var ErrMissingRequired = errors.New("config: missing required environment variable")

// ErrInvalidValue is returned by Load when a value is set but cannot be parsed
// (for example, an unrecognized LOG_LEVEL).
var ErrInvalidValue = errors.New("config: invalid environment variable value")

// Load reads the process environment and returns a populated *Config.
//
// Required variables: DATABASE_URL, REDIS_URL, KAFKA_BROKERS. Missing or
// empty values produce an ErrMissingRequired-wrapped error. Optional
// variables fall back to documented defaults.
//
// WEBHOOK_URL is treated as optional here (Phase 1); Phase 2 will tighten it
// to required at the worker layer where the value is actually consumed.
func Load() (*Config, error) {
	var missing []string
	required := func(key string) string {
		v := strings.TrimSpace(os.Getenv(key))
		if v == "" {
			missing = append(missing, key)
		}
		return v
	}

	databaseURL := required("DATABASE_URL")
	redisURL := required("REDIS_URL")
	kafkaCSV := required("KAFKA_BROKERS")

	if len(missing) > 0 {
		return nil, fmt.Errorf("%w: %s", ErrMissingRequired, strings.Join(missing, ", "))
	}

	level, err := parseLogLevel(os.Getenv("LOG_LEVEL"))
	if err != nil {
		return nil, err
	}

	cfg := &Config{
		HTTPAddr:     stringDefault(os.Getenv("HTTP_ADDR"), ":8080"),
		DatabaseURL:  databaseURL,
		RedisURL:     redisURL,
		KafkaBrokers: splitCSV(kafkaCSV),
		LogLevel:     level,
		OTelEndpoint: strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")),
		WebhookURL:   strings.TrimSpace(os.Getenv("WEBHOOK_URL")),
	}

	if len(cfg.KafkaBrokers) == 0 {
		return nil, fmt.Errorf("%w: KAFKA_BROKERS resolved to zero brokers", ErrInvalidValue)
	}

	return cfg, nil
}

func parseLogLevel(raw string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "info":
		return slog.LevelInfo, nil
	case "debug":
		return slog.LevelDebug, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("%w: LOG_LEVEL=%q", ErrInvalidValue, raw)
	}
}

func stringDefault(v, fallback string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return fallback
	}
	return v
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
