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
// environment by Load. The shape is locked by docs/phases/01-foundation.md §4;
// docs/phases/05-observability.md §10 adds MetricsAddr.
type Config struct {
	HTTPAddr     string
	DatabaseURL  string
	RedisURL     string
	KafkaBrokers []string
	LogLevel     slog.Level
	OTelEndpoint string
	WebhookURL   string
	// MetricsAddr binds the per-binary /metrics + /healthz endpoint
	// served by internal/metricsserver. Defaults to :9090. The api
	// binary additionally exposes /metrics on HTTPAddr (the Phase 1
	// :8080 contract); MetricsAddr is the uniform per-binary
	// Prometheus scrape target every binary exposes.
	MetricsAddr string
}

// ErrMissingRequired is returned by Load when one or more required env vars
// are unset or empty. The wrapped error names each missing variable.
var ErrMissingRequired = errors.New("config: missing required environment variable")

// ErrInvalidValue is returned by Load when a value is set but cannot be parsed
// (for example, an unrecognized LOG_LEVEL).
var ErrInvalidValue = errors.New("config: invalid environment variable value")

// webhookURLPlaceholder is the literal string baked into the committed
// .env. docs/phases/02-walking-skeleton.md §12 requires Load() to reject
// it so a deployer who forgets to substitute their own webhook.site URL
// fails fast at every binary's startup (including the migrate job),
// rather than silently posting deliveries into the void.
const webhookURLPlaceholder = "https://webhook.site/REPLACE-WITH-YOUR-UUID"

// Load reads the process environment and returns a populated *Config.
//
// Required variables: DATABASE_URL, REDIS_URL, KAFKA_BROKERS, WEBHOOK_URL.
// Missing or empty values produce an ErrMissingRequired-wrapped error.
// WEBHOOK_URL additionally rejects the committed-placeholder value
// (docs/phases/02-walking-skeleton.md §12). Optional variables fall back
// to documented defaults.
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
	webhookURL := required("WEBHOOK_URL")

	if len(missing) > 0 {
		return nil, fmt.Errorf("%w: %s", ErrMissingRequired, strings.Join(missing, ", "))
	}

	if webhookURL == webhookURLPlaceholder {
		return nil, fmt.Errorf("%w: WEBHOOK_URL is still the committed placeholder; replace REPLACE-WITH-YOUR-UUID with a real https://webhook.site/<uuid>", ErrInvalidValue)
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
		WebhookURL:   webhookURL,
		MetricsAddr:  stringDefault(os.Getenv("METRICS_ADDR"), ":9090"),
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
