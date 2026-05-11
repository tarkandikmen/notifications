package config

import (
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setEnv installs values for the duration of the test. t.Setenv handles
// cleanup; the empty-string convention exists so callers can unset a key
// the surrounding shell may have set.
func setEnv(t *testing.T, kv map[string]string) {
	t.Helper()
	for k, v := range kv {
		t.Setenv(k, v)
	}
}

func validEnv() map[string]string {
	return map[string]string{
		"DATABASE_URL":  "postgres://u:p@db:5432/notifications?sslmode=disable",
		"REDIS_URL":     "redis://redis:6379",
		"KAFKA_BROKERS": "kafka1:9092,kafka2:9092",
		"LOG_LEVEL":     "info",
		"HTTP_ADDR":     ":9090",
		"WEBHOOK_URL":   "https://webhook.site/abc",
	}
}

func TestLoad_HappyPath(t *testing.T) {
	setEnv(t, validEnv())
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")

	cfg, err := Load()
	require.NoError(t, err)
	require.NotNil(t, cfg)

	assert.Equal(t, ":9090", cfg.HTTPAddr)
	assert.Equal(t, "postgres://u:p@db:5432/notifications?sslmode=disable", cfg.DatabaseURL)
	assert.Equal(t, "redis://redis:6379", cfg.RedisURL)
	assert.Equal(t, []string{"kafka1:9092", "kafka2:9092"}, cfg.KafkaBrokers)
	assert.Equal(t, slog.LevelInfo, cfg.LogLevel)
	assert.Empty(t, cfg.OTelEndpoint)
	assert.Equal(t, "https://webhook.site/abc", cfg.WebhookURL)
}

func TestLoad_DefaultsWhenOptionalsUnset(t *testing.T) {
	setEnv(t, map[string]string{
		"DATABASE_URL":  "postgres://x",
		"REDIS_URL":     "redis://x",
		"KAFKA_BROKERS": "k:9092",
		"WEBHOOK_URL":   "https://webhook.site/abc",
	})
	t.Setenv("HTTP_ADDR", "")
	t.Setenv("LOG_LEVEL", "")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")

	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, ":8080", cfg.HTTPAddr)
	assert.Equal(t, slog.LevelInfo, cfg.LogLevel)
	assert.Equal(t, "https://webhook.site/abc", cfg.WebhookURL)
	assert.Empty(t, cfg.OTelEndpoint)
}

func TestLoad_MissingRequired(t *testing.T) {
	cases := []struct {
		name      string
		env       map[string]string
		wantMatch string
	}{
		{
			name: "missing DATABASE_URL",
			env: map[string]string{
				"DATABASE_URL":  "",
				"REDIS_URL":     "redis://x",
				"KAFKA_BROKERS": "k:9092",
				"WEBHOOK_URL":   "https://webhook.site/abc",
			},
			wantMatch: "DATABASE_URL",
		},
		{
			name: "missing REDIS_URL",
			env: map[string]string{
				"DATABASE_URL":  "postgres://x",
				"REDIS_URL":     "",
				"KAFKA_BROKERS": "k:9092",
				"WEBHOOK_URL":   "https://webhook.site/abc",
			},
			wantMatch: "REDIS_URL",
		},
		{
			name: "missing KAFKA_BROKERS",
			env: map[string]string{
				"DATABASE_URL":  "postgres://x",
				"REDIS_URL":     "redis://x",
				"KAFKA_BROKERS": "",
				"WEBHOOK_URL":   "https://webhook.site/abc",
			},
			wantMatch: "KAFKA_BROKERS",
		},
		{
			name: "missing WEBHOOK_URL",
			env: map[string]string{
				"DATABASE_URL":  "postgres://x",
				"REDIS_URL":     "redis://x",
				"KAFKA_BROKERS": "k:9092",
				"WEBHOOK_URL":   "",
			},
			wantMatch: "WEBHOOK_URL",
		},
		{
			name: "all required missing names every key",
			env: map[string]string{
				"DATABASE_URL":  "",
				"REDIS_URL":     "",
				"KAFKA_BROKERS": "",
				"WEBHOOK_URL":   "",
			},
			wantMatch: "DATABASE_URL",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setEnv(t, tc.env)

			cfg, err := Load()
			require.Error(t, err)
			assert.Nil(t, cfg)
			assert.True(t, errors.Is(err, ErrMissingRequired), "want ErrMissingRequired, got %v", err)
			assert.Contains(t, err.Error(), tc.wantMatch)
		})
	}
}

// TestLoad_RejectsWebhookPlaceholder enforces docs/phases/02-walking-skeleton.md
// §12: Load() must refuse the committed-placeholder URL so a deployer who
// forgets to swap in a real webhook.site UUID fails at startup rather than
// silently delivering nowhere.
func TestLoad_RejectsWebhookPlaceholder(t *testing.T) {
	setEnv(t, validEnv())
	t.Setenv("WEBHOOK_URL", "https://webhook.site/REPLACE-WITH-YOUR-UUID")

	cfg, err := Load()
	require.Error(t, err)
	assert.Nil(t, cfg)
	assert.True(t, errors.Is(err, ErrInvalidValue), "want ErrInvalidValue, got %v", err)
	assert.Contains(t, err.Error(), "WEBHOOK_URL")
}

func TestLoad_InvalidLogLevel(t *testing.T) {
	setEnv(t, validEnv())
	t.Setenv("LOG_LEVEL", "ludicrous")

	cfg, err := Load()
	require.Error(t, err)
	assert.Nil(t, cfg)
	assert.True(t, errors.Is(err, ErrInvalidValue), "want ErrInvalidValue, got %v", err)
}

func TestLoad_ParsesLogLevels(t *testing.T) {
	cases := map[string]slog.Level{
		"debug":   slog.LevelDebug,
		"info":    slog.LevelInfo,
		"warn":    slog.LevelWarn,
		"warning": slog.LevelWarn,
		"error":   slog.LevelError,
		"":        slog.LevelInfo,
	}

	for raw, want := range cases {
		t.Run(raw, func(t *testing.T) {
			setEnv(t, validEnv())
			t.Setenv("LOG_LEVEL", raw)
			cfg, err := Load()
			require.NoError(t, err)
			assert.Equal(t, want, cfg.LogLevel)
		})
	}
}

func TestLoad_KafkaBrokersOnlyCommas(t *testing.T) {
	setEnv(t, validEnv())
	t.Setenv("KAFKA_BROKERS", " , , ")

	cfg, err := Load()
	require.Error(t, err)
	assert.Nil(t, cfg)
	assert.True(t, errors.Is(err, ErrInvalidValue))
}
