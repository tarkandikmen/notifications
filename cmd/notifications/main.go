// Command notifications is the single binary that powers every run mode of
// the system: api, dispatcher, worker, relay, reaper, and the migrate
// helper. See docs/ARCHITECTURE.md §3 for the rationale.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/joho/godotenv"
	"github.com/spf13/cobra"

	"github.com/tarkandikmen/notifications/internal/api"
	"github.com/tarkandikmen/notifications/internal/config"
	"github.com/tarkandikmen/notifications/internal/dispatcher"
	"github.com/tarkandikmen/notifications/internal/migrate"
	"github.com/tarkandikmen/notifications/internal/reaper"
	"github.com/tarkandikmen/notifications/internal/relay"
	"github.com/tarkandikmen/notifications/internal/worker"
)

const defaultMigrationsSourceURL = "file://migrations"

func main() {
	// godotenv.Load() is best-effort; a missing .env is not an error. Pre-existing
	// env vars are not overwritten.
	_ = godotenv.Load()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	if err := newRootCmd().ExecuteContext(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func newRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:           "notifications",
		Short:         "Event-driven notification system",
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	root.AddCommand(
		newAPICmd(),
		newDispatcherCmd(),
		newWorkerCmd(),
		newRelayCmd(),
		newReaperCmd(),
		newMigrateCmd(),
		newKafkaBootstrapCmd(),
	)
	return root
}

func newAPICmd() *cobra.Command {
	return &cobra.Command{
		Use:   "api",
		Short: "Serve the HTTP API",
		RunE:  api.Run,
	}
}

func newDispatcherCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "dispatcher",
		Short: "Claim eligible notifications and queue them for delivery",
		RunE:  dispatcher.Run,
	}
}

func newWorkerCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "worker",
		Short: "Consume send.<channel> and call the provider",
		RunE:  worker.Run,
	}
	cmd.Flags().String(worker.ChannelFlag, "", "delivery channel (sms|email|push) (required)")
	_ = cmd.MarkFlagRequired(worker.ChannelFlag)
	return cmd
}

func newRelayCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "relay",
		Short: "Drain the outbox table to Kafka",
		RunE:  relay.Run,
	}
}

func newReaperCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "reaper",
		Short: "Recover stuck DISPATCHED rows",
		RunE:  reaper.Run,
	}
}

// newKafkaBootstrapCmd is the one-shot topic-creation subcommand used
// by docker-compose's kafka-bootstrap service. It runs
// relay.Bootstrap (idempotent) and exits; downstream services
// (dispatcher, workers, reaper, relay) gate on
// service_completed_successfully so they never query Kafka admin for
// topics that haven't been created yet.
func newKafkaBootstrapCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "kafka-bootstrap",
		Short: "Create the Kafka topic set and exit (one-shot)",
		RunE:  relay.RunBootstrap,
	}
}

// newMigrateCmd wires `migrate up` and `migrate down` inline rather than
// behind another package.
func newMigrateCmd() *cobra.Command {
	migrateCmd := &cobra.Command{
		Use:   "migrate",
		Short: "Apply or revert database migrations",
	}

	up := &cobra.Command{
		Use:   "up",
		Short: "Apply every pending migration",
		RunE: func(_ *cobra.Command, _ []string) error {
			cfg, err := config.Load()
			if err != nil {
				return fmt.Errorf("migrate up: load config: %w", err)
			}
			return migrate.Up(cfg.DatabaseURL, defaultMigrationsSourceURL)
		},
	}

	down := &cobra.Command{
		Use:   "down",
		Short: "Revert every applied migration",
		RunE: func(_ *cobra.Command, _ []string) error {
			cfg, err := config.Load()
			if err != nil {
				return fmt.Errorf("migrate down: load config: %w", err)
			}
			return migrate.Down(cfg.DatabaseURL, defaultMigrationsSourceURL)
		},
	}

	migrateCmd.AddCommand(up, down)
	return migrateCmd
}
