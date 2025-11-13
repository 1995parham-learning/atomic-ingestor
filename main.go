package main

import (
	"flag"
	"log/slog"
	"os"
	"time"

	"github.com/1995parham-learning/interface-ai-coding-challenge/internal/config"
	"github.com/1995parham-learning/interface-ai-coding-challenge/internal/processor"
	"github.com/1995parham-learning/interface-ai-coding-challenge/internal/storage"
	"github.com/1995parham-learning/interface-ai-coding-challenge/internal/watcher"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func main() {
	// Parse command-line flags
	cfg := &config.Config{}

	flag.StringVar(&cfg.Path, "input", config.DefaultInputPath, "Input directory to monitor")
	flag.StringVar(&cfg.Destination, "warehouse", config.DefaultWarehousePath, "Warehouse directory for ingested files")
	flag.StringVar(&cfg.ManifestsPath, "manifests", config.DefaultManifestsPath, "Manifests directory")
	flag.StringVar(&cfg.Method, "mode", config.DefaultMethod, "Completion detection mode (stability_window or sidecar)")
	flag.IntVar(&cfg.StabilitySeconds, "stability-seconds", config.DefaultStabilitySeconds, "Stability window duration in seconds")
	flag.StringVar(&cfg.StatePath, "state-path", config.DefaultStatePath, "Path to state database file")
	flag.StringVar(&cfg.LogLevel, "log-level", config.DefaultLogLevel, "Log level (debug, info, warn, error)")
	flag.IntVar(&cfg.Concurrency, "concurrency", config.DefaultConcurrency, "Number of concurrent workers")
	flag.BoolVar(&cfg.DryRun, "dry-run", false, "Dry run mode (do not actually move files)")

	flag.Parse()

	// Parse log level
	var logLevel slog.Level
	switch cfg.LogLevel {
	case "debug":
		logLevel = slog.LevelDebug
	case "info":
		logLevel = slog.LevelInfo
	case "warn":
		logLevel = slog.LevelWarn
	case "error":
		logLevel = slog.LevelError
	default:
		slog.Error("invalid log level", "level", cfg.LogLevel)
		os.Exit(1)
	}

	// Initialize structured logger
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel,
	}))
	slog.SetDefault(logger)

	slog.Info("starting atomic ingestor",
		"input", cfg.Path,
		"warehouse", cfg.Destination,
		"manifests", cfg.ManifestsPath,
		"mode", cfg.Method,
		"stability_seconds", cfg.StabilitySeconds,
		"state_path", cfg.StatePath,
		"log_level", cfg.LogLevel,
		"concurrency", cfg.Concurrency,
		"dry_run", cfg.DryRun,
	)

	// Validate configuration
	if cfg.Method != config.MethodStabilityWindow && cfg.Method != config.MethodSidecar {
		slog.Error("invalid method name", "method", cfg.Method)
		os.Exit(1)
	}

	// Initialize database
	db, err := gorm.Open(sqlite.Open(cfg.StatePath), &gorm.Config{})
	if err != nil {
		slog.Error("failed to open database", "error", err)
		os.Exit(1)
	}

	store := storage.New(db)
	if err := store.AutoMigrate(); err != nil {
		slog.Error("failed to migrate database", "error", err)
		os.Exit(1)
	}

	// Initialize file watcher
	w, err := watcher.New(cfg.Method, cfg.Path)
	if err != nil {
		slog.Error("failed to create watcher", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := w.Close(); err != nil {
			slog.Error("failed to close watcher", "error", err)
		}
	}()

	if err := w.Start(); err != nil {
		slog.Error("failed to start watcher", "path", cfg.Path, "error", err)
		os.Exit(1)
	}

	// Initialize processor
	proc := processor.New(cfg, store, w)

	// Process files periodically
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		slog.Debug("checking for files to process")
		proc.ProcessFiles()
	}
}
