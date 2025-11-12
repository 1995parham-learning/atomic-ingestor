package main

import (
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
	// Initialize structured logger
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	cfg := &config.Config{
		Path:        "files",
		Method:      config.MethodSidecar,
		Destination: "dest",
	}

	// Validate configuration
	if cfg.Method != config.MethodStabilityWindow && cfg.Method != config.MethodSidecar {
		slog.Error("invalid method name", "method", cfg.Method)
		os.Exit(1)
	}

	// Initialize database
	db, err := gorm.Open(sqlite.Open("gorm.db"), &gorm.Config{})
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
	defer w.Close()

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
