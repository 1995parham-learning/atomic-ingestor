package processor

import (
	"log/slog"
	"os"
	"path/filepath"

	"github.com/1995parham-learning/interface-ai-coding-challenge/internal/config"
	"github.com/1995parham-learning/interface-ai-coding-challenge/internal/fileops"
	"github.com/1995parham-learning/interface-ai-coding-challenge/internal/storage"
	"github.com/1995parham-learning/interface-ai-coding-challenge/internal/watcher"
)

type Processor struct {
	cfg     *config.Config
	storage *storage.Storage
	watcher *watcher.Watcher
}

func New(cfg *config.Config, storage *storage.Storage, watcher *watcher.Watcher) *Processor {
	return &Processor{
		cfg:     cfg,
		storage: storage,
		watcher: watcher,
	}
}

func (p *Processor) ProcessFiles() {
	files := p.watcher.GetFilesToProcess()

	if len(files) > 0 {
		slog.Info("files ready to process", "count", len(files), "files", files)
	}

	for _, f := range files {
		if err := p.processFile(f); err != nil {
			slog.Error("failed to process file", "path", f, "error", err)
		}
	}
}

func (p *Processor) processFile(filePath string) error {
	// Get file info and calculate SHA256
	info, err := os.Stat(filePath)
	if err != nil {
		slog.Warn("failed to stat file", "path", filePath, "error", err)
		p.watcher.RemoveFromTracking(filePath)
		return err
	}

	hash, err := fileops.CalculateSHA256(filePath)
	if err != nil {
		slog.Warn("failed to calculate SHA256", "path", filePath, "error", err)
		p.watcher.RemoveFromTracking(filePath)
		return err
	}

	// Check if file with same SHA256 was already processed
	exists, err := p.storage.FileExists(hash)
	if err != nil {
		slog.Error("failed to check file existence", "path", filePath, "error", err)
		return err
	}

	if exists {
		slog.Info("file already processed, skipping", "path", filePath, "sha256", hash)
		p.watcher.RemoveFromTracking(filePath)
		return nil
	}

	// Process the file in a transaction
	err = p.storage.Transaction(func(txStorage *storage.Storage) error {
		// Create database record
		if err := txStorage.CreateFile(hash, info.Name(), filePath, info.Size()); err != nil {
			return err
		}

		// Calculate relative path and join with destination
		relPath, err := filepath.Rel(p.cfg.Path, filePath)
		if err != nil {
			return err
		}
		dstPath := filepath.Join(p.cfg.Destination, relPath)

		// Ensure destination directory exists
		dstDir := filepath.Dir(dstPath)
		if err := os.MkdirAll(dstDir, 0o755); err != nil {
			return err
		}

		// Copy the file
		if err := fileops.CopyFile(filePath, dstPath); err != nil {
			return err
		}

		p.watcher.RemoveFromTracking(filePath)

		slog.Info("file processed successfully", "path", filePath, "sha256", hash, "destination", dstPath)
		return nil
	})

	return err
}
