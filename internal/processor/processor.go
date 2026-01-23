package processor

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/1995parham-learning/interface-ai-coding-challenge/internal/config"
	"github.com/1995parham-learning/interface-ai-coding-challenge/internal/fileops"
	"github.com/1995parham-learning/interface-ai-coding-challenge/internal/manifest"
	"github.com/1995parham-learning/interface-ai-coding-challenge/internal/storage"
	"github.com/1995parham-learning/interface-ai-coding-challenge/internal/watcher"
)

type Processor struct {
	cfg      *config.Config
	storage  *storage.Storage
	watcher  *watcher.Watcher
	manifest *manifest.Writer
}

func New(cfg *config.Config, storage *storage.Storage, watcher *watcher.Watcher) *Processor {
	return &Processor{
		cfg:      cfg,
		storage:  storage,
		watcher:  watcher,
		manifest: manifest.NewWriter(cfg.ManifestsPath),
	}
}

func (p *Processor) ProcessFiles() {
	files := p.watcher.GetFilesToProcess()

	if len(files) == 0 {
		return
	}

	slog.Info("files ready to process", "count", len(files), "files", files)

	// Use worker pool for concurrent processing
	concurrency := p.cfg.Concurrency
	if concurrency < 1 {
		concurrency = 1
	}
	if concurrency > len(files) {
		concurrency = len(files)
	}

	var wg sync.WaitGroup
	fileChan := make(chan string, len(files))

	// Start workers
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for f := range fileChan {
				slog.Debug("worker processing file", "worker", workerID, "path", f)
				if err := p.processFile(f); err != nil {
					slog.Error("failed to process file", "worker", workerID, "path", f, "error", err)
				}
			}
		}(i)
	}

	// Send files to workers
	for _, f := range files {
		fileChan <- f
	}
	close(fileChan)

	// Wait for all workers to complete
	wg.Wait()
}

func (p *Processor) processFile(filePath string) error {
	// Get file info and calculate SHA256
	info, err := os.Stat(filePath)
	if err != nil {
		slog.Warn("failed to stat file", "path", filePath, "error", err)
		p.watcher.RemoveFromTracking(filePath)
		return fmt.Errorf("stat file %s: %w", filePath, err)
	}

	hash, err := fileops.CalculateSHA256(filePath)
	if err != nil {
		slog.Warn("failed to calculate SHA256", "path", filePath, "error", err)
		p.watcher.RemoveFromTracking(filePath)
		return fmt.Errorf("calculate SHA256 for %s: %w", filePath, err)
	}

	// Check if file with same SHA256 was already processed
	exists, err := p.storage.FileExists(hash)
	if err != nil {
		slog.Error("failed to check file existence", "path", filePath, "error", err)
		return fmt.Errorf("check file existence for %s: %w", filePath, err)
	}

	if exists {
		slog.Info("file already processed, skipping", "path", filePath, "sha256", hash)
		p.watcher.RemoveFromTracking(filePath)
		return nil
	}

	// Calculate destination path
	relPath, err := filepath.Rel(p.cfg.Path, filePath)
	if err != nil {
		return fmt.Errorf("calculate relative path for %s: %w", filePath, err)
	}
	dstPath := filepath.Join(p.cfg.Destination, relPath)

	// Dry run mode - log what would happen but don't make changes
	if p.cfg.DryRun {
		slog.Info("dry run: would process file",
			"path", filePath,
			"sha256", hash,
			"destination", dstPath,
			"size", info.Size(),
		)
		p.watcher.RemoveFromTracking(filePath)
		return nil
	}

	// Process the file in a transaction
	processedAt := time.Now()
	err = p.storage.Transaction(func(txStorage *storage.Storage) error {
		// Create database record
		if err := txStorage.CreateFile(hash, info.Name(), filePath, info.Size()); err != nil {
			return fmt.Errorf("create database record: %w", err)
		}

		// Ensure destination directory exists
		dstDir := filepath.Dir(dstPath)
		if err := os.MkdirAll(dstDir, 0o755); err != nil {
			return fmt.Errorf("create destination directory %s: %w", dstDir, err)
		}

		// Move the file atomically (rename if same filesystem, copy+delete otherwise)
		if err := fileops.MoveFile(filePath, dstPath); err != nil {
			return fmt.Errorf("move file to %s: %w", dstPath, err)
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("process file %s: %w", filePath, err)
	}

	// Write manifest entry (outside transaction - best effort)
	manifestEntry := manifest.Entry{
		SHA256:      hash,
		Name:        info.Name(),
		SourcePath:  filePath,
		DestPath:    dstPath,
		Size:        info.Size(),
		ProcessedAt: processedAt,
	}
	if err := p.manifest.Append(manifestEntry); err != nil {
		slog.Warn("failed to write manifest entry", "path", filePath, "error", err)
		// Don't fail the operation for manifest errors
	}

	p.watcher.RemoveFromTracking(filePath)

	slog.Info("file processed successfully",
		"path", filePath,
		"sha256", hash,
		"destination", dstPath,
		"size", info.Size(),
	)
	return nil
}
