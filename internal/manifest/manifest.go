package manifest

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Entry represents a single manifest record
type Entry struct {
	SHA256      string    `json:"sha256"`
	Name        string    `json:"name"`
	SourcePath  string    `json:"source_path"`
	DestPath    string    `json:"dest_path"`
	Size        int64     `json:"size"`
	ProcessedAt time.Time `json:"processed_at"`
}

// Writer handles writing manifest entries to JSON Lines files
type Writer struct {
	basePath string
}

// NewWriter creates a new manifest writer
func NewWriter(basePath string) *Writer {
	return &Writer{basePath: basePath}
}

// Append adds an entry to the appropriate manifest file based on timestamp
func (w *Writer) Append(entry Entry) error {
	// Determine manifest file path based on timestamp
	manifestPath := w.getManifestPath(entry.ProcessedAt)

	// Ensure directory exists
	dir := filepath.Dir(manifestPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("failed to create manifest directory: %w", err)
	}

	// Open file in append mode, create if doesn't exist
	file, err := os.OpenFile(manifestPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("failed to open manifest file: %w", err)
	}
	defer func() {
		_ = file.Close()
	}()

	// Encode entry as JSON line
	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("failed to marshal manifest entry: %w", err)
	}

	// Write JSON line with newline
	if _, err := file.Write(append(data, '\n')); err != nil {
		return fmt.Errorf("failed to write manifest entry: %w", err)
	}

	// Sync to ensure durability
	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync manifest file: %w", err)
	}

	return nil
}

// getManifestPath returns the path for the manifest file based on timestamp
// Format: basePath/YYYY/MM/DD/HH/manifest.jsonl
func (w *Writer) getManifestPath(t time.Time) string {
	return filepath.Join(
		w.basePath,
		t.Format("2006"),
		t.Format("01"),
		t.Format("02"),
		t.Format("15"),
		"manifest.jsonl",
	)
}
