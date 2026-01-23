package watcher

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/1995parham-learning/interface-ai-coding-challenge/internal/config"
)

func TestShouldIgnoreFile(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		expected bool
	}{
		// Hidden files
		{"hidden file", "/path/to/.hidden", true},
		{"hidden file in dir", "/path/to/.config/file", false},
		{"dotfile", ".gitignore", true},

		// Temp file patterns
		{"tmp suffix", "/path/to/file.tmp", true},
		{"part suffix", "/path/to/file.part", true},
		{"swp suffix", "/path/to/file.swp", true},
		{"crdownload suffix", "/path/to/file.crdownload", true},
		{"partial suffix", "/path/to/file.partial", true},
		{"download suffix", "/path/to/file.download", true},
		{"tilde suffix", "/path/to/file~", true},

		// Normal files
		{"normal txt", "/path/to/file.txt", false},
		{"normal csv", "/path/to/data.csv", false},
		{"normal json", "/path/to/config.json", false},
		{"no extension", "/path/to/filename", false},

		// Edge cases
		{"tmp in name not suffix", "/path/to/tmp_file.txt", false},
		{"part in name not suffix", "/path/to/partial_data.csv", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := shouldIgnoreFile(tt.path)
			if result != tt.expected {
				t.Errorf("shouldIgnoreFile(%q) = %v, want %v", tt.path, result, tt.expected)
			}
		})
	}
}

func TestNew_StabilityWindow(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := New(config.MethodStabilityWindow, tmpDir, 5)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() { _ = w.Close() }()

	if w.method != config.MethodStabilityWindow {
		t.Errorf("method = %q, want %q", w.method, config.MethodStabilityWindow)
	}
	if w.modification == nil {
		t.Error("modification map should not be nil for stability_window mode")
	}
	if w.completed != nil {
		t.Error("completed map should be nil for stability_window mode")
	}
	if w.stabilitySeconds != 5 {
		t.Errorf("stabilitySeconds = %d, want 5", w.stabilitySeconds)
	}
}

func TestNew_Sidecar(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := New(config.MethodSidecar, tmpDir, 5)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() { _ = w.Close() }()

	if w.method != config.MethodSidecar {
		t.Errorf("method = %q, want %q", w.method, config.MethodSidecar)
	}
	if w.modification != nil {
		t.Error("modification map should be nil for sidecar mode")
	}
	if w.completed == nil {
		t.Error("completed map should not be nil for sidecar mode")
	}
}

func TestNew_InvalidMethod(t *testing.T) {
	tmpDir := t.TempDir()

	_, err := New("invalid_method", tmpDir, 5)
	if err == nil {
		t.Error("expected error for invalid method, got nil")
	}
}

func TestWatcher_StabilityWindow_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	tmpDir := t.TempDir()

	// Use a short stability window for testing
	stabilitySeconds := 1

	w, err := New(config.MethodStabilityWindow, tmpDir, stabilitySeconds)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() { _ = w.Close() }()

	if err := w.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Create a file
	testFile := filepath.Join(tmpDir, "test.txt")
	if err := os.WriteFile(testFile, []byte("initial content"), 0o644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// Wait for fsnotify to detect the file
	time.Sleep(100 * time.Millisecond)

	// File should not be ready yet (stability window not passed)
	files := w.GetFilesToProcess()
	if len(files) != 0 {
		t.Errorf("expected 0 files before stability window, got %d", len(files))
	}

	// Wait for stability window to pass
	time.Sleep(time.Duration(stabilitySeconds+1) * time.Second)

	// File should now be ready
	files = w.GetFilesToProcess()
	if len(files) != 1 {
		t.Errorf("expected 1 file after stability window, got %d", len(files))
	}
}

func TestWatcher_Sidecar_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	tmpDir := t.TempDir()

	w, err := New(config.MethodSidecar, tmpDir, 5)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() { _ = w.Close() }()

	if err := w.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Create a data file
	testFile := filepath.Join(tmpDir, "data.csv")
	if err := os.WriteFile(testFile, []byte("col1,col2\na,b"), 0o644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// Wait for fsnotify
	time.Sleep(100 * time.Millisecond)

	// File should not be ready yet (no .ok sidecar)
	files := w.GetFilesToProcess()
	if len(files) != 0 {
		t.Errorf("expected 0 files without sidecar, got %d", len(files))
	}

	// Create the sidecar file
	sidecarFile := testFile + ".ok"
	if err := os.WriteFile(sidecarFile, []byte{}, 0o644); err != nil {
		t.Fatalf("failed to create sidecar file: %v", err)
	}

	// Wait for fsnotify
	time.Sleep(100 * time.Millisecond)

	// File should now be ready
	files = w.GetFilesToProcess()
	if len(files) != 1 {
		t.Errorf("expected 1 file with sidecar, got %d", len(files))
	}
	if len(files) == 1 && files[0] != testFile {
		t.Errorf("expected file %q, got %q", testFile, files[0])
	}
}

func TestWatcher_IgnoresHiddenFiles(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	tmpDir := t.TempDir()
	stabilitySeconds := 1

	w, err := New(config.MethodStabilityWindow, tmpDir, stabilitySeconds)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() { _ = w.Close() }()

	if err := w.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Create a hidden file
	hiddenFile := filepath.Join(tmpDir, ".hidden")
	if err := os.WriteFile(hiddenFile, []byte("hidden content"), 0o644); err != nil {
		t.Fatalf("failed to create hidden file: %v", err)
	}

	// Wait for stability window
	time.Sleep(time.Duration(stabilitySeconds+1) * time.Second)

	// Hidden file should not be in the list
	files := w.GetFilesToProcess()
	for _, f := range files {
		if filepath.Base(f) == ".hidden" {
			t.Error("hidden file should be ignored")
		}
	}
}

func TestWatcher_IgnoresTempFiles(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	tmpDir := t.TempDir()
	stabilitySeconds := 1

	w, err := New(config.MethodStabilityWindow, tmpDir, stabilitySeconds)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() { _ = w.Close() }()

	if err := w.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Create temp files with various patterns
	tempPatterns := []string{"file.tmp", "file.part", "file.swp"}
	for _, pattern := range tempPatterns {
		f := filepath.Join(tmpDir, pattern)
		if err := os.WriteFile(f, []byte("temp content"), 0o644); err != nil {
			t.Fatalf("failed to create temp file %s: %v", pattern, err)
		}
	}

	// Wait for stability window
	time.Sleep(time.Duration(stabilitySeconds+1) * time.Second)

	// Temp files should not be in the list
	files := w.GetFilesToProcess()
	for _, f := range files {
		base := filepath.Base(f)
		for _, pattern := range tempPatterns {
			if base == pattern {
				t.Errorf("temp file %s should be ignored", pattern)
			}
		}
	}
}

func TestRemoveFromTracking(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := New(config.MethodStabilityWindow, tmpDir, 1)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() { _ = w.Close() }()

	// Manually add a file to tracking
	testPath := filepath.Join(tmpDir, "test.txt")
	w.modification.Store(testPath, time.Now().Add(-2*time.Second))

	// Verify it's tracked
	files := w.GetFilesToProcess()
	if len(files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(files))
	}

	// Remove from tracking
	w.RemoveFromTracking(testPath)

	// Verify it's removed
	files = w.GetFilesToProcess()
	if len(files) != 0 {
		t.Errorf("expected 0 files after removal, got %d", len(files))
	}
}

func TestRemoveFromTracking_Sidecar(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := New(config.MethodSidecar, tmpDir, 1)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() { _ = w.Close() }()

	// Manually add a file to completed tracking
	testPath := filepath.Join(tmpDir, "test.txt")
	w.completed.Store(testPath, true)

	// Verify it's tracked
	files := w.GetFilesToProcess()
	if len(files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(files))
	}

	// Remove from tracking
	w.RemoveFromTracking(testPath)

	// Verify it's removed
	files = w.GetFilesToProcess()
	if len(files) != 0 {
		t.Errorf("expected 0 files after removal, got %d", len(files))
	}
}

func TestClose(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := New(config.MethodStabilityWindow, tmpDir, 5)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	if err := w.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Close should not error
	if err := w.Close(); err != nil {
		t.Errorf("Close failed: %v", err)
	}
}
