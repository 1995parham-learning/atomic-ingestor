package manifest

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNewWriter(t *testing.T) {
	tmpDir := t.TempDir()

	w := NewWriter(tmpDir)
	if w == nil {
		t.Fatal("NewWriter returned nil")
	}
	if w.basePath != tmpDir {
		t.Errorf("basePath = %q, want %q", w.basePath, tmpDir)
	}
}

func TestWriter_getManifestPath(t *testing.T) {
	w := NewWriter("/manifests")

	// Fixed timestamp for testing
	ts := time.Date(2024, 3, 15, 14, 30, 0, 0, time.UTC)

	expected := "/manifests/2024/03/15/14/manifest.jsonl"
	result := w.getManifestPath(ts)

	if result != expected {
		t.Errorf("getManifestPath() = %q, want %q", result, expected)
	}
}

func TestWriter_getManifestPath_Various(t *testing.T) {
	w := NewWriter("/base")

	tests := []struct {
		name     string
		time     time.Time
		expected string
	}{
		{
			name:     "start of year",
			time:     time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			expected: "/base/2024/01/01/00/manifest.jsonl",
		},
		{
			name:     "end of year",
			time:     time.Date(2024, 12, 31, 23, 59, 0, 0, time.UTC),
			expected: "/base/2024/12/31/23/manifest.jsonl",
		},
		{
			name:     "middle of day",
			time:     time.Date(2024, 6, 15, 12, 30, 0, 0, time.UTC),
			expected: "/base/2024/06/15/12/manifest.jsonl",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := w.getManifestPath(tt.time)
			if result != tt.expected {
				t.Errorf("getManifestPath() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestWriter_Append(t *testing.T) {
	tmpDir := t.TempDir()
	w := NewWriter(tmpDir)

	entry := Entry{
		SHA256:      "abc123def456",
		Name:        "test.csv",
		SourcePath:  "/input/test.csv",
		DestPath:    "/warehouse/test.csv",
		Size:        1024,
		ProcessedAt: time.Date(2024, 3, 15, 14, 30, 0, 0, time.UTC),
	}

	if err := w.Append(entry); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Verify file was created
	manifestPath := w.getManifestPath(entry.ProcessedAt)
	if _, err := os.Stat(manifestPath); os.IsNotExist(err) {
		t.Fatal("manifest file was not created")
	}

	// Read and verify content
	file, err := os.Open(manifestPath)
	if err != nil {
		t.Fatalf("failed to open manifest file: %v", err)
	}
	defer func() { _ = file.Close() }()

	scanner := bufio.NewScanner(file)
	if !scanner.Scan() {
		t.Fatal("manifest file is empty")
	}

	var readEntry Entry
	if err := json.Unmarshal(scanner.Bytes(), &readEntry); err != nil {
		t.Fatalf("failed to unmarshal entry: %v", err)
	}

	if readEntry.SHA256 != entry.SHA256 {
		t.Errorf("SHA256 = %q, want %q", readEntry.SHA256, entry.SHA256)
	}
	if readEntry.Name != entry.Name {
		t.Errorf("Name = %q, want %q", readEntry.Name, entry.Name)
	}
	if readEntry.SourcePath != entry.SourcePath {
		t.Errorf("SourcePath = %q, want %q", readEntry.SourcePath, entry.SourcePath)
	}
	if readEntry.DestPath != entry.DestPath {
		t.Errorf("DestPath = %q, want %q", readEntry.DestPath, entry.DestPath)
	}
	if readEntry.Size != entry.Size {
		t.Errorf("Size = %d, want %d", readEntry.Size, entry.Size)
	}
}

func TestWriter_Append_MultipleEntries(t *testing.T) {
	tmpDir := t.TempDir()
	w := NewWriter(tmpDir)

	// Same hour for all entries
	baseTime := time.Date(2024, 3, 15, 14, 0, 0, 0, time.UTC)

	entries := []Entry{
		{SHA256: "hash1", Name: "file1.csv", SourcePath: "/in/1", DestPath: "/out/1", Size: 100, ProcessedAt: baseTime},
		{SHA256: "hash2", Name: "file2.csv", SourcePath: "/in/2", DestPath: "/out/2", Size: 200, ProcessedAt: baseTime.Add(1 * time.Minute)},
		{SHA256: "hash3", Name: "file3.csv", SourcePath: "/in/3", DestPath: "/out/3", Size: 300, ProcessedAt: baseTime.Add(2 * time.Minute)},
	}

	for _, entry := range entries {
		if err := w.Append(entry); err != nil {
			t.Fatalf("Append failed for %s: %v", entry.Name, err)
		}
	}

	// Verify all entries are in the same file
	manifestPath := w.getManifestPath(baseTime)
	file, err := os.Open(manifestPath)
	if err != nil {
		t.Fatalf("failed to open manifest file: %v", err)
	}
	defer func() { _ = file.Close() }()

	scanner := bufio.NewScanner(file)
	count := 0
	for scanner.Scan() {
		count++
	}

	if count != len(entries) {
		t.Errorf("expected %d entries, got %d", len(entries), count)
	}
}

func TestWriter_Append_DifferentHours(t *testing.T) {
	tmpDir := t.TempDir()
	w := NewWriter(tmpDir)

	entry1 := Entry{
		SHA256:      "hash1",
		Name:        "file1.csv",
		ProcessedAt: time.Date(2024, 3, 15, 14, 0, 0, 0, time.UTC),
	}
	entry2 := Entry{
		SHA256:      "hash2",
		Name:        "file2.csv",
		ProcessedAt: time.Date(2024, 3, 15, 15, 0, 0, 0, time.UTC), // Different hour
	}

	if err := w.Append(entry1); err != nil {
		t.Fatalf("Append entry1 failed: %v", err)
	}
	if err := w.Append(entry2); err != nil {
		t.Fatalf("Append entry2 failed: %v", err)
	}

	// Verify two different files were created
	path1 := w.getManifestPath(entry1.ProcessedAt)
	path2 := w.getManifestPath(entry2.ProcessedAt)

	if path1 == path2 {
		t.Error("expected different paths for different hours")
	}

	if _, err := os.Stat(path1); os.IsNotExist(err) {
		t.Error("manifest file 1 was not created")
	}
	if _, err := os.Stat(path2); os.IsNotExist(err) {
		t.Error("manifest file 2 was not created")
	}
}

func TestWriter_Append_CreatesDirectories(t *testing.T) {
	tmpDir := t.TempDir()
	w := NewWriter(tmpDir)

	entry := Entry{
		SHA256:      "abc123",
		Name:        "test.csv",
		ProcessedAt: time.Date(2024, 3, 15, 14, 30, 0, 0, time.UTC),
	}

	if err := w.Append(entry); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Verify directory structure was created
	expectedDir := filepath.Join(tmpDir, "2024", "03", "15", "14")
	if _, err := os.Stat(expectedDir); os.IsNotExist(err) {
		t.Errorf("expected directory %q was not created", expectedDir)
	}
}

func TestEntry_JSONFormat(t *testing.T) {
	entry := Entry{
		SHA256:      "abc123",
		Name:        "test.csv",
		SourcePath:  "/source/test.csv",
		DestPath:    "/dest/test.csv",
		Size:        1024,
		ProcessedAt: time.Date(2024, 3, 15, 14, 30, 45, 0, time.UTC),
	}

	data, err := json.Marshal(entry)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}

	// Verify JSON contains expected fields
	jsonStr := string(data)
	expectedFields := []string{
		`"sha256":"abc123"`,
		`"name":"test.csv"`,
		`"source_path":"/source/test.csv"`,
		`"dest_path":"/dest/test.csv"`,
		`"size":1024`,
	}

	for _, field := range expectedFields {
		if !contains(jsonStr, field) {
			t.Errorf("JSON missing field %s: %s", field, jsonStr)
		}
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsAt(s, substr, 0))
}

func containsAt(s, substr string, start int) bool {
	for i := start; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
