package fileops

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCalculateSHA256(t *testing.T) {
	// Create a temporary file with known content
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.txt")

	content := []byte("hello world")
	if err := os.WriteFile(testFile, content, 0o644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// Known SHA256 hash for "hello world"
	expectedHash := "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"

	hash, err := CalculateSHA256(testFile)
	if err != nil {
		t.Fatalf("CalculateSHA256 failed: %v", err)
	}

	if hash != expectedHash {
		t.Errorf("hash mismatch: got %s, want %s", hash, expectedHash)
	}
}

func TestCalculateSHA256_EmptyFile(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "empty.txt")

	if err := os.WriteFile(testFile, []byte{}, 0o644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// Known SHA256 hash for empty content
	expectedHash := "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

	hash, err := CalculateSHA256(testFile)
	if err != nil {
		t.Fatalf("CalculateSHA256 failed: %v", err)
	}

	if hash != expectedHash {
		t.Errorf("hash mismatch: got %s, want %s", hash, expectedHash)
	}
}

func TestCalculateSHA256_NonExistentFile(t *testing.T) {
	_, err := CalculateSHA256("/nonexistent/file.txt")
	if err == nil {
		t.Error("expected error for non-existent file, got nil")
	}
}

func TestCopyFile(t *testing.T) {
	tmpDir := t.TempDir()
	srcFile := filepath.Join(tmpDir, "source.txt")
	dstFile := filepath.Join(tmpDir, "dest.txt")

	content := []byte("test content for copy")
	if err := os.WriteFile(srcFile, content, 0o644); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	if err := CopyFile(srcFile, dstFile); err != nil {
		t.Fatalf("CopyFile failed: %v", err)
	}

	// Verify destination file exists and has correct content
	dstContent, err := os.ReadFile(dstFile)
	if err != nil {
		t.Fatalf("failed to read destination file: %v", err)
	}

	if string(dstContent) != string(content) {
		t.Errorf("content mismatch: got %q, want %q", dstContent, content)
	}
}

func TestCopyFile_SameFile(t *testing.T) {
	tmpDir := t.TempDir()
	srcFile := filepath.Join(tmpDir, "source.txt")

	content := []byte("test content")
	if err := os.WriteFile(srcFile, content, 0o644); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	// Create a hard link to simulate same file
	linkFile := filepath.Join(tmpDir, "link.txt")
	if err := os.Link(srcFile, linkFile); err != nil {
		t.Skipf("hard links not supported: %v", err)
	}

	// CopyFile should return nil for same file
	if err := CopyFile(srcFile, linkFile); err != nil {
		t.Errorf("CopyFile failed for same file: %v", err)
	}
}

func TestCopyFile_NonRegularSource(t *testing.T) {
	tmpDir := t.TempDir()
	subDir := filepath.Join(tmpDir, "subdir")

	if err := os.Mkdir(subDir, 0o755); err != nil {
		t.Fatalf("failed to create subdir: %v", err)
	}

	dstFile := filepath.Join(tmpDir, "dest.txt")

	err := CopyFile(subDir, dstFile)
	if err == nil {
		t.Error("expected error when copying directory, got nil")
	}
}

func TestCopyFile_NonExistentSource(t *testing.T) {
	tmpDir := t.TempDir()
	err := CopyFile("/nonexistent/source.txt", filepath.Join(tmpDir, "dest.txt"))
	if err == nil {
		t.Error("expected error for non-existent source, got nil")
	}
}

func TestMoveFile(t *testing.T) {
	tmpDir := t.TempDir()
	srcFile := filepath.Join(tmpDir, "source.txt")
	dstFile := filepath.Join(tmpDir, "dest.txt")

	content := []byte("test content for move")
	if err := os.WriteFile(srcFile, content, 0o644); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	if err := MoveFile(srcFile, dstFile); err != nil {
		t.Fatalf("MoveFile failed: %v", err)
	}

	// Verify source no longer exists
	if _, err := os.Stat(srcFile); !os.IsNotExist(err) {
		t.Error("source file should not exist after move")
	}

	// Verify destination exists with correct content
	dstContent, err := os.ReadFile(dstFile)
	if err != nil {
		t.Fatalf("failed to read destination file: %v", err)
	}

	if string(dstContent) != string(content) {
		t.Errorf("content mismatch: got %q, want %q", dstContent, content)
	}
}

func TestMoveFile_CrossDirectory(t *testing.T) {
	tmpDir := t.TempDir()
	srcDir := filepath.Join(tmpDir, "src")
	dstDir := filepath.Join(tmpDir, "dst")

	if err := os.MkdirAll(srcDir, 0o755); err != nil {
		t.Fatalf("failed to create src dir: %v", err)
	}
	if err := os.MkdirAll(dstDir, 0o755); err != nil {
		t.Fatalf("failed to create dst dir: %v", err)
	}

	srcFile := filepath.Join(srcDir, "file.txt")
	dstFile := filepath.Join(dstDir, "file.txt")

	content := []byte("cross directory move test")
	if err := os.WriteFile(srcFile, content, 0o644); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	if err := MoveFile(srcFile, dstFile); err != nil {
		t.Fatalf("MoveFile failed: %v", err)
	}

	// Verify move was successful
	if _, err := os.Stat(srcFile); !os.IsNotExist(err) {
		t.Error("source file should not exist after move")
	}

	dstContent, err := os.ReadFile(dstFile)
	if err != nil {
		t.Fatalf("failed to read destination file: %v", err)
	}

	if string(dstContent) != string(content) {
		t.Errorf("content mismatch: got %q, want %q", dstContent, content)
	}
}

func TestMoveFile_NonExistentSource(t *testing.T) {
	tmpDir := t.TempDir()
	err := MoveFile("/nonexistent/source.txt", filepath.Join(tmpDir, "dest.txt"))
	if err == nil {
		t.Error("expected error for non-existent source, got nil")
	}
}

func TestMoveFile_NonRegularSource(t *testing.T) {
	tmpDir := t.TempDir()
	subDir := filepath.Join(tmpDir, "subdir")

	if err := os.Mkdir(subDir, 0o755); err != nil {
		t.Fatalf("failed to create subdir: %v", err)
	}

	dstFile := filepath.Join(tmpDir, "dest.txt")

	err := MoveFile(subDir, dstFile)
	if err == nil {
		t.Error("expected error when moving directory, got nil")
	}
}
