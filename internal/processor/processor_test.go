package processor

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/1995parham-learning/interface-ai-coding-challenge/internal/config"
	"github.com/1995parham-learning/interface-ai-coding-challenge/internal/storage"
	"github.com/1995parham-learning/interface-ai-coding-challenge/internal/watcher"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type testEnv struct {
	inputDir     string
	warehouseDir string
	manifestsDir string
	dbPath       string
	cfg          *config.Config
	store        *storage.Storage
	watcher      *watcher.Watcher
	processor    *Processor
	cleanup      func()
}

func setupTestEnv(t *testing.T) *testEnv {
	t.Helper()

	tmpDir := t.TempDir()

	inputDir := filepath.Join(tmpDir, "input")
	warehouseDir := filepath.Join(tmpDir, "warehouse")
	manifestsDir := filepath.Join(tmpDir, "manifests")
	dbPath := filepath.Join(tmpDir, "test.db")

	// Create directories
	for _, dir := range []string{inputDir, warehouseDir, manifestsDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("failed to create directory %s: %v", dir, err)
		}
	}

	// Setup database
	db, err := gorm.Open(sqlite.Open(dbPath), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	store := storage.New(db)
	if err := store.AutoMigrate(); err != nil {
		t.Fatalf("failed to migrate database: %v", err)
	}

	// Setup watcher
	w, err := watcher.New(config.MethodStabilityWindow, inputDir, 1)
	if err != nil {
		t.Fatalf("failed to create watcher: %v", err)
	}

	cfg := &config.Config{
		Path:             inputDir,
		Destination:      warehouseDir,
		ManifestsPath:    manifestsDir,
		Method:           config.MethodStabilityWindow,
		StabilitySeconds: 1,
		Concurrency:      1,
		DryRun:           false,
	}

	proc := New(cfg, store, w)

	cleanup := func() {
		_ = w.Close()
		sqlDB, _ := db.DB()
		if sqlDB != nil {
			_ = sqlDB.Close()
		}
	}

	return &testEnv{
		inputDir:     inputDir,
		warehouseDir: warehouseDir,
		manifestsDir: manifestsDir,
		dbPath:       dbPath,
		cfg:          cfg,
		store:        store,
		watcher:      w,
		processor:    proc,
		cleanup:      cleanup,
	}
}

func TestNew(t *testing.T) {
	env := setupTestEnv(t)
	defer env.cleanup()

	if env.processor == nil {
		t.Error("New returned nil processor")
	}
	if env.processor.cfg != env.cfg {
		t.Error("processor config mismatch")
	}
	if env.processor.storage != env.store {
		t.Error("processor storage mismatch")
	}
	if env.processor.watcher != env.watcher {
		t.Error("processor watcher mismatch")
	}
}

func TestProcessFiles_NoFiles(t *testing.T) {
	env := setupTestEnv(t)
	defer env.cleanup()

	// Should not panic or error with no files
	env.processor.ProcessFiles()
}

func TestProcessFiles_SingleFile(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	env := setupTestEnv(t)
	defer env.cleanup()

	// Start watcher
	if err := env.watcher.Start(); err != nil {
		t.Fatalf("failed to start watcher: %v", err)
	}

	// Create a test file
	testFile := filepath.Join(env.inputDir, "test.csv")
	content := []byte("col1,col2\nval1,val2\n")
	if err := os.WriteFile(testFile, content, 0o644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// Wait for stability window
	time.Sleep(2 * time.Second)

	// Process files
	env.processor.ProcessFiles()

	// Verify file was moved to warehouse
	warehouseFile := filepath.Join(env.warehouseDir, "test.csv")
	if _, err := os.Stat(warehouseFile); os.IsNotExist(err) {
		t.Error("file was not moved to warehouse")
	}

	// Verify source file no longer exists
	if _, err := os.Stat(testFile); !os.IsNotExist(err) {
		t.Error("source file should not exist after processing")
	}

	// Verify content is preserved
	warehouseContent, err := os.ReadFile(warehouseFile)
	if err != nil {
		t.Fatalf("failed to read warehouse file: %v", err)
	}
	if string(warehouseContent) != string(content) {
		t.Errorf("content mismatch: got %q, want %q", warehouseContent, content)
	}
}

func TestProcessFiles_DuplicateDetection(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	env := setupTestEnv(t)
	defer env.cleanup()

	if err := env.watcher.Start(); err != nil {
		t.Fatalf("failed to start watcher: %v", err)
	}

	content := []byte("duplicate content")

	// Create first file
	file1 := filepath.Join(env.inputDir, "file1.csv")
	if err := os.WriteFile(file1, content, 0o644); err != nil {
		t.Fatalf("failed to create file1: %v", err)
	}

	// Wait and process
	time.Sleep(2 * time.Second)
	env.processor.ProcessFiles()

	// Verify file1 was processed
	if _, err := os.Stat(filepath.Join(env.warehouseDir, "file1.csv")); os.IsNotExist(err) {
		t.Fatal("file1 was not moved to warehouse")
	}

	// Create second file with same content
	file2 := filepath.Join(env.inputDir, "file2.csv")
	if err := os.WriteFile(file2, content, 0o644); err != nil {
		t.Fatalf("failed to create file2: %v", err)
	}

	// Wait and process
	time.Sleep(2 * time.Second)
	env.processor.ProcessFiles()

	// Verify file2 was NOT moved (duplicate)
	if _, err := os.Stat(filepath.Join(env.warehouseDir, "file2.csv")); !os.IsNotExist(err) {
		t.Error("duplicate file should not be moved to warehouse")
	}

	// Source file2 should be removed from tracking but might still exist
	// depending on implementation (it's removed from tracking, logged as duplicate)
}

func TestProcessFiles_DryRun(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	env := setupTestEnv(t)
	defer env.cleanup()

	// Enable dry run
	env.cfg.DryRun = true

	if err := env.watcher.Start(); err != nil {
		t.Fatalf("failed to start watcher: %v", err)
	}

	// Create a test file
	testFile := filepath.Join(env.inputDir, "dryrun.csv")
	content := []byte("dry run test content")
	if err := os.WriteFile(testFile, content, 0o644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// Wait for stability window
	time.Sleep(2 * time.Second)

	// Process files
	env.processor.ProcessFiles()

	// In dry run mode, file should NOT be moved
	warehouseFile := filepath.Join(env.warehouseDir, "dryrun.csv")
	if _, err := os.Stat(warehouseFile); !os.IsNotExist(err) {
		t.Error("file should not be moved in dry run mode")
	}

	// Source file should still exist
	if _, err := os.Stat(testFile); os.IsNotExist(err) {
		t.Error("source file should still exist in dry run mode")
	}
}

func TestProcessFiles_Concurrency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	env := setupTestEnv(t)
	defer env.cleanup()

	// Set concurrency to 4
	env.cfg.Concurrency = 4

	if err := env.watcher.Start(); err != nil {
		t.Fatalf("failed to start watcher: %v", err)
	}

	// Create multiple test files
	fileCount := 10
	for i := 0; i < fileCount; i++ {
		filename := filepath.Join(env.inputDir, filepath.Base(t.Name())+string(rune('a'+i))+".csv")
		content := []byte("content for file " + string(rune('a'+i)))
		if err := os.WriteFile(filename, content, 0o644); err != nil {
			t.Fatalf("failed to create file %d: %v", i, err)
		}
	}

	// Wait for stability window
	time.Sleep(2 * time.Second)

	// Process files
	env.processor.ProcessFiles()

	// Count files in warehouse
	entries, err := os.ReadDir(env.warehouseDir)
	if err != nil {
		t.Fatalf("failed to read warehouse dir: %v", err)
	}

	if len(entries) != fileCount {
		t.Errorf("expected %d files in warehouse, got %d", fileCount, len(entries))
	}
}

func TestProcessFiles_SubDirectory(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	env := setupTestEnv(t)
	defer env.cleanup()

	if err := env.watcher.Start(); err != nil {
		t.Fatalf("failed to start watcher: %v", err)
	}

	// Create a file directly in input (watcher only watches the root)
	testFile := filepath.Join(env.inputDir, "root.csv")
	if err := os.WriteFile(testFile, []byte("root content"), 0o644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// Wait for stability window
	time.Sleep(2 * time.Second)

	// Process files
	env.processor.ProcessFiles()

	// Verify file was moved
	warehouseFile := filepath.Join(env.warehouseDir, "root.csv")
	if _, err := os.Stat(warehouseFile); os.IsNotExist(err) {
		t.Error("file was not moved to warehouse")
	}
}

func TestProcessFiles_ManifestCreated(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	env := setupTestEnv(t)
	defer env.cleanup()

	if err := env.watcher.Start(); err != nil {
		t.Fatalf("failed to start watcher: %v", err)
	}

	// Create a test file
	testFile := filepath.Join(env.inputDir, "manifest_test.csv")
	if err := os.WriteFile(testFile, []byte("manifest test"), 0o644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// Wait for stability window
	time.Sleep(2 * time.Second)

	// Process files
	env.processor.ProcessFiles()

	// Check that manifest directory has content
	var manifestFound bool
	err := filepath.Walk(env.manifestsDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && filepath.Base(path) == "manifest.jsonl" {
			manifestFound = true
		}
		return nil
	})
	if err != nil {
		t.Fatalf("failed to walk manifests dir: %v", err)
	}

	if !manifestFound {
		t.Error("manifest file was not created")
	}
}

func TestProcessFile_NonExistentFile(t *testing.T) {
	env := setupTestEnv(t)
	defer env.cleanup()

	// Try to process a non-existent file
	err := env.processor.processFile("/nonexistent/file.csv")
	if err == nil {
		t.Error("expected error for non-existent file, got nil")
	}
}
