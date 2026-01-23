package storage

import (
	"os"
	"path/filepath"
	"testing"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func setupTestDB(t *testing.T) (*Storage, func()) {
	t.Helper()

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := gorm.Open(sqlite.Open(dbPath), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	store := New(db)
	if err := store.AutoMigrate(); err != nil {
		t.Fatalf("failed to migrate database: %v", err)
	}

	cleanup := func() {
		sqlDB, _ := db.DB()
		if sqlDB != nil {
			_ = sqlDB.Close()
		}
		_ = os.RemoveAll(tmpDir)
	}

	return store, cleanup
}

func TestNew(t *testing.T) {
	store, cleanup := setupTestDB(t)
	defer cleanup()

	if store == nil {
		t.Error("New returned nil storage")
	}
}

func TestAutoMigrate(t *testing.T) {
	store, cleanup := setupTestDB(t)
	defer cleanup()

	// AutoMigrate is already called in setupTestDB, just verify no error
	if err := store.AutoMigrate(); err != nil {
		t.Errorf("AutoMigrate failed: %v", err)
	}
}

func TestCreateFile(t *testing.T) {
	store, cleanup := setupTestDB(t)
	defer cleanup()

	err := store.CreateFile("abc123", "test.txt", "/path/to/test.txt", 1024)
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}

	// Verify file exists
	exists, err := store.FileExists("abc123")
	if err != nil {
		t.Fatalf("FileExists failed: %v", err)
	}
	if !exists {
		t.Error("file should exist after creation")
	}
}

func TestCreateFile_Duplicate(t *testing.T) {
	store, cleanup := setupTestDB(t)
	defer cleanup()

	sha256 := "duplicate123"

	err := store.CreateFile(sha256, "test1.txt", "/path/to/test1.txt", 1024)
	if err != nil {
		t.Fatalf("first CreateFile failed: %v", err)
	}

	// Attempt to create file with same SHA256
	err = store.CreateFile(sha256, "test2.txt", "/path/to/test2.txt", 2048)
	if err == nil {
		t.Error("expected error when creating duplicate SHA256, got nil")
	}
}

func TestFileExists_NotFound(t *testing.T) {
	store, cleanup := setupTestDB(t)
	defer cleanup()

	exists, err := store.FileExists("nonexistent")
	if err != nil {
		t.Fatalf("FileExists failed: %v", err)
	}
	if exists {
		t.Error("file should not exist")
	}
}

func TestFileExists_Found(t *testing.T) {
	store, cleanup := setupTestDB(t)
	defer cleanup()

	sha256 := "existingfile123"
	err := store.CreateFile(sha256, "test.txt", "/path/to/test.txt", 1024)
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}

	exists, err := store.FileExists(sha256)
	if err != nil {
		t.Fatalf("FileExists failed: %v", err)
	}
	if !exists {
		t.Error("file should exist")
	}
}

func TestTransaction_Success(t *testing.T) {
	store, cleanup := setupTestDB(t)
	defer cleanup()

	err := store.Transaction(func(txStore *Storage) error {
		return txStore.CreateFile("tx123", "test.txt", "/path/test.txt", 512)
	})
	if err != nil {
		t.Fatalf("Transaction failed: %v", err)
	}

	// Verify file was created
	exists, err := store.FileExists("tx123")
	if err != nil {
		t.Fatalf("FileExists failed: %v", err)
	}
	if !exists {
		t.Error("file should exist after successful transaction")
	}
}

func TestTransaction_Rollback(t *testing.T) {
	store, cleanup := setupTestDB(t)
	defer cleanup()

	// Create first file
	err := store.CreateFile("first123", "first.txt", "/path/first.txt", 100)
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}

	// Transaction that should fail (duplicate SHA256)
	err = store.Transaction(func(txStore *Storage) error {
		// This should fail due to unique constraint
		return txStore.CreateFile("first123", "second.txt", "/path/second.txt", 200)
	})
	if err == nil {
		t.Error("expected transaction to fail")
	}

	// Original file should still exist
	exists, err := store.FileExists("first123")
	if err != nil {
		t.Fatalf("FileExists failed: %v", err)
	}
	if !exists {
		t.Error("original file should still exist after failed transaction")
	}
}

func TestCreateFile_MultipleFiles(t *testing.T) {
	store, cleanup := setupTestDB(t)
	defer cleanup()

	files := []struct {
		sha256 string
		name   string
		path   string
		size   int64
	}{
		{"hash1", "file1.txt", "/path/file1.txt", 100},
		{"hash2", "file2.txt", "/path/file2.txt", 200},
		{"hash3", "file3.txt", "/path/file3.txt", 300},
	}

	for _, f := range files {
		err := store.CreateFile(f.sha256, f.name, f.path, f.size)
		if err != nil {
			t.Fatalf("CreateFile failed for %s: %v", f.name, err)
		}
	}

	// Verify all files exist
	for _, f := range files {
		exists, err := store.FileExists(f.sha256)
		if err != nil {
			t.Fatalf("FileExists failed for %s: %v", f.sha256, err)
		}
		if !exists {
			t.Errorf("file %s should exist", f.sha256)
		}
	}
}
