package storage

import (
	"fmt"
	"time"

	"gorm.io/gorm"
)

type File struct {
	gorm.Model

	SHA256 string `gorm:"uniqueIndex;not null"`
	Name   string
	Path   string
	Size   int64
}

type Storage struct {
	db *gorm.DB
}

func New(db *gorm.DB) *Storage {
	return &Storage{db: db}
}

// AutoMigrate runs database migrations
func (s *Storage) AutoMigrate() error {
	if err := s.db.AutoMigrate(&File{}); err != nil {
		return fmt.Errorf("auto migrate file table: %w", err)
	}
	return nil
}

// FileExists checks if a file with the given SHA256 already exists
func (s *Storage) FileExists(sha256 string) (bool, error) {
	var file File
	err := s.db.Where("sha256 = ?", sha256).First(&file).Error
	if err == gorm.ErrRecordNotFound {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("query file by sha256: %w", err)
	}
	return true, nil
}

// CreateFile stores a new file record in the database
func (s *Storage) CreateFile(sha256, name, path string, size int64) error {
	file := File{
		Model: gorm.Model{
			CreatedAt: time.Now(),
		},
		SHA256: sha256,
		Name:   name,
		Path:   path,
		Size:   size,
	}
	if err := s.db.Create(&file).Error; err != nil {
		return fmt.Errorf("create file record: %w", err)
	}
	return nil
}

// Transaction wraps operations in a database transaction
func (s *Storage) Transaction(fn func(*Storage) error) error {
	return s.db.Transaction(func(tx *gorm.DB) error {
		txStorage := &Storage{db: tx}
		return fn(txStorage)
	})
}
