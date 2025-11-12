package storage

import (
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
	return s.db.AutoMigrate(&File{})
}

// FileExists checks if a file with the given SHA256 already exists
func (s *Storage) FileExists(sha256 string) (bool, error) {
	var file File
	err := s.db.Where("sha256 = ?", sha256).First(&file).Error
	if err == gorm.ErrRecordNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
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
	return s.db.Create(&file).Error
}

// Transaction wraps operations in a database transaction
func (s *Storage) Transaction(fn func(*Storage) error) error {
	return s.db.Transaction(func(tx *gorm.DB) error {
		txStorage := &Storage{db: tx}
		return fn(txStorage)
	})
}
