package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type Files struct {
	gorm.Model

	SHA256 string `gorm:"uniqueIndex;not null"`
	Name   string
	Path   string
	Size   int64
}

type Config struct {
	Path        string
	Method      string
	Destination string
}

func main() {
	// Initialize structured logger
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	cfg := Config{
		Path:        "files",
		Method:      "sidecar",
		Destination: "dest",
	}

	db, err := gorm.Open(sqlite.Open("gorm.db"), &gorm.Config{})
	if err != nil {
		slog.Error("failed to open database", "error", err)
		os.Exit(1)
	}

	if err := db.Migrator().AutoMigrate(new(Files)); err != nil {
		slog.Error("failed to migrate database", "error", err)
		os.Exit(1)
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		slog.Error("failed to create watcher", "error", err)
		os.Exit(1)
	}
	defer watcher.Close()

	var modification *sync.Map = nil
	var completed *sync.Map = nil

	if cfg.Method == "stability_window" {
		modification = &sync.Map{}
	} else if cfg.Method == "sidecar" {
		completed = &sync.Map{}
	} else {
		slog.Error("invalid method name", "method", cfg.Method)
		os.Exit(1)
	}

	go func() {
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				slog.Info("file system event", "event", event.Op.String(), "path", event.Name)
				if event.Has(fsnotify.Create) {
					if modification != nil {
						modification.Store(event.Name, time.Now())
					}
					if completed != nil && strings.HasSuffix(event.Name, ".ok") {
						completed.Store(strings.TrimSuffix(event.Name, ".ok"), true)
					}
				}
				if event.Has(fsnotify.Write) {
					if modification != nil {
						modification.Store(event.Name, time.Now())
					}
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				slog.Error("watcher error", "error", err)
			}
		}
	}()

	err = watcher.Add(cfg.Path)
	if err != nil {
		slog.Error("failed to add path to watcher", "path", cfg.Path, "error", err)
		os.Exit(1)
	}

	t := time.Tick(1 * time.Second)
	for range t {
		slog.Debug("checking for files to process")

		toProcess := make([]string, 0)

		if modification != nil {
			modification.Range(func(key, value any) bool {
				name := key.(string)
				mtime := value.(time.Time)

				if mtime.Add(10 * time.Second).Before(time.Now()) {
					toProcess = append(toProcess, name)
				}

				return true
			})
		}

		if completed != nil {
			completed.Range(func(key, value any) bool {
				name := key.(string)
				ok := value.(bool)

				if ok {
					toProcess = append(toProcess, name)
				}

				return true
			})
		}

		if len(toProcess) > 0 {
			slog.Info("files ready to process", "count", len(toProcess), "files", toProcess)
		}

		for _, f := range toProcess {
			// Get file info and calculate SHA256
			info, err := os.Stat(f)
			if err != nil {
				slog.Warn("failed to stat file", "path", f, "error", err)
				if completed != nil {
					completed.Delete(f)
				}
				if modification != nil {
					modification.Delete(f)
				}
				continue
			}

			hash, err := calculateSHA256(f)
			if err != nil {
				slog.Warn("failed to calculate SHA256", "path", f, "error", err)
				if completed != nil {
					completed.Delete(f)
				}
				if modification != nil {
					modification.Delete(f)
				}
				continue
			}

			// Check if file with same SHA256 was already processed
			var existingFile Files
			if err := db.Where("sha256 = ?", hash).First(&existingFile).Error; err == nil {
				// File with same content already exists in database, skip processing
				slog.Info("file already processed, skipping", "path", f, "sha256", hash)
				if completed != nil {
					completed.Delete(f)
				}
				if modification != nil {
					modification.Delete(f)
				}
				continue
			}

			if err := db.Transaction(func(tx *gorm.DB) error {
				if err := tx.Create(&Files{
					Model: gorm.Model{
						CreatedAt: time.Now(),
					},
					SHA256: hash,
					Name:   info.Name(),
					Path:   f,
					Size:   info.Size(),
				}).Error; err != nil {
					return err
				}

				// Calculate relative path and join with destination
				relPath, err := filepath.Rel(cfg.Path, f)
				if err != nil {
					return err
				}
				dstPath := filepath.Join(cfg.Destination, relPath)

				// Ensure destination directory exists
				dstDir := filepath.Dir(dstPath)
				if err := os.MkdirAll(dstDir, 0755); err != nil {
					return err
				}

				if err := CopyFile(f, dstPath); err != nil {
					return err
				}

				if completed != nil {
					completed.Delete(f)
				}
				if modification != nil {
					modification.Delete(f)
				}

				slog.Info("file processed successfully", "path", f, "sha256", hash, "destination", dstPath)
				return nil
			}, nil); err != nil {
				slog.Error("failed to process file", "path", f, "error", err)
			}
		}
	}
}

// CopyFile copies a file from src to dst. If src and dst files exist, and are
// the same, then return success. Otherise, attempt to create a hard link
// between the two files. If that fail, copy the file contents from src to dst.
func CopyFile(src, dst string) (err error) {
	// Clean paths
	src = filepath.Clean(src)
	dst = filepath.Clean(dst)

	sfi, err := os.Stat(src)
	if err != nil {
		return
	}
	if !sfi.Mode().IsRegular() {
		// cannot copy non-regular files (e.g., directories,
		// symlinks, devices, etc.)
		return fmt.Errorf("CopyFile: non-regular source file %s (%q)", filepath.Base(src), sfi.Mode().String())
	}
	dfi, err := os.Stat(dst)
	if err != nil {
		if !os.IsNotExist(err) {
			return
		}
	} else {
		if !(dfi.Mode().IsRegular()) {
			return fmt.Errorf("CopyFile: non-regular destination file %s (%q)", filepath.Base(dst), dfi.Mode().String())
		}
		if os.SameFile(sfi, dfi) {
			return
		}
	}
	if err = os.Link(src, dst); err == nil {
		return
	}
	err = copyFileContents(src, dst)
	return
}

// copyFileContents copies the contents of the file named src to the file named
// by dst. The file will be created if it does not already exist. If the
// destination file exists, all it's contents will be replaced by the contents
// of the source file.
func copyFileContents(src, dst string) (err error) {
	in, err := os.Open(src)
	if err != nil {
		return
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return
	}
	defer func() {
		cerr := out.Close()
		if err == nil {
			err = cerr
		}
	}()
	if _, err = io.Copy(out, in); err != nil {
		return
	}
	err = out.Sync()
	return
}

// calculateSHA256 calculates the SHA256 hash of a file
func calculateSHA256(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}

	return hex.EncodeToString(hash.Sum(nil)), nil
}
