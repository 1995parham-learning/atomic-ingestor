package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type Files struct {
	gorm.Model

	Name string
}

type Config struct {
	Path        string
	Method      string
	Destination string
}

func main() {
	cfg := Config{
		Path:        "files",
		Method:      "sidecar",
		Destination: "dest",
	}

	db, err := gorm.Open(sqlite.Open("gorm.db"), &gorm.Config{})
	if err != nil {
		log.Fatal(err)
	}

	if err := db.Migrator().AutoMigrate(new(Files)); err != nil {
		log.Fatal(err)
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}
	defer watcher.Close()

	var modification *sync.Map = nil
	var completed *sync.Map = nil

	if cfg.Method == "stability_window" {
		modification = &sync.Map{}
	} else if cfg.Method == "sidecar" {
		completed = &sync.Map{}
	} else {
		log.Fatal("invalid method name")
	}

	go func() {
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				log.Println("event:", event)
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
				log.Println("error:", err)
			}
		}
	}()

	err = watcher.Add(cfg.Path)
	if err != nil {
		log.Fatal(err)
	}

	t := time.Tick(1 * time.Second)
	for range t {
		log.Println("Checking...")

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
			log.Println(toProcess)
		}

		for _, f := range toProcess {
			// if err := db.Model(new(Files)).Where("name = ?", f).Error; err == nil {
			// 	if completed != nil {
			// 		completed.Delete(f)
			// 	}
			// 	if modification != nil {
			// 		modification.Delete(f)
			// 	}
			// 	continue
			// }

			if err := db.Transaction(func(tx *gorm.DB) error {
				info, err := os.Stat(f)
				if err != nil {
					return err
				}

				if err := tx.Create(&Files{
					Model: gorm.Model{
						CreatedAt: time.Now(),
					},
					Name: f,
				}).Error; err != nil {
					return err
				}

				if err := CopyFile(f, cfg.Destination+strings.TrimPrefix(f, cfg.Path)); err != nil {
					return err
				}

				if completed != nil {
					completed.Delete(f)
				}
				if modification != nil {
					modification.Delete(f)
				}

				return nil
			}, nil); err != nil {
				log.Printf("failed to move the file into the destination %s", err)
			}
		}
	}
}

// CopyFile copies a file from src to dst. If src and dst files exist, and are
// the same, then return success. Otherise, attempt to create a hard link
// between the two files. If that fail, copy the file contents from src to dst.
func CopyFile(src, dst string) (err error) {
	sfi, err := os.Stat(src)
	if err != nil {
		return
	}
	if !sfi.Mode().IsRegular() {
		// cannot copy non-regular files (e.g., directories,
		// symlinks, devices, etc.)
		return fmt.Errorf("CopyFile: non-regular source file %s (%q)", sfi.Name(), sfi.Mode().String())
	}
	dfi, err := os.Stat(dst)
	if err != nil {
		if !os.IsNotExist(err) {
			return
		}
	} else {
		if !(dfi.Mode().IsRegular()) {
			return fmt.Errorf("CopyFile: non-regular destination file %s (%q)", dfi.Name(), dfi.Mode().String())
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
