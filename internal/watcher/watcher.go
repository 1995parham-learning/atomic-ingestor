package watcher

import (
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/1995parham-learning/interface-ai-coding-challenge/internal/config"
	"github.com/fsnotify/fsnotify"
)

// tempFileSuffixes lists file extensions that indicate temporary/incomplete files
var tempFileSuffixes = []string{
	".tmp",
	".part",
	".swp",
	".crdownload",
	".partial",
	".download",
	"~",
}

// shouldIgnoreFile returns true if the file should be ignored based on its name
func shouldIgnoreFile(path string) bool {
	name := filepath.Base(path)

	// Ignore hidden files (starting with .)
	if strings.HasPrefix(name, ".") {
		return true
	}

	// Ignore temporary file patterns
	for _, suffix := range tempFileSuffixes {
		if strings.HasSuffix(name, suffix) {
			return true
		}
	}

	return false
}

type Watcher struct {
	fsWatcher        *fsnotify.Watcher
	modification     *sync.Map
	completed        *sync.Map
	method           string
	watchPath        string
	stabilitySeconds int
}

func New(method, watchPath string, stabilitySeconds int) (*Watcher, error) {
	fsWatcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("create fsnotify watcher: %w", err)
	}

	w := &Watcher{
		fsWatcher:        fsWatcher,
		method:           method,
		watchPath:        watchPath,
		stabilitySeconds: stabilitySeconds,
		completed:        nil,
		modification:     nil,
	}

	switch method {
	case config.MethodStabilityWindow:
		w.modification = &sync.Map{}
	case config.MethodSidecar:
		w.completed = &sync.Map{}
	default:
		return nil, fmt.Errorf("unknown watch method: %s", method)
	}

	return w, nil
}

func (w *Watcher) Start() error {
	go w.eventLoop()

	if err := w.fsWatcher.Add(w.watchPath); err != nil {
		return fmt.Errorf("add watch path %s: %w", w.watchPath, err)
	}

	return nil
}

func (w *Watcher) Close() error {
	return w.fsWatcher.Close()
}

func (w *Watcher) eventLoop() {
	for {
		select {
		case event, ok := <-w.fsWatcher.Events:
			if !ok {
				return
			}

			// Handle .ok sidecar files first (they signal completion of another file)
			if w.completed != nil && strings.HasSuffix(event.Name, ".ok") {
				if event.Has(fsnotify.Create) {
					targetFile := strings.TrimSuffix(event.Name, ".ok")
					slog.Debug("sidecar file detected", "sidecar", event.Name, "target", targetFile)
					w.completed.Store(targetFile, true)
				}
				continue
			}

			// Skip files that should be ignored (hidden, temp, etc.)
			if shouldIgnoreFile(event.Name) {
				slog.Debug("ignoring file", "path", event.Name, "reason", "hidden or temp file")
				continue
			}

			slog.Debug("file system event", "event", event.Op.String(), "path", event.Name)

			if event.Has(fsnotify.Create) || event.Has(fsnotify.Write) {
				if w.modification != nil {
					w.modification.Store(event.Name, time.Now())
				}
			}
		case err, ok := <-w.fsWatcher.Errors:
			if !ok {
				return
			}
			slog.Error("watcher error", "error", err)
		}
	}
}

func (w *Watcher) GetFilesToProcess() []string {
	toProcess := make([]string, 0)

	if w.modification != nil {
		w.modification.Range(func(key, value any) bool {
			name := key.(string)
			mtime := value.(time.Time)

			if mtime.Add(time.Duration(w.stabilitySeconds) * time.Second).Before(time.Now()) {
				toProcess = append(toProcess, name)
			}

			return true
		})
	}

	if w.completed != nil {
		w.completed.Range(func(key, value any) bool {
			name := key.(string)
			ok := value.(bool)

			if ok {
				toProcess = append(toProcess, name)
			}

			return true
		})
	}

	return toProcess
}

func (w *Watcher) RemoveFromTracking(path string) {
	if w.completed != nil {
		w.completed.Delete(path)
	}
	if w.modification != nil {
		w.modification.Delete(path)
	}
}
