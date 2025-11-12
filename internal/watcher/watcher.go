package watcher

import (
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/1995parham-learning/interface-ai-coding-challenge/internal/config"
	"github.com/fsnotify/fsnotify"
)

type Watcher struct {
	fsWatcher    *fsnotify.Watcher
	modification *sync.Map
	completed    *sync.Map
	method       string
	watchPath    string
}

func New(method, watchPath string) (*Watcher, error) {
	fsWatcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}

	w := &Watcher{
		fsWatcher: fsWatcher,
		method:    method,
		watchPath: watchPath,
	}

	switch method {
	case config.MethodStabilityWindow:
		w.modification = &sync.Map{}
	case config.MethodSidecar:
		w.completed = &sync.Map{}
	}

	return w, nil
}

func (w *Watcher) Start() error {
	go w.eventLoop()

	err := w.fsWatcher.Add(w.watchPath)
	if err != nil {
		return err
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
			slog.Info("file system event", "event", event.Op.String(), "path", event.Name)
			if event.Has(fsnotify.Create) {
				if w.modification != nil {
					w.modification.Store(event.Name, time.Now())
				}
				if w.completed != nil && strings.HasSuffix(event.Name, ".ok") {
					w.completed.Store(strings.TrimSuffix(event.Name, ".ok"), true)
				}
			}
			if event.Has(fsnotify.Write) {
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

			if mtime.Add(10 * time.Second).Before(time.Now()) {
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
