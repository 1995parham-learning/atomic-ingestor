package config

type Config struct {
	Path             string
	Method           string
	Destination      string
	ManifestsPath    string
	StabilitySeconds int
	StatePath        string
	LogLevel         string
	Concurrency      int
	DryRun           bool
}

const (
	MethodStabilityWindow = "stability_window"
	MethodSidecar         = "sidecar"
)

// Default values
const (
	DefaultInputPath        = "files"
	DefaultWarehousePath    = "warehouse"
	DefaultManifestsPath    = "manifests"
	DefaultMethod           = MethodSidecar
	DefaultStabilitySeconds = 10
	DefaultStatePath        = "gorm.db"
	DefaultLogLevel         = "info"
	DefaultConcurrency      = 1
)
