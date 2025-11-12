package config

type Config struct {
	Path        string
	Method      string
	Destination string
}

const (
	MethodStabilityWindow = "stability_window"
	MethodSidecar         = "sidecar"
)
