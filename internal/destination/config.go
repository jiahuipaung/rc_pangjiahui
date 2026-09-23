package destination

import (
	"fmt"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/jiahuipaung/rc_pangjiahui/internal/notification"
	"gopkg.in/yaml.v3"
)

type Destination struct {
	ID               string
	Method           string
	URL              *url.URL
	StaticHeaders    http.Header
	SecretHeaders    map[string]string
	Timeout          time.Duration
	Retry            notification.RetryPolicy
	ConcurrencyLimit int
}

type Registry struct {
	items map[string]Destination
}

func (r Registry) Get(id string) (Destination, bool) {
	destination, ok := r.items[id]
	if !ok {
		return Destination{}, false
	}
	return clone(destination), true
}

type fileConfig struct {
	Destinations []rawDestination `yaml:"destinations"`
}

type rawDestination struct {
	ID               string            `yaml:"id"`
	Method           string            `yaml:"method"`
	URL              string            `yaml:"url"`
	StaticHeaders    map[string]string `yaml:"static_headers"`
	SecretHeaders    map[string]string `yaml:"secret_headers"`
	Timeout          string            `yaml:"timeout"`
	MaxAttempts      int               `yaml:"max_attempts"`
	Lifetime         string            `yaml:"lifetime"`
	RetryDelays      []string          `yaml:"retry_delays"`
	ConcurrencyLimit int               `yaml:"concurrency_limit"`
}

func Load(path string, lookupEnv func(string) (string, bool)) (Registry, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Registry{}, fmt.Errorf("read destination config: %w", err)
	}
	var raw fileConfig
	decoder := yaml.NewDecoder(bytesReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&raw); err != nil {
		return Registry{}, fmt.Errorf("decode destination config: %w", err)
	}
	if len(raw.Destinations) == 0 {
		return Registry{}, fmt.Errorf("destination config is empty")
	}
	items := make(map[string]Destination, len(raw.Destinations))
	for _, candidate := range raw.Destinations {
		if _, exists := items[candidate.ID]; exists {
			return Registry{}, fmt.Errorf("duplicate destination %q", candidate.ID)
		}
		destination, err := parseDestination(candidate, lookupEnv)
		if err != nil {
			return Registry{}, fmt.Errorf("destination %q: %w", candidate.ID, err)
		}
		items[destination.ID] = destination
	}
	return Registry{items: items}, nil
}

func clone(source Destination) Destination {
	copy := source
	if source.URL != nil {
		urlCopy := *source.URL
		copy.URL = &urlCopy
	}
	copy.StaticHeaders = source.StaticHeaders.Clone()
	copy.SecretHeaders = make(map[string]string, len(source.SecretHeaders))
	for key, value := range source.SecretHeaders {
		copy.SecretHeaders[key] = value
	}
	copy.Retry.Delays = append([]time.Duration(nil), source.Retry.Delays...)
	return copy
}
