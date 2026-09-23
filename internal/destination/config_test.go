package destination

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "destinations.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

func lookupFixtureEnv(key string) (string, bool) {
	if key == "CRM_TOKEN" {
		return "secret-value", true
	}
	return "", false
}

func validConfig(rawURL string) string {
	return strings.ReplaceAll(`
destinations:
  - id: crm
    method: POST
    url: URL_VALUE
    static_headers:
      Content-Type: application/json
    secret_headers:
      Authorization: CRM_TOKEN
    timeout: 5s
    max_attempts: 8
    lifetime: 24h
    retry_delays: [5s, 30s, 2m]
    concurrency_limit: 4
`, "URL_VALUE", rawURL)
}

func TestLoadRejectsUnsafeProductionDestination(t *testing.T) {
	_, err := Load(writeConfig(t, validConfig("https://127.0.0.1/hook")), lookupFixtureEnv)
	if err == nil || !strings.Contains(err.Error(), "disallowed destination network") {
		t.Fatalf("Load() error = %v, want disallowed destination network", err)
	}
}

func TestLoadRejectsMissingSecretReference(t *testing.T) {
	_, err := Load(writeConfig(t, validConfig("https://198.51.100.10/hook")), func(string) (string, bool) { return "", false })
	if err == nil || !strings.Contains(err.Error(), "missing environment variable") {
		t.Fatalf("Load() error = %v, want missing environment variable", err)
	}
}

func TestLoadRejectsDuplicateDestinationID(t *testing.T) {
	one := strings.TrimPrefix(validConfig("https://198.51.100.10/hook"), "\ndestinations:\n")
	config := "destinations:\n" + one + one
	_, err := Load(writeConfig(t, config), lookupFixtureEnv)
	if err == nil || !strings.Contains(err.Error(), "duplicate destination") {
		t.Fatalf("Load() error = %v, want duplicate destination", err)
	}
}

func TestRegistryReturnsDefensiveCopy(t *testing.T) {
	registry, err := Load(writeConfig(t, validConfig("https://198.51.100.10/hook")), lookupFixtureEnv)
	if err != nil {
		t.Fatalf("Load(): %v", err)
	}
	first, ok := registry.Get("crm")
	if !ok {
		t.Fatal("destination crm not found")
	}
	first.StaticHeaders.Set("X-Mutated", "yes")
	second, _ := registry.Get("crm")
	if second.StaticHeaders.Get("X-Mutated") != "" {
		t.Fatal("registry leaked mutable header state")
	}
}
