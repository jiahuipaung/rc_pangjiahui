package config

import "testing"

func TestParseRoleAcceptsSupportedRoles(t *testing.T) {
	for _, input := range []string{"api", "publisher", "worker", "scheduler", "all"} {
		if got, err := ParseRole(input); err != nil || string(got) != input {
			t.Fatalf("ParseRole(%q) = %q, %v", input, got, err)
		}
	}
}

func TestParseRoleRejectsUnknownRole(t *testing.T) {
	if _, err := ParseRole("unknown"); err == nil {
		t.Fatal("expected unsupported role to be rejected")
	}
}

func TestLoadRuntimeRequiresRoleDependenciesAndParsesTokens(t *testing.T) {
	environment := map[string]string{
		"DATABASE_URL": "postgres://db/notifier", "RABBITMQ_URL": "amqp://broker/", "DESTINATIONS_FILE": "/config/destinations.yaml",
		"CALLER_TOKENS": "caller-token:orders", "ADMIN_TOKENS": "admin-token:operator", "HTTP_ADDR": ":8080",
	}
	lookup := func(key string) (string, bool) { value, ok := environment[key]; return value, ok }
	config, err := LoadRuntime(RoleAll, lookup)
	if err != nil {
		t.Fatal(err)
	}
	if config.CallerTokens["caller-token"] != "orders" || config.AdminTokens["admin-token"] != "operator" || config.HTTPAddr != ":8080" {
		t.Fatalf("config=%+v", config)
	}
}

func TestLoadRuntimeRejectsMissingDatabase(t *testing.T) {
	_, err := LoadRuntime(RoleAPI, func(string) (string, bool) { return "", false })
	if err == nil {
		t.Fatal("expected missing database URL error")
	}
}
