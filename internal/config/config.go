package config

import (
	"fmt"
	"strings"
	"time"
)

type Role string

const (
	RoleAPI       Role = "api"
	RolePublisher Role = "publisher"
	RoleWorker    Role = "worker"
	RoleScheduler Role = "scheduler"
	RoleAll       Role = "all"
)

func ParseRole(value string) (Role, error) {
	role := Role(value)
	switch role {
	case RoleAPI, RolePublisher, RoleWorker, RoleScheduler, RoleAll:
		return role, nil
	default:
		return "", fmt.Errorf("unsupported role %q", value)
	}
}

type Runtime struct {
	Role             Role
	DatabaseURL      string
	RabbitMQURL      string
	DestinationsFile string
	HTTPAddr         string
	MetricsAddr      string
	CallerTokens     map[string]string
	AdminTokens      map[string]string
	ShutdownTimeout  time.Duration
}

func LoadRuntime(role Role, lookup func(string) (string, bool)) (Runtime, error) {
	get := func(key string) string { value, _ := lookup(key); return strings.TrimSpace(value) }
	config := Runtime{Role: role, DatabaseURL: get("DATABASE_URL"), RabbitMQURL: get("RABBITMQ_URL"), DestinationsFile: get("DESTINATIONS_FILE"), HTTPAddr: get("HTTP_ADDR"), ShutdownTimeout: 10 * time.Second}
	if config.DatabaseURL == "" {
		return Runtime{}, fmt.Errorf("DATABASE_URL is required")
	}
	if role == RolePublisher || role == RoleWorker || role == RoleAll {
		if config.RabbitMQURL == "" {
			return Runtime{}, fmt.Errorf("RABBITMQ_URL is required")
		}
	}
	if role == RoleAPI || role == RoleWorker || role == RoleAll {
		if config.DestinationsFile == "" {
			return Runtime{}, fmt.Errorf("DESTINATIONS_FILE is required")
		}
	}
	if role == RoleAPI || role == RoleAll {
		config.CallerTokens = parseTokens(get("CALLER_TOKENS"))
		config.AdminTokens = parseTokens(get("ADMIN_TOKENS"))
		if len(config.CallerTokens) == 0 || len(config.AdminTokens) == 0 {
			return Runtime{}, fmt.Errorf("CALLER_TOKENS and ADMIN_TOKENS are required")
		}
		if config.HTTPAddr == "" {
			config.HTTPAddr = ":8080"
		}
	} else {
		config.MetricsAddr = get("METRICS_ADDR")
		if config.MetricsAddr == "" {
			config.MetricsAddr = ":9090"
		}
	}
	return config, nil
}

func parseTokens(value string) map[string]string {
	result := map[string]string{}
	for _, entry := range strings.Split(value, ",") {
		parts := strings.SplitN(strings.TrimSpace(entry), ":", 2)
		if len(parts) == 2 && parts[0] != "" && parts[1] != "" {
			result[parts[0]] = parts[1]
		}
	}
	return result
}
