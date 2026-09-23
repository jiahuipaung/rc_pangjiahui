package config

import "fmt"

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
