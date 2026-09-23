package observability

import (
	"encoding/json"
	"net/http"
)

type Check func() error
type Checks struct {
	Postgres     Check
	RabbitMQ     Check
	Destinations Check
}
type healthHandler struct {
	role   string
	checks Checks
}

func NewHealthHandler(role string, checks Checks) http.Handler {
	return healthHandler{role: role, checks: checks}
}
func (handler healthHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set("Content-Type", "application/json")
	if request.URL.Path == "/livez" {
		_ = json.NewEncoder(writer).Encode(map[string]string{"status": "ok"})
		return
	}
	if request.URL.Path != "/readyz" {
		http.NotFound(writer, request)
		return
	}
	required := []Check{handler.checks.Postgres}
	if handler.role == "publisher" || handler.role == "worker" || handler.role == "all" {
		required = append(required, handler.checks.RabbitMQ)
	}
	if handler.role == "api" || handler.role == "worker" || handler.role == "all" {
		required = append(required, handler.checks.Destinations)
	}
	for _, check := range required {
		if check == nil || check() != nil {
			writer.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(writer).Encode(map[string]string{"status": "not_ready"})
			return
		}
	}
	_ = json.NewEncoder(writer).Encode(map[string]string{"status": "ready"})
}
