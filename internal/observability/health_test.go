package observability

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestWorkerReadinessRequiresPostgresRabbitAndDestinations(t *testing.T) {
	checks := Checks{
		Postgres:     func() error { return nil },
		RabbitMQ:     func() error { return errors.New("down") },
		Destinations: func() error { return nil },
	}
	handler := NewHealthHandler("worker", checks)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d", rr.Code)
	}
}

func TestAPIReadinessDoesNotRequireRabbitMQ(t *testing.T) {
	checks := Checks{Postgres: func() error { return nil }, RabbitMQ: func() error { return errors.New("down") }, Destinations: func() error { return nil }}
	rr := httptest.NewRecorder()
	NewHealthHandler("api", checks).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d", rr.Code)
	}
}

func TestLivenessDoesNotProbeDependencies(t *testing.T) {
	called := false
	checks := Checks{Postgres: func() error { called = true; return errors.New("down") }}
	rr := httptest.NewRecorder()
	NewHealthHandler("api", checks).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/livez", nil))
	if rr.Code != http.StatusOK || called {
		t.Fatalf("status=%d called=%v", rr.Code, called)
	}
}
