package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jiahuipaung/rc_pangjiahui/internal/intake"
	"github.com/jiahuipaung/rc_pangjiahui/internal/notification"
)

type fakeCreator struct {
	task notification.Task
	err  error
}

func (creator fakeCreator) Create(context.Context, string, string, string, json.RawMessage) (notification.Task, bool, error) {
	return creator.task, false, creator.err
}

func newRouter(creator Creator) http.Handler {
	return NewRouter(Dependencies{Creator: creator, CallerTokens: map[string]string{"caller-token": "orders"}})
}

func request(body string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/v1/notifications", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer caller-token")
	req.Header.Set("Idempotency-Key", "order-1")
	return req
}

func TestCreateReturnsAcceptedJSON(t *testing.T) {
	router := newRouter(fakeCreator{task: notification.Task{ID: "n-1", Status: notification.StatusPending}})
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, request(`{"destination_id":"crm","payload":{"order_id":"1"}}`))
	if rr.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body=%s", rr.Code, rr.Body.String())
	}
	if got := rr.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("content type = %q", got)
	}
}

func TestCreateRejectsTrailingJSON(t *testing.T) {
	rr := httptest.NewRecorder()
	newRouter(fakeCreator{}).ServeHTTP(rr, request(`{"destination_id":"crm","payload":{}} {}`))
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d", rr.Code)
	}
}

func TestCreateRejectsUnknownFields(t *testing.T) {
	rr := httptest.NewRecorder()
	newRouter(fakeCreator{}).ServeHTTP(rr, request(`{"destination_id":"crm","payload":{},"url":"https://evil.invalid"}`))
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d", rr.Code)
	}
}

func TestCreateRejectsOversizedBody(t *testing.T) {
	payload := bytes.Repeat([]byte("a"), MaxRequestBodyBytes+1)
	rr := httptest.NewRecorder()
	newRouter(fakeCreator{}).ServeHTTP(rr, request(`{"destination_id":"crm","payload":"`+string(payload)+`"}`))
	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d", rr.Code)
	}
}

func TestCreateRequiresAuthenticationAndIdempotencyKey(t *testing.T) {
	router := newRouter(fakeCreator{})
	unauthenticated := request(`{"destination_id":"crm","payload":{}}`)
	unauthenticated.Header.Del("Authorization")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, unauthenticated)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status = %d", rr.Code)
	}

	missingKey := request(`{"destination_id":"crm","payload":{}}`)
	missingKey.Header.Del("Idempotency-Key")
	rr = httptest.NewRecorder()
	router.ServeHTTP(rr, missingKey)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("missing key status = %d", rr.Code)
	}
}

func TestCreateMapsDomainErrors(t *testing.T) {
	tests := []struct {
		err  error
		want int
	}{
		{intake.ErrIdempotencyConflict, http.StatusConflict},
		{intake.ErrUnknownDestination, http.StatusUnprocessableEntity},
		{errors.New("database unavailable"), http.StatusInternalServerError},
	}
	for _, tt := range tests {
		rr := httptest.NewRecorder()
		newRouter(fakeCreator{err: tt.err}).ServeHTTP(rr, request(`{"destination_id":"crm","payload":{}}`))
		if rr.Code != tt.want {
			t.Fatalf("error %v status = %d, want %d", tt.err, rr.Code, tt.want)
		}
		if strings.Contains(rr.Body.String(), "database unavailable") {
			t.Fatal("internal error leaked")
		}
	}
}
