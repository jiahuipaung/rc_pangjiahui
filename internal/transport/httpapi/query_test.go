package httpapi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jiahuipaung/rc_pangjiahui/internal/notification"
)

type fakeQuerier struct {
	view notification.View
	err  error
}

func (query fakeQuerier) Get(context.Context, string, string) (notification.View, error) {
	return query.view, query.err
}

type fakeReplayer struct{ err error }

func (replay fakeReplayer) Replay(context.Context, string, string) error { return replay.err }

func TestQueryDoesNotReturnPayloadOrSecrets(t *testing.T) {
	router := NewRouter(Dependencies{Creator: fakeCreator{}, Querier: fakeQuerier{view: notification.View{ID: "n-1", DestinationID: "crm", Status: notification.StatusRetryWait}}, CallerTokens: map[string]string{"caller-token": "orders"}})
	req := httptest.NewRequest(http.MethodGet, "/v1/notifications/n-1", nil)
	req.Header.Set("Authorization", "Bearer caller-token")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	if strings.Contains(rr.Body.String(), "api-key") || strings.Contains(rr.Body.String(), "order_id") {
		t.Fatalf("sensitive response: %s", rr.Body.String())
	}
}

func TestQueryMapsMissingOrCrossCallerTaskToNotFound(t *testing.T) {
	router := NewRouter(Dependencies{Creator: fakeCreator{}, Querier: fakeQuerier{err: notification.ErrTaskNotFound}, CallerTokens: map[string]string{"caller-token": "orders"}})
	req := httptest.NewRequest(http.MethodGet, "/v1/notifications/n-1", nil)
	req.Header.Set("Authorization", "Bearer caller-token")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("status=%d", rr.Code)
	}
}

func TestReplayNonDeadTaskReturnsConflict(t *testing.T) {
	router := NewRouter(Dependencies{Creator: fakeCreator{}, Replayer: fakeReplayer{err: notification.ErrNotDead}, CallerTokens: map[string]string{"caller-token": "orders"}, AdminTokens: map[string]string{"admin-token": "operator"}})
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/notifications/n-1/replay", nil)
	req.Header.Set("Authorization", "Bearer admin-token")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusConflict {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
}

func TestReplayRequiresSeparateAdminToken(t *testing.T) {
	router := NewRouter(Dependencies{Creator: fakeCreator{}, Replayer: fakeReplayer{}, CallerTokens: map[string]string{"caller-token": "orders"}, AdminTokens: map[string]string{"admin-token": "operator"}})
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/notifications/n-1/replay", nil)
	req.Header.Set("Authorization", "Bearer caller-token")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d", rr.Code)
	}
}

func TestQueryInternalFailureIsSanitized(t *testing.T) {
	router := NewRouter(Dependencies{Creator: fakeCreator{}, Querier: fakeQuerier{err: errors.New("database password leaked")}, CallerTokens: map[string]string{"caller-token": "orders"}})
	req := httptest.NewRequest(http.MethodGet, "/v1/notifications/n-1", nil)
	req.Header.Set("Authorization", "Bearer caller-token")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusInternalServerError || strings.Contains(rr.Body.String(), "password") {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
}
