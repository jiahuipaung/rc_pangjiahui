package delivery

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jiahuipaung/rc_pangjiahui/internal/notification"
)

func testSender(t *testing.T) *HTTPSender {
	t.Helper()
	return NewHTTPSender(SenderConfig{AllowPrivateNetworks: true, MaxDiagnosticBytes: 1024}, func(key string) (string, bool) {
		if key == "API_TOKEN" {
			return "Bearer secret", true
		}
		return "", false
	})
}

func snapshotFor(rawURL string) notification.DeliverySnapshot {
	return notification.DeliverySnapshot{
		DestinationID: "test", Method: http.MethodPost, URL: rawURL, Body: []byte(`{"hello":"world"}`), Timeout: time.Second,
		StaticHeaders: http.Header{"Content-Type": {"application/json"}}, SecretHeaders: map[string]string{"Authorization": "API_TOKEN"},
		IdempotencyKey: "key-1",
	}
}

func TestSenderClassifiesStatusAndForwardsConfiguredRequest(t *testing.T) {
	var receivedAuth, receivedKey, receivedBody string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		receivedAuth = request.Header.Get("Authorization")
		receivedKey = request.Header.Get("Idempotency-Key")
		buffer := new(bytes.Buffer)
		_, _ = buffer.ReadFrom(request.Body)
		receivedBody = buffer.String()
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	result := testSender(t).Send(t.Context(), snapshotFor(server.URL))
	if result.Outcome != notification.Delivered || result.HTTPStatus != http.StatusNoContent {
		t.Fatalf("result = %+v", result)
	}
	if receivedAuth != "Bearer secret" || receivedKey != "key-1" || receivedBody != `{"hello":"world"}` {
		t.Fatalf("received auth=%q key=%q body=%q", receivedAuth, receivedKey, receivedBody)
	}
}

func TestSenderRejectsCrossHostRedirect(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	defer target.Close()
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer source.Close()

	result := testSender(t).Send(t.Context(), snapshotFor(source.URL))
	if result.Outcome != notification.Permanent || result.ErrorCode != "redirect_not_allowed" {
		t.Fatalf("result = %+v", result)
	}
}

func TestSenderLimitsResponseBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write(bytes.Repeat([]byte("x"), 64<<10))
	}))
	defer server.Close()
	result := testSender(t).Send(t.Context(), snapshotFor(server.URL))
	if result.Outcome != notification.Retryable || result.ErrorCode != "http_5xx" || len(result.ErrorMessage) > 1024 {
		t.Fatalf("result = %+v, message length=%d", result, len(result.ErrorMessage))
	}
}

func TestSenderParsesRetryAfter(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "120")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()
	before := time.Now()
	result := testSender(t).Send(t.Context(), snapshotFor(server.URL))
	if result.RetryAfter == nil || result.RetryAfter.Before(before.Add(119*time.Second)) {
		t.Fatalf("retry after = %v", result.RetryAfter)
	}
}

func TestSenderRejectsPrivateDestinationByDefault(t *testing.T) {
	sender := NewHTTPSender(SenderConfig{MaxDiagnosticBytes: 1024}, func(string) (string, bool) { return "", false })
	result := sender.Send(t.Context(), snapshotFor("http://127.0.0.1/hook"))
	if result.Outcome != notification.Permanent || result.ErrorCode != "destination_not_allowed" {
		t.Fatalf("result = %+v", result)
	}
}

func TestSenderTimesOut(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { time.Sleep(100 * time.Millisecond) }))
	defer server.Close()
	snapshot := snapshotFor(server.URL)
	snapshot.Timeout = 10 * time.Millisecond
	result := testSender(t).Send(context.Background(), snapshot)
	if result.Outcome != notification.Retryable || result.ErrorCode != "transport_error" {
		t.Fatalf("result = %+v", result)
	}
}
