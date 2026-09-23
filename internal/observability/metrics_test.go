package observability

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMetricsExposePrometheusCountersWithoutHighCardinalityIDs(t *testing.T) {
	metrics := NewMetrics()
	metrics.Accepted.WithLabelValues("accepted").Inc()
	metrics.DeliveryAttempts.WithLabelValues("delivered").Inc()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rr := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(rr, req)
	body := rr.Body.String()
	if rr.Code != http.StatusOK || !strings.Contains(body, "notifier_notifications_accepted_total") || !strings.Contains(body, "notifier_delivery_attempts_total") {
		t.Fatalf("status=%d body=%s", rr.Code, body)
	}
	if strings.Contains(body, "notification_id") {
		t.Fatalf("high-cardinality identifier exposed as metric label: %s", body)
	}
}
