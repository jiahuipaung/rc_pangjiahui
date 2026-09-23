package observability

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jiahuipaung/rc_pangjiahui/internal/notification"
)

func TestMetricsExposePrometheusCountersWithoutHighCardinalityIDs(t *testing.T) {
	metrics := NewMetrics()
	metrics.ObserveIntake("accepted")
	metrics.ObserveOutboxPublish("confirmed")
	metrics.ObserveDelivery("delivered")
	metrics.ObserveDelivery("dead")
	metrics.ObserveSchedule(notification.ScheduleResult{Scheduled: 2, Dead: 1, RecoveredLeases: 1})
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rr := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(rr, req)
	body := rr.Body.String()
	for _, expected := range []string{
		"notifier_notifications_accepted_total{outcome=\"accepted\"} 1",
		"notifier_outbox_publish_total{outcome=\"confirmed\"} 1",
		"notifier_delivery_attempts_total{outcome=\"delivered\"} 1",
		"notifier_dead_total 2",
		"notifier_retry_scheduled_total 2",
		"notifier_delivery_leases_recovered_total 1",
	} {
		if !strings.Contains(body, expected) {
			t.Fatalf("missing %q in metrics body:\n%s", expected, body)
		}
	}
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, body)
	}
	if strings.Contains(body, "notification_id") {
		t.Fatalf("high-cardinality identifier exposed as metric label: %s", body)
	}
}
