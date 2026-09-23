package observability

import (
	"net/http"

	"github.com/jiahuipaung/rc_pangjiahui/internal/notification"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	registry         *prometheus.Registry
	Accepted         *prometheus.CounterVec
	OutboxPublish    *prometheus.CounterVec
	DeliveryAttempts *prometheus.CounterVec
	Dead             prometheus.Counter
	RetryScheduled   prometheus.Counter
	RecoveredLeases  prometheus.Counter
}

func NewMetrics() *Metrics {
	metrics := &Metrics{
		registry:         prometheus.NewRegistry(),
		Accepted:         prometheus.NewCounterVec(prometheus.CounterOpts{Name: "notifier_notifications_accepted_total", Help: "Accepted notification requests by bounded outcome."}, []string{"outcome"}),
		OutboxPublish:    prometheus.NewCounterVec(prometheus.CounterOpts{Name: "notifier_outbox_publish_total", Help: "Outbox publish attempts by bounded outcome."}, []string{"outcome"}),
		DeliveryAttempts: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "notifier_delivery_attempts_total", Help: "External delivery attempts by bounded outcome."}, []string{"outcome"}),
		Dead:             prometheus.NewCounter(prometheus.CounterOpts{Name: "notifier_dead_total", Help: "Notifications moved to dead state."}),
		RetryScheduled:   prometheus.NewCounter(prometheus.CounterOpts{Name: "notifier_retry_scheduled_total", Help: "Retry generations scheduled."}),
		RecoveredLeases:  prometheus.NewCounter(prometheus.CounterOpts{Name: "notifier_delivery_leases_recovered_total", Help: "Expired delivery leases recovered."}),
	}
	metrics.registry.MustRegister(metrics.Accepted, metrics.OutboxPublish, metrics.DeliveryAttempts, metrics.Dead, metrics.RetryScheduled, metrics.RecoveredLeases)
	return metrics
}

func (metrics *Metrics) ObserveIntake(outcome string) {
	metrics.Accepted.WithLabelValues(outcome).Inc()
}

func (metrics *Metrics) ObserveOutboxPublish(outcome string) {
	metrics.OutboxPublish.WithLabelValues(outcome).Inc()
}

func (metrics *Metrics) ObserveDelivery(outcome string) {
	metrics.DeliveryAttempts.WithLabelValues(outcome).Inc()
	if outcome == "dead" {
		metrics.Dead.Inc()
	}
}

func (metrics *Metrics) ObserveSchedule(result notification.ScheduleResult) {
	metrics.RetryScheduled.Add(float64(result.Scheduled))
	metrics.RecoveredLeases.Add(float64(result.RecoveredLeases))
	metrics.Dead.Add(float64(result.Dead))
}

func (metrics *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(metrics.registry, promhttp.HandlerOpts{})
}
