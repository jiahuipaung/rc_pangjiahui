package observability

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	registry         *prometheus.Registry
	Accepted         *prometheus.CounterVec
	DeliveryAttempts *prometheus.CounterVec
	Dead             prometheus.Counter
}

func NewMetrics() *Metrics {
	metrics := &Metrics{
		registry:         prometheus.NewRegistry(),
		Accepted:         prometheus.NewCounterVec(prometheus.CounterOpts{Name: "notifier_notifications_accepted_total", Help: "Accepted notification requests by bounded outcome."}, []string{"outcome"}),
		DeliveryAttempts: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "notifier_delivery_attempts_total", Help: "External delivery attempts by bounded outcome."}, []string{"outcome"}),
		Dead:             prometheus.NewCounter(prometheus.CounterOpts{Name: "notifier_dead_total", Help: "Notifications moved to dead state."}),
	}
	metrics.registry.MustRegister(metrics.Accepted, metrics.DeliveryAttempts, metrics.Dead)
	return metrics
}

func (metrics *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(metrics.registry, promhttp.HandlerOpts{})
}
