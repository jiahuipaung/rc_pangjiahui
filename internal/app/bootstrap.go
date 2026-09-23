package app

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jiahuipaung/rc_pangjiahui/internal/config"
	"github.com/jiahuipaung/rc_pangjiahui/internal/delivery"
	"github.com/jiahuipaung/rc_pangjiahui/internal/destination"
	"github.com/jiahuipaung/rc_pangjiahui/internal/intake"
	broker "github.com/jiahuipaung/rc_pangjiahui/internal/messaging/rabbitmq"
	"github.com/jiahuipaung/rc_pangjiahui/internal/notification"
	"github.com/jiahuipaung/rc_pangjiahui/internal/observability"
	"github.com/jiahuipaung/rc_pangjiahui/internal/outbox"
	"github.com/jiahuipaung/rc_pangjiahui/internal/retention"
	retrysvc "github.com/jiahuipaung/rc_pangjiahui/internal/retry"
	"github.com/jiahuipaung/rc_pangjiahui/internal/store/postgres"
	"github.com/jiahuipaung/rc_pangjiahui/internal/transport/httpapi"
)

func RunConfigured(ctx context.Context, cfg config.Runtime) error {
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("open postgres: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping postgres: %w", err)
	}
	store := postgres.New(pool)
	now := func() time.Time { return time.Now().UTC() }
	var registry destination.Registry
	if cfg.Role == config.RoleAPI || cfg.Role == config.RoleWorker || cfg.Role == config.RoleAll {
		registry, err = destination.Load(cfg.DestinationsFile, os.LookupEnv)
		if err != nil {
			return err
		}
	}
	topology := broker.Topology{Exchange: "notifications", Queue: "notifications", RoutingKey: "notification.ready"}
	var publisherRabbit *broker.Client
	if cfg.Role == config.RolePublisher || cfg.Role == config.RoleAll {
		publisherRabbit, err = broker.Open(cfg.RabbitMQURL, topology)
		if err != nil {
			return err
		}
		defer func() { _ = publisherRabbit.Close() }()
	}
	var workerRabbit *broker.Client
	if cfg.Role == config.RoleWorker || cfg.Role == config.RoleAll {
		workerRabbit, err = broker.Open(cfg.RabbitMQURL, topology)
		if err != nil {
			return err
		}
		defer func() { _ = workerRabbit.Close() }()
	}
	errChannel := make(chan error, 5)
	if cfg.Role == config.RoleAPI || cfg.Role == config.RoleAll {
		intakeService := intake.New(store, registry, now, newID)
		api := httpapi.NewRouter(httpapi.Dependencies{Creator: intakeService, Querier: notification.NewQueryService(store), Replayer: notification.NewReplayService(store, now, newID), CallerTokens: cfg.CallerTokens, AdminTokens: cfg.AdminTokens})
		metrics := observability.NewMetrics()
		mux := http.NewServeMux()
		mux.Handle("/v1/", api)
		checks := observability.Checks{Postgres: postgresHealthCheck(pool), Destinations: func() error { return nil }}
		if publisherRabbit != nil {
			checks.RabbitMQ = publisherRabbit.Healthy
		} else if workerRabbit != nil {
			checks.RabbitMQ = workerRabbit.Healthy
		}
		mux.Handle("/livez", observability.NewHealthHandler(string(cfg.Role), checks))
		mux.Handle("/readyz", observability.NewHealthHandler(string(cfg.Role), checks))
		mux.Handle("/metrics", metrics.Handler())
		server := &http.Server{Addr: cfg.HTTPAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second}
		go func() {
			err := server.ListenAndServe()
			if !errors.Is(err, http.ErrServerClosed) {
				errChannel <- err
			}
		}()
		defer func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
			defer cancel()
			_ = server.Shutdown(shutdownCtx)
		}()
	}
	if cfg.Role == config.RolePublisher || cfg.Role == config.RoleAll {
		service := outbox.NewService(store, publisherRabbit, now, newID, outbox.Config{BatchSize: 100, ClaimTTL: time.Minute, PollInterval: 250 * time.Millisecond})
		go func() { errChannel <- service.Run(ctx) }()
	}
	if cfg.Role == config.RoleWorker || cfg.Role == config.RoleAll {
		sender := delivery.NewHTTPSender(delivery.SenderConfig{MaxDiagnosticBytes: 1024}, os.LookupEnv)
		worker := delivery.NewService(store, sender, now, newID, time.Minute)
		deliveries, consumeErr := workerRabbit.Consume(ctx, "notifier-worker")
		if consumeErr != nil {
			return consumeErr
		}
		go func() {
			for message := range deliveries {
				disposition := worker.Handle(ctx, delivery.Message{EventID: message.Message.EventID, NotificationID: message.Message.NotificationID})
				switch disposition {
				case delivery.Ack:
					_ = message.Ack()
				case delivery.Requeue:
					_ = message.Nack(true)
				default:
					_ = message.Nack(false)
				}
			}
			errChannel <- ctx.Err()
		}()
	}
	if cfg.Role == config.RoleScheduler || cfg.Role == config.RoleAll {
		scheduler := retrysvc.NewService(store, now, retrysvc.Config{BatchSize: 100, PollInterval: time.Second})
		go func() { errChannel <- scheduler.Run(ctx) }()
		cleaner := retention.NewService(store, now, retention.Config{Retention: 30 * 24 * time.Hour, BatchSize: 500})
		go func() {
			ticker := time.NewTicker(time.Hour)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					errChannel <- ctx.Err()
					return
				case <-ticker.C:
					_, _ = cleaner.RunOnce(ctx)
				}
			}
		}()
	}
	select {
	case <-ctx.Done():
		return nil
	case runErr := <-errChannel:
		if errors.Is(runErr, context.Canceled) {
			return nil
		}
		return runErr
	}
}

func postgresHealthCheck(pool *pgxpool.Pool) observability.Check {
	return func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		return pool.Ping(ctx)
	}
}

func newID() string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		panic("secure random source unavailable")
	}
	return hex.EncodeToString(value[:])
}
