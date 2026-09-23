package delivery

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/jiahuipaung/rc_pangjiahui/internal/destination"
	"github.com/jiahuipaung/rc_pangjiahui/internal/notification"
)

var errDestinationNotAllowed = errors.New("destination not allowed")
var errRedirectNotAllowed = errors.New("redirect not allowed")

type Result struct {
	Outcome      notification.Outcome
	HTTPStatus   int
	ErrorCode    string
	ErrorMessage string
	RetryAfter   *time.Time
}

type SenderConfig struct {
	AllowPrivateNetworks bool
	MaxDiagnosticBytes   int64
}

type HTTPSender struct {
	client    *http.Client
	lookupEnv func(string) (string, bool)
	limit     int64
	allowDev  bool
}

func NewHTTPSender(config SenderConfig, lookupEnv func(string) (string, bool)) *HTTPSender {
	limit := config.MaxDiagnosticBytes
	if limit <= 0 {
		limit = 1024
	}
	sender := &HTTPSender{lookupEnv: lookupEnv, limit: limit, allowDev: config.AllowPrivateNetworks}
	transport := &http.Transport{Proxy: http.ProxyFromEnvironment, MaxIdleConns: 100, MaxIdleConnsPerHost: 10, IdleConnTimeout: 90 * time.Second, TLSHandshakeTimeout: 5 * time.Second, ResponseHeaderTimeout: 10 * time.Second}
	dialer := &net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, fmt.Errorf("split destination address: %w", err)
		}
		addresses, err := net.DefaultResolver.LookupIP(ctx, "ip", host)
		if err != nil {
			return nil, fmt.Errorf("resolve destination: %w", err)
		}
		if !config.AllowPrivateNetworks && destination.ValidateResolvedIPs(addresses) != nil {
			return nil, errDestinationNotAllowed
		}
		return dialer.DialContext(ctx, network, net.JoinHostPort(addresses[0].String(), port))
	}
	sender.client = &http.Client{Transport: transport, CheckRedirect: func(request *http.Request, via []*http.Request) error {
		if len(via) == 0 || !strings.EqualFold(request.URL.Scheme, via[0].URL.Scheme) || !strings.EqualFold(request.URL.Host, via[0].URL.Host) || len(via) >= 3 {
			return errRedirectNotAllowed
		}
		return nil
	}}
	return sender
}

func (sender *HTTPSender) Send(parent context.Context, snapshot notification.DeliverySnapshot) Result {
	parsed, err := url.Parse(snapshot.URL)
	if err != nil || parsed.Hostname() == "" || (parsed.Scheme != "https" && !sender.allowDev) {
		return Result{Outcome: notification.Permanent, ErrorCode: "destination_not_allowed"}
	}
	if ip := net.ParseIP(parsed.Hostname()); ip != nil && !sender.allowDev && destination.ValidateResolvedIPs([]net.IP{ip}) != nil {
		return Result{Outcome: notification.Permanent, ErrorCode: "destination_not_allowed"}
	}
	ctx, cancel := context.WithTimeout(parent, snapshot.Timeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, snapshot.Method, snapshot.URL, bytes.NewReader(snapshot.Body))
	if err != nil {
		return Result{Outcome: notification.Permanent, ErrorCode: "invalid_request"}
	}
	request.Header = snapshot.StaticHeaders.Clone()
	for name, environmentName := range snapshot.SecretHeaders {
		value, ok := sender.lookupEnv(environmentName)
		if !ok {
			return Result{Outcome: notification.Permanent, ErrorCode: "missing_secret"}
		}
		request.Header.Set(name, value)
	}
	request.Header.Set("Idempotency-Key", snapshot.IdempotencyKey)
	response, err := sender.client.Do(request)
	if err != nil {
		if errors.Is(err, errRedirectNotAllowed) {
			return Result{Outcome: notification.Permanent, ErrorCode: "redirect_not_allowed"}
		}
		if errors.Is(err, errDestinationNotAllowed) {
			return Result{Outcome: notification.Permanent, ErrorCode: "destination_not_allowed"}
		}
		return Result{Outcome: notification.Retryable, ErrorCode: "transport_error", ErrorMessage: sanitize(err.Error(), sender.limit)}
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(response.Body, sender.limit))
	outcome := notification.ClassifyResult(response.StatusCode, nil)
	result := Result{Outcome: outcome, HTTPStatus: response.StatusCode, ErrorMessage: sanitize(string(body), sender.limit)}
	if outcome == notification.Delivered {
		return result
	}
	if response.StatusCode >= 500 {
		result.ErrorCode = "http_5xx"
	} else {
		result.ErrorCode = "http_4xx"
	}
	if response.StatusCode == http.StatusTooManyRequests || response.StatusCode == http.StatusServiceUnavailable {
		result.RetryAfter = parseRetryAfter(response.Header.Get("Retry-After"), time.Now())
	}
	return result
}

func parseRetryAfter(value string, now time.Time) *time.Time {
	if seconds, err := strconv.Atoi(strings.TrimSpace(value)); err == nil && seconds >= 0 {
		result := now.Add(time.Duration(seconds) * time.Second)
		return &result
	}
	if parsed, err := http.ParseTime(value); err == nil && parsed.After(now) {
		return &parsed
	}
	return nil
}

func sanitize(value string, limit int64) string {
	value = strings.Map(func(r rune) rune {
		if r < 0x20 && r != '\t' {
			return ' '
		}
		return r
	}, value)
	if int64(len(value)) > limit {
		value = value[:limit]
	}
	return value
}
