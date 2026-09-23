package destination

import (
	"bytes"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/jiahuipaung/rc_pangjiahui/internal/notification"
)

func bytesReader(data []byte) *bytes.Reader { return bytes.NewReader(data) }

func parseDestination(raw rawDestination, lookupEnv func(string) (string, bool)) (Destination, error) {
	if strings.TrimSpace(raw.ID) == "" {
		return Destination{}, fmt.Errorf("id is required")
	}
	method := strings.ToUpper(raw.Method)
	if method != http.MethodPost && method != http.MethodPut && method != http.MethodPatch {
		return Destination{}, fmt.Errorf("unsupported method %q", raw.Method)
	}
	parsedURL, err := url.Parse(raw.URL)
	if err != nil || parsedURL.Hostname() == "" {
		return Destination{}, fmt.Errorf("invalid URL")
	}
	if parsedURL.Scheme != "https" {
		return Destination{}, fmt.Errorf("production destination must use HTTPS")
	}
	if parsedURL.User != nil || parsedURL.Fragment != "" {
		return Destination{}, fmt.Errorf("URL userinfo and fragments are not allowed")
	}
	if ip := net.ParseIP(parsedURL.Hostname()); ip != nil && disallowedIP(ip) {
		return Destination{}, fmt.Errorf("disallowed destination network")
	}
	timeout, err := time.ParseDuration(raw.Timeout)
	if err != nil || timeout <= 0 || timeout > 30*time.Second {
		return Destination{}, fmt.Errorf("timeout must be between 1ns and 30s")
	}
	lifetime, err := time.ParseDuration(raw.Lifetime)
	if err != nil || lifetime <= 0 || lifetime > 7*24*time.Hour {
		return Destination{}, fmt.Errorf("lifetime must be between 1ns and 168h")
	}
	if raw.MaxAttempts < 1 || raw.MaxAttempts > 32 {
		return Destination{}, fmt.Errorf("max_attempts must be between 1 and 32")
	}
	if raw.ConcurrencyLimit < 1 || raw.ConcurrencyLimit > 1000 {
		return Destination{}, fmt.Errorf("concurrency_limit must be between 1 and 1000")
	}
	delays := make([]time.Duration, len(raw.RetryDelays))
	for index, value := range raw.RetryDelays {
		delay, parseErr := time.ParseDuration(value)
		if parseErr != nil || delay <= 0 || delay > lifetime {
			return Destination{}, fmt.Errorf("invalid retry delay %q", value)
		}
		delays[index] = delay
	}
	if raw.MaxAttempts > 1 && len(delays) == 0 {
		return Destination{}, fmt.Errorf("retry_delays are required when max_attempts exceeds one")
	}
	staticHeaders := make(http.Header, len(raw.StaticHeaders))
	for name, value := range raw.StaticHeaders {
		if strings.EqualFold(name, "Authorization") || strings.ContainsAny(value, "\r\n") {
			return Destination{}, fmt.Errorf("unsafe static header %q", name)
		}
		staticHeaders.Set(name, value)
	}
	secretHeaders := make(map[string]string, len(raw.SecretHeaders))
	for name, environmentName := range raw.SecretHeaders {
		if environmentName == "" || strings.ContainsAny(name, "\r\n") {
			return Destination{}, fmt.Errorf("invalid secret header reference")
		}
		if _, ok := lookupEnv(environmentName); !ok {
			return Destination{}, fmt.Errorf("missing environment variable %q", environmentName)
		}
		secretHeaders[http.CanonicalHeaderKey(name)] = environmentName
	}
	return Destination{
		ID:               raw.ID,
		Method:           method,
		URL:              parsedURL,
		StaticHeaders:    staticHeaders,
		SecretHeaders:    secretHeaders,
		Timeout:          timeout,
		Retry:            notification.RetryPolicy{MaxAttempts: raw.MaxAttempts, Lifetime: lifetime, Delays: delays},
		ConcurrencyLimit: raw.ConcurrencyLimit,
	}, nil
}

func disallowedIP(ip net.IP) bool {
	return ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsMulticast() || ip.IsUnspecified()
}
