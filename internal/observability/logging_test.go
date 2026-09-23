package observability

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

func TestRedactingHandlerRemovesSensitiveAttributes(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(NewRedactingHandler(slog.NewJSONHandler(&output, nil)))
	logger.Info("delivery", "notification_id", "n-1", "authorization", "Bearer secret", "body", `{"password":"secret"}`)
	text := output.String()
	if strings.Contains(text, "Bearer secret") || strings.Contains(text, "password") {
		t.Fatalf("sensitive log: %s", text)
	}
	if !strings.Contains(text, "notification_id") {
		t.Fatalf("missing safe context: %s", text)
	}
}
