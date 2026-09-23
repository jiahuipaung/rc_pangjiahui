package observability

import (
	"context"
	"log/slog"
	"strings"
)

type redactingHandler struct{ next slog.Handler }

func NewRedactingHandler(next slog.Handler) slog.Handler { return redactingHandler{next: next} }
func (handler redactingHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return handler.next.Enabled(ctx, level)
}
func (handler redactingHandler) Handle(ctx context.Context, record slog.Record) error {
	copy := slog.NewRecord(record.Time, record.Level, record.Message, record.PC)
	record.Attrs(func(attribute slog.Attr) bool { copy.AddAttrs(redact(attribute)); return true })
	return handler.next.Handle(ctx, copy)
}
func (handler redactingHandler) WithAttrs(attributes []slog.Attr) slog.Handler {
	redacted := make([]slog.Attr, len(attributes))
	for i, attribute := range attributes {
		redacted[i] = redact(attribute)
	}
	return redactingHandler{next: handler.next.WithAttrs(redacted)}
}
func (handler redactingHandler) WithGroup(name string) slog.Handler {
	return redactingHandler{next: handler.next.WithGroup(name)}
}
func redact(attribute slog.Attr) slog.Attr {
	key := strings.ToLower(attribute.Key)
	for _, fragment := range []string{"authorization", "token", "secret", "password", "body", "payload"} {
		if strings.Contains(key, fragment) {
			return slog.String(attribute.Key, "[REDACTED]")
		}
	}
	if attribute.Value.Kind() == slog.KindGroup {
		children := attribute.Value.Group()
		for i := range children {
			children[i] = redact(children[i])
		}
		return slog.Group(attribute.Key, childrenToAny(children)...)
	}
	return attribute
}
func childrenToAny(attributes []slog.Attr) []any {
	values := make([]any, len(attributes))
	for i := range attributes {
		values[i] = attributes[i]
	}
	return values
}
