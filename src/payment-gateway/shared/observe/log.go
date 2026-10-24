package observe

import (
	"context"
	"io"
	"log/slog"
	"strings"
)

type logOperation struct {
	group string
	attrs []slog.Attr
}
type logHandler struct {
	slog.Handler
	operations []logOperation
}

func NewLogHandler(w io.Writer, service string) slog.Handler {
	return logHandler{Handler: slog.NewJSONHandler(w, &slog.HandlerOptions{ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
		key := strings.ToLower(a.Key)
		if key == "pan" {
			return slog.Attr{}
		}
		if key == slog.LevelKey {
			return slog.String(key, strings.ToLower(a.Value.String()))
		}
		for _, field := range append(append([]string(nil), groups...), key) {
			field = strings.ToLower(field)
			for _, sensitive := range []string{"authorization", "cookie", "token", "secret", "password", "card", "fingerprint", "payload", "body", "headers", "query", "signature", "cvv", "url", "api_key", "apikey"} {
				if strings.Contains(field, sensitive) {
					return slog.Attr{}
				}
			}
		}
		return a
	}}).WithAttrs([]slog.Attr{slog.String("service", service)})}
}

func (h logHandler) Handle(ctx context.Context, record slog.Record) error {
	handler := h.Handler
	if traceID, spanID := SpanIDs(ctx); traceID != "" {
		handler = handler.WithAttrs([]slog.Attr{slog.String("trace_id", traceID), slog.String("span_id", spanID)})
	}
	for _, op := range h.operations {
		if op.group != "" {
			handler = handler.WithGroup(op.group)
		} else {
			handler = handler.WithAttrs(op.attrs)
		}
	}
	return handler.Handle(ctx, record)
}

func (h logHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	operations := append([]logOperation(nil), h.operations...)
	return logHandler{Handler: h.Handler, operations: append(operations, logOperation{attrs: append([]slog.Attr(nil), attrs...)})}
}

func (h logHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	operations := append([]logOperation(nil), h.operations...)
	return logHandler{Handler: h.Handler, operations: append(operations, logOperation{group: name})}
}
