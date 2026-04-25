package telemetry

import (
	"context"
	"log/slog"
	"strings"
)

var SensitiveKeys = []string{
	"password",
	"secret",
	"token",
	"api_key",
	"email",
	"phone",
	"address",
	"authorization",
	"cookie",
	"set-cookie",
}

func MaskPII(groups []string, a slog.Attr) slog.Attr {
	if a.Value.Kind() == slog.KindGroup {
		nested := a.Value.Group()
		masked := make([]any, len(nested))
		for i, ga := range nested {
			masked[i] = MaskPII(groups, ga)
		}
		return slog.Group(a.Key, masked...)
	}
	for _, key := range SensitiveKeys {
		if strings.EqualFold(a.Key, key) {
			return slog.String(a.Key, "[REDACTED]")
		}
	}
	return a
}

type PIIHandler struct {
	slog.Handler
}

func NewPIIHandler(h slog.Handler) slog.Handler {
	return &PIIHandler{h}
}

func (h *PIIHandler) Handle(ctx context.Context, r slog.Record) error {
	newRecord := slog.NewRecord(r.Time, r.Level, r.Message, r.PC)
	r.Attrs(func(a slog.Attr) bool {
		newRecord.AddAttrs(MaskPII(nil, a))
		return true
	})
	return h.Handler.Handle(ctx, newRecord)
}

func (h *PIIHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	maskedAttrs := make([]slog.Attr, len(attrs))
	for i, a := range attrs {
		maskedAttrs[i] = MaskPII(nil, a)
	}
	return &PIIHandler{h.Handler.WithAttrs(maskedAttrs)}
}

func (h *PIIHandler) WithGroup(name string) slog.Handler {
	return &PIIHandler{h.Handler.WithGroup(name)}
}
