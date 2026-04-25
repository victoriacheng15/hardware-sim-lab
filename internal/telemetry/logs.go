package telemetry

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutlog"
	otellog "go.opentelemetry.io/otel/log"
	otellogglobal "go.opentelemetry.io/otel/log/global"
	nooplog "go.opentelemetry.io/otel/log/noop"
	noopmetric "go.opentelemetry.io/otel/metric/noop"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"

	"google.golang.org/grpc"
)

func GetLogger(name string) otellog.Logger {
	if name == "" {
		name = ScopeName
	}
	return otellogglobal.GetLoggerProvider().Logger(name)
}

func Info(msg string, args ...any) {
	slog.Info(msg, args...)
}

func Warn(msg string, args ...any) {
	slog.Warn(msg, args...)
}

func Error(msg string, args ...any) {
	slog.Error(msg, args...)
}

type MultiHandler struct {
	Handlers []slog.Handler
}

func (h *MultiHandler) Enabled(ctx context.Context, l slog.Level) bool {
	for _, h := range h.Handlers {
		if h.Enabled(ctx, l) {
			return true
		}
	}
	return false
}

func (h *MultiHandler) Handle(ctx context.Context, r slog.Record) error {
	for _, h := range h.Handlers {
		if err := h.Handle(ctx, r); err != nil {
			return err
		}
	}
	return nil
}

func (h *MultiHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	newHandlers := make([]slog.Handler, len(h.Handlers))
	for i, h := range h.Handlers {
		newHandlers[i] = h.WithAttrs(attrs)
	}
	return &MultiHandler{Handlers: newHandlers}
}

func (h *MultiHandler) WithGroup(name string) slog.Handler {
	newHandlers := make([]slog.Handler, len(h.Handlers))
	for i, h := range h.Handlers {
		newHandlers[i] = h.WithGroup(name)
	}
	return &MultiHandler{Handlers: newHandlers}
}

func initLogs(ctx context.Context, conn *grpc.ClientConn, res *resource.Resource) (func(context.Context) error, error) {
	logExporter, err := otlploggrpc.New(ctx, otlploggrpc.WithGRPCConn(conn))
	if err != nil {
		return nil, fmt.Errorf("failed to create log exporter: %w", err)
	}

	consoleExporter, err := stdoutlog.New(stdoutlog.WithWriter(os.Stderr))
	if err != nil {
		return nil, fmt.Errorf("failed to create stdout log exporter: %w", err)
	}

	lp := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(sdklog.NewBatchProcessor(logExporter)),
		sdklog.WithProcessor(sdklog.NewSimpleProcessor(consoleExporter)),
		sdklog.WithResource(res),
	)
	otellogglobal.SetLoggerProvider(lp)

	otelHandler := otelslog.NewHandler(ScopeName, otelslog.WithLoggerProvider(lp))
	jsonHandler := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		ReplaceAttr: MaskPII,
	})
	multiHandler := &MultiHandler{
		Handlers: []slog.Handler{
			otelHandler,
			jsonHandler,
		},
	}

	slog.SetDefault(slog.New(NewPIIHandler(multiHandler)))
	return lp.Shutdown, nil
}

func SilenceLogs() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.NewFile(0, os.DevNull), nil)))
	otellogglobal.SetLoggerProvider(nooplog.NewLoggerProvider())
	otel.SetMeterProvider(noopmetric.NewMeterProvider())
}
