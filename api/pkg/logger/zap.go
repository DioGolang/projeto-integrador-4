package logger

import (
	"context"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
)

type zapLogger struct {
	log *zap.Logger
}

func NewZapLogger(serviceName string, isProd bool) Logger {
	var config zapcore.EncoderConfig
	var level zapcore.Level

	if isProd {
		config = zap.NewProductionEncoderConfig()
		level = zapcore.InfoLevel
	} else {
		config = zap.NewDevelopmentEncoderConfig()
		config.EncodeLevel = zapcore.CapitalColorLevelEncoder
		level = zapcore.DebugLevel
	}
	config.EncodeTime = zapcore.ISO8601TimeEncoder
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(config),
		zapcore.AddSync(os.Stdout),
		level,
	)
	if isProd {
		core = zapcore.NewSamplerWithOptions(core, 1, 100, 0)
	}
	l := zap.New(core).With(zap.String("service", serviceName))
	return &zapLogger{log: l}
}

// Logging methods with Level Check for Performance

func (z *zapLogger) Info(ctx context.Context, msg string, fields ...Field) {
	if z.log.Core().Enabled(zap.InfoLevel) {
		z.log.Info(msg, z.enrich(ctx, fields)...)
	}
}

func (z *zapLogger) Debug(ctx context.Context, msg string, fields ...Field) {
	if z.log.Core().Enabled(zap.DebugLevel) {
		z.log.Debug(msg, z.enrich(ctx, fields)...)
	}
}

func (z *zapLogger) Warn(ctx context.Context, msg string, fields ...Field) {
	if z.log.Core().Enabled(zap.WarnLevel) {
		z.log.Warn(msg, z.enrich(ctx, fields)...)
	}
}

func (z *zapLogger) Error(ctx context.Context, msg string, fields ...Field) {
	if z.log.Core().Enabled(zap.ErrorLevel) {
		z.log.Error(msg, z.enrich(ctx, fields)...)
	}
}

func (z *zapLogger) With(fields ...Field) Logger {
	return &zapLogger{log: z.log.With(z.convertFields(fields)...)}
}

// enrichment com OpenTelemetry
func (z *zapLogger) enrich(ctx context.Context, fields []Field) []zap.Field {
	zapFields := make([]zap.Field, 0, len(fields)+2)
	zapFields = append(zapFields, z.convertFields(fields)...)

	span := trace.SpanFromContext(ctx)
	if span.SpanContext().IsValid() {
		zapFields = append(zapFields,
			zap.String("trace_id", span.SpanContext().TraceID().String()),
			zap.String("span_id", span.SpanContext().SpanID().String()),
		)
	}
	return zapFields
}

func (z *zapLogger) convertFields(fields []Field) []zap.Field {
	if len(fields) == 0 {
		return nil
	}
	out := make([]zap.Field, len(fields))
	for i, f := range fields {
		val := f.Value
		if fn, ok := f.Value.(func() any); ok {
			val = fn()
		}
		switch f.Kind {
		case KindString:
			if v, ok := val.(string); ok {
				out[i] = zap.String(f.Key, v)
				continue
			}
		case KindInt:
			if v, ok := val.(int); ok {
				out[i] = zap.Int(f.Key, v)
				continue
			}
		case KindFloat64:
			if v, ok := val.(float64); ok {
				out[i] = zap.Float64(f.Key, v)
				continue
			}
		case KindError:
			if v, ok := val.(error); ok {
				out[i] = zap.Error(v)
				continue
			}
		case KindAny:
			out[i] = zap.Any(f.Key, val)
			continue

		default:
			out[i] = zap.Any(f.Key, val)
			continue
		}
		// Security fallback if the above type assertions fail
		// (ex: KindString passed with an Int value)
		out[i] = zap.Any(f.Key, val)
	}
	return out
}
