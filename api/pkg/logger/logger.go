package logger

import "context"

type FieldType int

const (
	KindString FieldType = iota
	KindInt
	KindFloat64
	KindError
	KindAny
)

type Field struct {
	Key   string
	Value any
	Kind  FieldType
}

func String(k, v string) Field          { return Field{Key: k, Value: v, Kind: KindString} }
func Int(k string, v int) Field         { return Field{Key: k, Value: v, Kind: KindInt} }
func Float64(k string, v float64) Field { return Field{Key: k, Value: v, Kind: KindFloat64} }
func Any(k string, v any) Field         { return Field{Key: k, Value: v, Kind: KindAny} }
func WithError(err error) Field         { return Field{Key: "error", Value: err, Kind: KindError} }
func Lazy(k string, f func() any) Field { return Field{Key: k, Value: f, Kind: KindAny} }

type Logger interface {
	Debug(ctx context.Context, msg string, fields ...Field)
	Info(ctx context.Context, msg string, fields ...Field)
	Warn(ctx context.Context, msg string, fields ...Field)
	Error(ctx context.Context, msg string, fields ...Field)
	With(fields ...Field) Logger
}
