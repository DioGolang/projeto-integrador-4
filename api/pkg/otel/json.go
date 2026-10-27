package otel

import (
	"context"
	"encoding/json"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

func ExtractContextToJSON(ctx context.Context) []byte {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)

	b, err := json.Marshal(carrier)
	if err != nil {
		return []byte("{}")
	}
	return b
}

func InjectContextFromJSON(parentCtx context.Context, data []byte) context.Context {
	if len(data) == 0 {
		return parentCtx
	}

	carrier := propagation.MapCarrier{}
	if err := json.Unmarshal(data, &carrier); err != nil {
		return parentCtx
	}

	return otel.GetTextMapPropagator().Extract(parentCtx, carrier)
}
