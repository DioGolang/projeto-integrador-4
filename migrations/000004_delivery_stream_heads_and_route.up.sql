-- 1. Autoridade de escrita de sequência causal por agregado
CREATE TABLE delivery_stream_heads (
    delivery_id UUID PRIMARY KEY,
    sequence BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 2. Tabela de rotas e idempotência de revisões por entrega
CREATE TABLE delivery_routes (
    delivery_id UUID NOT NULL,
    revision BIGINT NOT NULL,
    route_id TEXT NOT NULL,
    leg TEXT NOT NULL,
    sequence BIGINT NOT NULL DEFAULT 0,
    route_payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (delivery_id, revision)
);

-- 3. Adicionar coluna active_route à projeção de leitura
ALTER TABLE delivery_read_model
ADD COLUMN IF NOT EXISTS active_route JSONB;
