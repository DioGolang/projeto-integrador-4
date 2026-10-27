FROM golang:1.26.7-alpine AS builder

ENV CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64

WORKDIR /app

RUN apk add --no-cache ca-certificates tzdata

RUN adduser -D -g '' appuser

COPY go.mod go.sum ./
RUN go mod download && go mod verify

COPY . .

RUN go build -ldflags="-s -w" -o /go/bin/logistics-api ./cmd/server

# ==========================================
# Estágio 2: Runner (Distroless/Scratch)
# ==========================================
FROM scratch

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=builder /etc/passwd /etc/passwd
COPY --from=builder /etc/group /etc/group
COPY --from=builder /go/bin/logistics-api /usr/local/bin/logistics-api

USER appuser:appuser

EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/logistics-api"]
