FROM golang:1.26.7-alpine AS builder

WORKDIR /app

# Copy the entire workspace to resolve local dependencies in go.work
COPY ./api ./api
COPY ./simulator ./simulator
COPY ./go.work .

WORKDIR /app/simulator

RUN CGO_ENABLED=0 GOOS=linux go build -o simulator .

FROM alpine:latest
WORKDIR /root/
COPY --from=builder /app/simulator/simulator .

CMD ["./simulator"]
