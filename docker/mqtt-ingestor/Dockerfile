# Stage 1: Build
FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY cmd/mqtt-ingestor/ cmd/mqtt-ingestor/
COPY internal/mqttingestor/ internal/mqttingestor/
COPY internal/telemetry/ internal/telemetry/

RUN CGO_ENABLED=0 GOOS=linux go build -o mqtt-ingestor ./cmd/mqtt-ingestor

FROM alpine:3.20

RUN apk add --no-cache ca-certificates \
    && addgroup -S app -g 1000 \
    && adduser -S app -G app -u 1000

WORKDIR /app
COPY --from=builder /app/mqtt-ingestor .

USER 1000:1000

ENTRYPOINT ["./mqtt-ingestor"]
