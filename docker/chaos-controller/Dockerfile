# Stage 1: Build
FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY cmd/chaos-controller/ cmd/chaos-controller/
COPY internal/hardware-sim/ internal/hardware-sim/

RUN CGO_ENABLED=0 GOOS=linux go build -o chaos-controller ./cmd/chaos-controller

FROM alpine:3.20

RUN apk add --no-cache ca-certificates \
    && addgroup -S app -g 1000 \
    && adduser -S app -G app -u 1000

WORKDIR /app
COPY --from=builder /app/chaos-controller .

USER 1000:1000

ENTRYPOINT ["./chaos-controller"]
