# Hardware Sim Lab

`hardware-sim-lab` is the hardware simulation repository extracted from
`observability-hub`.

It owns the simulated device fleet, chaos injection, and MQTT ingestion path used
to generate and observe synthetic hardware behavior. The observability platform can
consume this repo as an external workload while still using the shared MQTT,
OpenTelemetry, Prometheus, and Grafana environment.

## Scope

This repo owns:

- `sensor`: simulated hardware devices that publish telemetry over MQTT
- `chaos-controller`: failure injection for the sensor fleet
- `mqtt-ingestor`: MQTT consumer that validates messages and emits telemetry
- hardware-specific OpenTelemetry bootstrap and runtime metrics
- Docker image definitions for the hardware services
- `k3s/` manifests and overlays for deploying the hardware simulation workloads

This repo does not own:

- the shared LGTM stack
- central Grafana provisioning
- Argo CD bootstrap
- platform-level dashboards and operator workflows

## Layout

- `cmd/`: service entrypoints
- `internal/hardware-sim/`: sensor and chaos-controller implementation
- `internal/mqttingestor/`: payload validation, analysis, and runtime metrics
- `internal/telemetry/`: OpenTelemetry setup, logging, metrics, and tracing
- `docker/`: per-service Dockerfiles
- `k3s/`: base manifests and dev/prod overlays

## Services

### `sensor`

Publishes synthetic hardware telemetry to MQTT. It can emulate:

- thermal spikes
- signal loss
- brownouts
- memory leaks
- slow loops
- sleep mode
- malformed payloads
- sequence gaps

### `chaos-controller`

Discovers sensor pods in Kubernetes and publishes chaos commands to MQTT topics so
the fleet can simulate faults.

### `mqtt-ingestor`

Consumes hardware telemetry from MQTT, validates the payload contract, detects
staleness and sequence anomalies, and exports metrics, logs, and traces through
OpenTelemetry.

## Telemetry

This repo keeps its own `internal/telemetry` package instead of depending on
private code from `observability-hub`.

`mqtt-ingestor` exports telemetry through OpenTelemetry. Those metrics feed the
shared Prometheus/Grafana dashboards used by the platform.

The services are expected to export to the shared OpenTelemetry endpoint using:

- `OTEL_EXPORTER_OTLP_ENDPOINT`

Current Kubernetes manifests point to:

- `opentelemetry.observability.svc.cluster.local:4317`

As long as metric names, labels, and resource attributes remain stable, existing
Grafana dashboards can continue to work even though the code now lives in a
separate repository.

## Deployment

The `k3s/` tree contains:

- `base/`: hardware workloads and hardware-specific RBAC
- `overlays/dev/`: development overlay
- `overlays/prod/`: production-style overlay

Both overlays currently use the `canary` image tag.

Argo CD is not wired to this repo yet in this staging copy. The intended model is:

- `observability-hub` remains the GitOps root
- Argo CD manages this repo through a child `Application`
- that `Application` points at one of this repo's `k3s/overlays/...` paths

## CI/CD

GitHub Actions in `.github/workflows/` provide:

- Go formatting, vet, and test checks
- Docker image builds for `sensor`, `chaos-controller`, and `mqtt-ingestor`
- GHCR push on `main` and version tags
- Dependabot updates for Go modules and GitHub Actions

## Current Status

This directory is still a staging extraction inside the monorepo. Remaining
follow-up work likely includes:

- generating `go.sum`
- moving tests into this repo
- validating `go test ./...`
- validating Docker builds
- wiring Argo CD to the new repo
- removing the old hardware-owned files from `observability-hub`
