# Hardware Sim Lab

`hardware-sim-lab` is the hardware simulator repository extracted from
`observability-hub`.

This repo focuses on simulated hardware workloads: sensor telemetry, chaos
injection, MQTT ingestion, and hardware-specific observability signals. The
larger observability platform still owns the shared MQTT, OpenTelemetry,
Prometheus, Grafana, GitOps, and dashboard infrastructure.

[Full Documentation](./docs/README.md)

## Scope

This repo owns:

- simulated sensor workloads
- sensor chaos injection
- MQTT ingestion for hardware telemetry
- hardware-specific telemetry processing and metrics
- Docker and Kubernetes manifests for the hardware simulator services

This repo does not own:

- the shared LGTM stack
- central Grafana provisioning
- Argo CD bootstrap
- platform-level dashboards and operator workflows

## Documentation

- [Architecture](./docs/architecture.md)
- [MQTT checks](./docs/mqtt-checks.md)
- [Deployment](./docs/deployment.md)
