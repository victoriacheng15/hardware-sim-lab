# Documentation

These docs separate the hardware simulator concerns from the top-level README.
Use this index to find the right document for architecture, MQTT verification,
or deployment flow.

The hardware simulator was extracted from `observability-hub`, so the docs focus
on what this repo owns: simulated device behavior, MQTT ingestion, digital twin
signals, Kubernetes manifests, and the CI/CD path that builds deployable service
images.

## Documentation Domains

| Domain | Description |
| :--- | :--- |
| [Architecture](architecture.md) | Service responsibilities, telemetry boundary, and digital twin data flow. |
| [MQTT checks](mqtt-checks.md) | MQTT publish commands, subscription checks, ingestor logs, and metric checks. |
| [Deployment](deployment.md) | Kubernetes layout, CI checks, security scans, Docker image builds, and CD flow. |
