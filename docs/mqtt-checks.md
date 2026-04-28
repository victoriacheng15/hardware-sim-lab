# MQTT Checks

Use these checks to publish a shadow update and confirm that the MQTT ingestor
receives it.

The examples assume Kubernetes DNS can resolve
`emqx.observability.svc.cluster.local`. Run them from inside the cluster, or use
port forwarding if running from your host.

## Publish A Shadow Update

Use a temporary pod with the Mosquitto client:

```bash
kubectl run mqtt-client --rm -it --image=eclipse-mosquitto --restart=Never -- \
  mosquitto_pub \
  -h emqx.observability.svc.cluster.local \
  -p 1883 \
  -t devices/sensor-1/shadow/update \
  -m '{"desired":{"temperature":36.0,"voltage":3.3,"device_state":"running","firmware_version":"1.0.1"}}'
```

`mosquitto_pub` exits silently on success. It does not print the message.

## Watch The Shadow Topic

Run a subscriber in one terminal:

```bash
kubectl run mqtt-sub --rm -it --image=eclipse-mosquitto --restart=Never -- \
  mosquitto_sub \
  -h emqx.observability.svc.cluster.local \
  -p 1883 \
  -t 'devices/sensor-1/shadow/update' \
  -v
```

Publish the shadow update from another terminal. The subscriber should print the
topic and payload.

## Check Ingestor Logs

Confirm the ingestor accepted the shadow update:

```bash
kubectl logs -n hardware-sim deploy/mqtt-ingestor --tail=100
```

Look for:

```text
digital_twin_shadow_updated
```

Follow logs while testing:

```bash
kubectl logs -n hardware-sim deploy/mqtt-ingestor -f
```

After telemetry arrives for the same `device_id`, look for:

```text
digital_twin_sync_check
```

## Expected Log Shape

Shadow update:

```json
{
  "msg": "digital_twin_shadow_updated",
  "device_id": "sensor-1",
  "properties": ["temperature", "voltage", "device_state", "firmware_version"]
}
```

Numeric drift:

```json
{
  "msg": "digital_twin_sync_check",
  "device_id": "sensor-1",
  "property": "temperature",
  "reported": 35.5,
  "desired": 36.0,
  "drift": 0.5,
  "status": "out_of_sync"
}
```

Status mismatch:

```json
{
  "msg": "digital_twin_sync_check",
  "device_id": "sensor-1",
  "property": "firmware_version",
  "reported": "1.0.0",
  "desired": "1.0.1",
  "match": false,
  "status": "out_of_sync"
}
```

## Metrics To Query

Query these through Prometheus or Grafana:

- `hardware.shadow.drift`
- `hardware.shadow.status_match`

Attributes:

- `environment`
- `device_id`
- `property`

## Manual Verification

1. Run `go test ./...`.
2. Run the ingestor with simulator telemetry.
3. Publish a shadow update to `devices/{device_id}/shadow/update`.
4. Confirm `digital_twin_shadow_updated` appears in ingestor logs.
5. Confirm telemetry for the same device emits `digital_twin_sync_check`.
6. Query `hardware.shadow.drift` and `hardware.shadow.status_match`.
