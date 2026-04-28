package mqttingestor

import (
	"errors"
	"math"
	"reflect"
	"testing"
)

func TestDefaultTopicsIncludeTelemetryShadowAndLegacyTopics(t *testing.T) {
	cfg := RuntimeConfig{}.withDefaults()
	runtime := &Runtime{config: cfg}

	got := runtime.subscriptionFilters()
	want := map[string]byte{
		"devices/+/telemetry":     1,
		"devices/+/shadow/update": 1,
		"sensors/thermal":         1,
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("subscriptionFilters = %+v, want %+v", got, want)
	}
}

func TestEvaluateMessageRoutesShadowUpdateWithoutTelemetryValidation(t *testing.T) {
	runtime := &Runtime{processor: NewProcessor(Config{})}

	result := runtime.evaluateMessage("devices/sensor-1/shadow/update", []byte(`{
		"desired": {
			"temperature": 36.0,
			"voltage": 3.3
		}
	}`), receivedAt())
	if result.err != nil {
		t.Fatalf("evaluateMessage returned error: %v", result.err)
	}
	if result.invalid {
		t.Fatal("invalid = true, want false")
	}
	if result.metrics {
		t.Fatal("metrics = true, want false for shadow update")
	}
	assertEvent(t, result.events, "digital_twin_shadow_updated")

	telemetryResult := runtime.evaluateMessage("devices/sensor-1/telemetry", validTelemetryPayload(), receivedAt())
	if telemetryResult.err != nil {
		t.Fatalf("telemetry evaluateMessage returned error: %v", telemetryResult.err)
	}
	if telemetryResult.analysis.Twin.SyncStatus != SyncStatusOutOfSync {
		t.Fatalf("SyncStatus = %q, want %q", telemetryResult.analysis.Twin.SyncStatus, SyncStatusOutOfSync)
	}
}

func TestEvaluateMessageRoutesTelemetryThroughProcessor(t *testing.T) {
	runtime := &Runtime{processor: NewProcessor(Config{})}

	result := runtime.evaluateMessage("devices/sensor-1/telemetry", validTelemetryPayload(), receivedAt())
	if result.err != nil {
		t.Fatalf("evaluateMessage returned error: %v", result.err)
	}
	if result.invalid {
		t.Fatal("invalid = true, want false")
	}
	if !result.metrics {
		t.Fatal("metrics = false, want true for telemetry")
	}
	if result.analysis.Message.DeviceID != "sensor-1" {
		t.Fatalf("DeviceID = %q, want sensor-1", result.analysis.Message.DeviceID)
	}
	assertEvent(t, result.events, "mqtt_message_received")
}

func TestEvaluateMessageRejectsTelemetryTopicPayloadDeviceMismatch(t *testing.T) {
	runtime := &Runtime{processor: NewProcessor(Config{})}

	result := runtime.evaluateMessage("devices/sensor-2/telemetry", validTelemetryPayload(), receivedAt())
	if !result.invalid {
		t.Fatal("invalid = false, want true")
	}
	if !errors.Is(result.err, ErrInvalidPayload) {
		t.Fatalf("error = %v, want ErrInvalidPayload", result.err)
	}
	assertEvent(t, result.events, "mqtt_payload_invalid")
}

func TestEvaluateMessageKeepsLegacyTelemetryTopic(t *testing.T) {
	runtime := &Runtime{processor: NewProcessor(Config{})}

	result := runtime.evaluateMessage("sensors/thermal", validTelemetryPayload(), receivedAt())
	if result.err != nil {
		t.Fatalf("evaluateMessage returned error: %v", result.err)
	}
	if result.invalid {
		t.Fatal("invalid = true, want false")
	}
	if !result.metrics {
		t.Fatal("metrics = false, want true for legacy telemetry")
	}
	assertEvent(t, result.events, "mqtt_message_received")
}

func TestEvaluateMessageAddsDigitalTwinSyncLogEvents(t *testing.T) {
	runtime := &Runtime{processor: NewProcessor(Config{})}

	shadowResult := runtime.evaluateMessage("devices/sensor-1/shadow/update", []byte(`{
		"desired": {
			"temperature": 34.0,
			"device_state": "maintenance"
		}
	}`), receivedAt())
	if shadowResult.err != nil {
		t.Fatalf("shadow evaluateMessage returned error: %v", shadowResult.err)
	}

	result := runtime.evaluateMessage("devices/sensor-1/telemetry", validTelemetryPayload(), receivedAt())
	if result.err != nil {
		t.Fatalf("telemetry evaluateMessage returned error: %v", result.err)
	}

	driftEvent := findSyncEvent(t, result.events, "temperature")
	driftAttrs := eventAttrs(driftEvent)
	if driftAttrs["drift"] != -1.5 {
		t.Fatalf("temperature drift log value = %v, want signed -1.5", driftAttrs["drift"])
	}
	if driftAttrs["status"] != string(SyncStatusOutOfSync) {
		t.Fatalf("temperature status = %v, want %s", driftAttrs["status"], SyncStatusOutOfSync)
	}

	statusEvent := findSyncEvent(t, result.events, "device_state")
	statusAttrs := eventAttrs(statusEvent)
	if statusAttrs["match"] != false {
		t.Fatalf("device_state match = %v, want false", statusAttrs["match"])
	}
}

func TestDigitalTwinMetricRecordsUseAbsoluteDriftAndStatusMatchValues(t *testing.T) {
	analysis := Analysis{
		Message: TelemetryMessage{DeviceID: "sensor-1"},
		Twin: TwinAnalysis{
			ShadowFound: true,
			NumericDrifts: []NumericDrift{{
				Property: "temperature",
				Reported: 35.5,
				Desired:  34.0,
				Drift:    -1.5,
			}},
			StatusComparisons: []StatusComparison{
				{Property: "device_state", Reported: "running", Desired: "running", Match: true},
				{Property: "firmware_version", Reported: "1.0.0", Desired: "1.0.1", Match: false},
			},
		},
	}

	records := digitalTwinMetricRecords("test", analysis)

	drift := findMetricRecord(t, records, "hardware.shadow.drift", "temperature")
	if math.Abs(drift.value-1.5) > 0.000001 {
		t.Fatalf("drift metric value = %v, want absolute 1.5", drift.value)
	}

	match := findMetricRecord(t, records, "hardware.shadow.status_match", "device_state")
	if match.value != 1 {
		t.Fatalf("matching status metric value = %v, want 1", match.value)
	}

	mismatch := findMetricRecord(t, records, "hardware.shadow.status_match", "firmware_version")
	if mismatch.value != 0 {
		t.Fatalf("mismatching status metric value = %v, want 0", mismatch.value)
	}
}

func assertEvent(t *testing.T, events []messageEvent, name string) {
	t.Helper()

	for _, event := range events {
		if event.name == name {
			return
		}
	}

	t.Fatalf("event %q not found in %+v", name, events)
}

func findSyncEvent(t *testing.T, events []messageEvent, property string) messageEvent {
	t.Helper()

	for _, event := range events {
		attrs := eventAttrs(event)
		if event.name == "digital_twin_sync_check" && attrs["property"] == property {
			return event
		}
	}

	t.Fatalf("digital_twin_sync_check event for %q not found in %+v", property, events)
	return messageEvent{}
}

func eventAttrs(event messageEvent) map[string]any {
	attrs := make(map[string]any, len(event.attrs)/2)
	for i := 0; i+1 < len(event.attrs); i += 2 {
		key, ok := event.attrs[i].(string)
		if !ok {
			continue
		}
		attrs[key] = event.attrs[i+1]
	}
	return attrs
}

func findMetricRecord(t *testing.T, records []twinMetricRecord, name, property string) twinMetricRecord {
	t.Helper()

	for _, record := range records {
		if record.name != name {
			continue
		}
		for _, attr := range record.attrs {
			if string(attr.Key) == "property" && attr.Value.AsString() == property {
				return record
			}
		}
	}

	t.Fatalf("metric record %q for %q not found in %+v", name, property, records)
	return twinMetricRecord{}
}
