package mqttingestor

import (
	"errors"
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

func assertEvent(t *testing.T, events []messageEvent, name string) {
	t.Helper()

	for _, event := range events {
		if event.name == name {
			return
		}
	}

	t.Fatalf("event %q not found in %+v", name, events)
}
