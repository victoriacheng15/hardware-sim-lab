package mqttingestor

import (
	"errors"
	"math"
	"testing"
	"time"
)

func TestProcessShadowUpdateStoresDesiredFields(t *testing.T) {
	processor := NewProcessor(Config{})
	receivedAt := time.Date(2026, 4, 28, 12, 0, 0, 0, time.UTC)

	shadow, err := processor.ProcessShadowUpdate("sensor-1", []byte(`{
		"desired": {
			"temperature": 36.0,
			"voltage": 3.3,
			"device_state": "running",
			"firmware_version": "1.0.1"
		}
	}`), receivedAt)
	if err != nil {
		t.Fatalf("ProcessShadowUpdate returned error: %v", err)
	}

	if shadow.Temperature == nil || *shadow.Temperature != 36.0 {
		t.Fatalf("Temperature = %v, want 36.0", shadow.Temperature)
	}
	if shadow.Voltage == nil || *shadow.Voltage != 3.3 {
		t.Fatalf("Voltage = %v, want 3.3", shadow.Voltage)
	}
	if shadow.DeviceState == nil || *shadow.DeviceState != "running" {
		t.Fatalf("DeviceState = %v, want running", shadow.DeviceState)
	}
	if shadow.FirmwareVersion == nil || *shadow.FirmwareVersion != "1.0.1" {
		t.Fatalf("FirmwareVersion = %v, want 1.0.1", shadow.FirmwareVersion)
	}
	if !shadow.UpdatedAt.Equal(receivedAt) {
		t.Fatalf("UpdatedAt = %s, want %s", shadow.UpdatedAt, receivedAt)
	}
}

func TestProcessShadowUpdateMergesPartialUpdates(t *testing.T) {
	processor := NewProcessor(Config{})
	firstUpdate := time.Date(2026, 4, 28, 12, 0, 0, 0, time.UTC)
	secondUpdate := firstUpdate.Add(time.Minute)

	_, err := processor.ProcessShadowUpdate("sensor-1", []byte(`{
		"desired": {
			"temperature": 36.0,
			"device_state": "running"
		}
	}`), firstUpdate)
	if err != nil {
		t.Fatalf("first ProcessShadowUpdate returned error: %v", err)
	}

	shadow, err := processor.ProcessShadowUpdate("sensor-1", []byte(`{
		"desired": {
			"voltage": 3.3
		}
	}`), secondUpdate)
	if err != nil {
		t.Fatalf("second ProcessShadowUpdate returned error: %v", err)
	}

	if shadow.Temperature == nil || *shadow.Temperature != 36.0 {
		t.Fatalf("Temperature = %v, want preserved 36.0", shadow.Temperature)
	}
	if shadow.DeviceState == nil || *shadow.DeviceState != "running" {
		t.Fatalf("DeviceState = %v, want preserved running", shadow.DeviceState)
	}
	if shadow.Voltage == nil || *shadow.Voltage != 3.3 {
		t.Fatalf("Voltage = %v, want 3.3", shadow.Voltage)
	}
	if !shadow.UpdatedAt.Equal(secondUpdate) {
		t.Fatalf("UpdatedAt = %s, want %s", shadow.UpdatedAt, secondUpdate)
	}
}

func TestProcessShadowUpdateRejectsInvalidPayload(t *testing.T) {
	processor := NewProcessor(Config{})
	tests := []struct {
		name    string
		payload string
		wantErr error
	}{
		{
			name:    "malformed json",
			payload: `{"desired":`,
			wantErr: ErrMalformedPayload,
		},
		{
			name:    "missing desired",
			payload: `{}`,
			wantErr: ErrInvalidPayload,
		},
		{
			name:    "unsupported field",
			payload: `{"desired":{"current":1.2}}`,
			wantErr: ErrInvalidPayload,
		},
		{
			name:    "empty desired",
			payload: `{"desired":{}}`,
			wantErr: ErrInvalidPayload,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := processor.ProcessShadowUpdate("sensor-1", []byte(tt.payload), time.Now())
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestProcessWithoutShadowReturnsUnknownTwinStatus(t *testing.T) {
	processor := NewProcessor(Config{})

	analysis, err := processor.Process(validTelemetryPayload(), receivedAt())
	if err != nil {
		t.Fatalf("Process returned error: %v", err)
	}

	if analysis.Twin.ShadowFound {
		t.Fatal("ShadowFound = true, want false")
	}
	if analysis.Twin.SyncStatus != SyncStatusUnknown {
		t.Fatalf("SyncStatus = %q, want %q", analysis.Twin.SyncStatus, SyncStatusUnknown)
	}
}

func TestProcessWithShadowReturnsDriftAndStatusComparisons(t *testing.T) {
	processor := NewProcessor(Config{})

	shadowUpdatedAt := receivedAt().Add(-time.Minute)
	_, err := processor.ProcessShadowUpdate("sensor-1", []byte(`{
		"desired": {
			"temperature": 36.0,
			"voltage": 3.3,
			"device_state": "running",
			"firmware_version": "1.0.1"
		}
	}`), shadowUpdatedAt)
	if err != nil {
		t.Fatalf("ProcessShadowUpdate returned error: %v", err)
	}

	analysis, err := processor.Process(validTelemetryPayload(), receivedAt())
	if err != nil {
		t.Fatalf("Process returned error: %v", err)
	}

	if !analysis.Twin.ShadowFound {
		t.Fatal("ShadowFound = false, want true")
	}
	if analysis.Twin.SyncStatus != SyncStatusOutOfSync {
		t.Fatalf("SyncStatus = %q, want %q", analysis.Twin.SyncStatus, SyncStatusOutOfSync)
	}
	if !analysis.Twin.ShadowUpdatedAt.Equal(shadowUpdatedAt) {
		t.Fatalf("ShadowUpdatedAt = %s, want %s", analysis.Twin.ShadowUpdatedAt, shadowUpdatedAt)
	}

	assertDrift(t, analysis.Twin.NumericDrifts, "temperature", 35.5, 36.0, 0.5)
	assertDrift(t, analysis.Twin.NumericDrifts, "voltage", 3.2, 3.3, 0.1)
	assertStatusComparison(t, analysis.Twin.StatusComparisons, "device_state", "running", "running", true)
	assertStatusComparison(t, analysis.Twin.StatusComparisons, "firmware_version", "1.0.0", "1.0.1", false)
}

func TestProcessWithMatchingShadowReturnsInSync(t *testing.T) {
	processor := NewProcessor(Config{})

	_, err := processor.ProcessShadowUpdate("sensor-1", []byte(`{
		"desired": {
			"temperature": 35.5,
			"voltage": 3.2,
			"device_state": "running",
			"firmware_version": "1.0.0"
		}
	}`), receivedAt())
	if err != nil {
		t.Fatalf("ProcessShadowUpdate returned error: %v", err)
	}

	analysis, err := processor.Process(validTelemetryPayload(), receivedAt())
	if err != nil {
		t.Fatalf("Process returned error: %v", err)
	}

	if analysis.Twin.SyncStatus != SyncStatusInSync {
		t.Fatalf("SyncStatus = %q, want %q", analysis.Twin.SyncStatus, SyncStatusInSync)
	}
}

func assertDrift(t *testing.T, drifts []NumericDrift, property string, reported, desired, drift float64) {
	t.Helper()

	for _, got := range drifts {
		if got.Property == property {
			if got.Reported != reported || got.Desired != desired || math.Abs(got.Drift-drift) > 0.000001 {
				t.Fatalf("%s drift = %+v, want reported=%v desired=%v drift=%v", property, got, reported, desired, drift)
			}
			return
		}
	}

	t.Fatalf("drift for %q not found in %+v", property, drifts)
}

func assertStatusComparison(t *testing.T, comparisons []StatusComparison, property, reported, desired string, match bool) {
	t.Helper()

	for _, got := range comparisons {
		if got.Property == property {
			if got.Reported != reported || got.Desired != desired || got.Match != match {
				t.Fatalf("%s comparison = %+v, want reported=%q desired=%q match=%v", property, got, reported, desired, match)
			}
			return
		}
	}

	t.Fatalf("comparison for %q not found in %+v", property, comparisons)
}

func validTelemetryPayload() []byte {
	return []byte(`{
		"sensor_id": "sensor-1",
		"schema_version": "1",
		"device_id": "sensor-1",
		"firmware_version": "1.0.0",
		"device_state": "running",
		"sequence_number": 1,
		"telemetry_topic": "devices/sensor-1/telemetry",
		"temperature": 35.5,
		"voltage": 3.2,
		"current": 0.2,
		"power_usage": 0.64,
		"rssi": -61,
		"snr": 7.5,
		"packet_loss_percent": 0,
		"free_heap": 1024,
		"loop_time_ms": 10,
		"uptime_seconds": 120,
		"reboot_reason": "power_on",
		"timestamp": "2026-04-28T11:59:58Z"
	}`)
}

func receivedAt() time.Time {
	return time.Date(2026, 4, 28, 12, 0, 0, 0, time.UTC)
}
