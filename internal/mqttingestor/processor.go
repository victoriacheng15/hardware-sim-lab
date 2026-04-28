package mqttingestor

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
)

const DefaultSchemaVersion = "1"

var (
	ErrMalformedPayload = errors.New("malformed payload")
	ErrInvalidPayload   = errors.New("invalid payload")
)

type Config struct {
	ExpectedSchemaVersion string
	StaleAfter            time.Duration
}

type TelemetryMessage struct {
	SensorID        string  `json:"sensor_id"`
	SchemaVersion   string  `json:"schema_version"`
	DeviceID        string  `json:"device_id"`
	FirmwareVersion string  `json:"firmware_version"`
	DeviceState     string  `json:"device_state"`
	SequenceNumber  uint64  `json:"sequence_number"`
	TelemetryTopic  string  `json:"telemetry_topic"`
	Temperature     float64 `json:"temperature"`
	Voltage         float64 `json:"voltage"`
	Current         float64 `json:"current"`
	PowerUsage      float64 `json:"power_usage"`
	RSSI            float64 `json:"rssi"`
	SNR             float64 `json:"snr"`
	PacketLoss      float64 `json:"packet_loss_percent"`
	FreeHeap        uint64  `json:"free_heap"`
	LoopTimeMS      float64 `json:"loop_time_ms"`
	UptimeSeconds   int64   `json:"uptime_seconds"`
	RebootReason    string  `json:"reboot_reason"`
	Timestamp       string  `json:"timestamp"`
}

type Analysis struct {
	Message             TelemetryMessage
	ReceivedAt          time.Time
	DeviceTime          time.Time
	MessageAge          time.Duration
	IsStale             bool
	IsDuplicate         bool
	IsReboot            bool
	StateChanged        bool
	SequenceGap         uint64
	PreviousFound       bool
	PreviousDeviceState string
	Twin                TwinAnalysis
}

type SyncStatus string

const (
	SyncStatusUnknown   SyncStatus = "unknown"
	SyncStatusInSync    SyncStatus = "in_sync"
	SyncStatusOutOfSync SyncStatus = "out_of_sync"
)

type TwinAnalysis struct {
	ShadowFound       bool
	ShadowUpdatedAt   time.Time
	SyncStatus        SyncStatus
	NumericDrifts     []NumericDrift
	StatusComparisons []StatusComparison
}

type NumericDrift struct {
	Property string
	Reported float64
	Desired  float64
	Drift    float64
}

type StatusComparison struct {
	Property string
	Reported string
	Desired  string
	Match    bool
}

type ShadowState struct {
	Temperature     *float64
	Voltage         *float64
	DeviceState     *string
	FirmwareVersion *string
	UpdatedAt       time.Time
}

type deviceState struct {
	SequenceNumber uint64
	UptimeSeconds  int64
	RebootReason   string
	DeviceState    string
}

type Processor struct {
	expectedSchemaVersion string
	staleAfter            time.Duration

	mu      sync.Mutex
	devices map[string]deviceState
	shadows map[string]ShadowState
}

func NewProcessor(cfg Config) *Processor {
	expectedSchemaVersion := cfg.ExpectedSchemaVersion
	if expectedSchemaVersion == "" {
		expectedSchemaVersion = DefaultSchemaVersion
	}

	return &Processor{
		expectedSchemaVersion: expectedSchemaVersion,
		staleAfter:            cfg.StaleAfter,
		devices:               make(map[string]deviceState),
		shadows:               make(map[string]ShadowState),
	}
}

func (p *Processor) ProcessShadowUpdate(deviceID string, payload []byte, receivedAt time.Time) (ShadowState, error) {
	if deviceID == "" {
		return ShadowState{}, fmt.Errorf("%w: missing device_id", ErrInvalidPayload)
	}

	desired, err := decodeShadowDesired(payload)
	if err != nil {
		return ShadowState{}, err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	shadow := p.shadows[deviceID]
	if desired.Temperature != nil {
		shadow.Temperature = cloneFloat64(desired.Temperature)
	}
	if desired.Voltage != nil {
		shadow.Voltage = cloneFloat64(desired.Voltage)
	}
	if desired.DeviceState != nil {
		shadow.DeviceState = cloneString(desired.DeviceState)
	}
	if desired.FirmwareVersion != nil {
		shadow.FirmwareVersion = cloneString(desired.FirmwareVersion)
	}
	shadow.UpdatedAt = receivedAt

	p.shadows[deviceID] = shadow
	return shadow, nil
}

func (p *Processor) Process(payload []byte, receivedAt time.Time) (Analysis, error) {
	msg, deviceTime, err := p.DecodeAndValidate(payload)
	if err != nil {
		return Analysis{}, err
	}

	analysis := Analysis{
		Message:    msg,
		ReceivedAt: receivedAt,
		DeviceTime: deviceTime,
		MessageAge: receivedAt.Sub(deviceTime),
		Twin: TwinAnalysis{
			SyncStatus: SyncStatusUnknown,
		},
	}

	if p.staleAfter > 0 && analysis.MessageAge > p.staleAfter {
		analysis.IsStale = true
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	previous, ok := p.devices[msg.DeviceID]
	analysis.PreviousFound = ok

	if ok {
		analysis.PreviousDeviceState = previous.DeviceState
		analysis.StateChanged = previous.DeviceState != "" && msg.DeviceState != previous.DeviceState

		switch {
		case msg.SequenceNumber == previous.SequenceNumber:
			analysis.IsDuplicate = true
		case msg.SequenceNumber > previous.SequenceNumber+1:
			analysis.SequenceGap = msg.SequenceNumber - previous.SequenceNumber - 1
		case msg.SequenceNumber < previous.SequenceNumber && rebootEvidence(msg, previous):
			analysis.IsReboot = true
		case msg.SequenceNumber < previous.SequenceNumber:
			analysis.IsDuplicate = true
		}
	}

	p.devices[msg.DeviceID] = deviceState{
		SequenceNumber: msg.SequenceNumber,
		UptimeSeconds:  msg.UptimeSeconds,
		RebootReason:   msg.RebootReason,
		DeviceState:    msg.DeviceState,
	}

	if shadow, ok := p.shadows[msg.DeviceID]; ok {
		analysis.Twin = analyzeTwin(msg, shadow)
	}

	return analysis, nil
}

type shadowUpdatePayload struct {
	Desired *shadowDesired `json:"desired"`
}

type shadowDesired struct {
	Temperature     *float64 `json:"temperature"`
	Voltage         *float64 `json:"voltage"`
	DeviceState     *string  `json:"device_state"`
	FirmwareVersion *string  `json:"firmware_version"`
}

func decodeShadowDesired(payload []byte) (shadowDesired, error) {
	var update shadowUpdatePayload
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&update); err != nil {
		if errors.As(err, new(*json.SyntaxError)) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			return shadowDesired{}, fmt.Errorf("%w: %v", ErrMalformedPayload, err)
		}
		return shadowDesired{}, fmt.Errorf("%w: %v", ErrInvalidPayload, err)
	}
	var extra any
	if err := decoder.Decode(&extra); err == nil {
		return shadowDesired{}, fmt.Errorf("%w: multiple JSON values", ErrMalformedPayload)
	} else if !errors.Is(err, io.EOF) {
		return shadowDesired{}, fmt.Errorf("%w: %v", ErrMalformedPayload, err)
	}
	if update.Desired == nil {
		return shadowDesired{}, fmt.Errorf("%w: missing desired", ErrInvalidPayload)
	}
	if !update.Desired.hasFields() {
		return shadowDesired{}, fmt.Errorf("%w: desired has no supported fields", ErrInvalidPayload)
	}
	return *update.Desired, nil
}

func (d shadowDesired) hasFields() bool {
	return d.Temperature != nil ||
		d.Voltage != nil ||
		d.DeviceState != nil ||
		d.FirmwareVersion != nil
}

func analyzeTwin(msg TelemetryMessage, shadow ShadowState) TwinAnalysis {
	twin := TwinAnalysis{
		ShadowFound:     true,
		ShadowUpdatedAt: shadow.UpdatedAt,
		SyncStatus:      SyncStatusInSync,
	}

	if shadow.Temperature != nil {
		twin.NumericDrifts = append(twin.NumericDrifts, NumericDrift{
			Property: "temperature",
			Reported: msg.Temperature,
			Desired:  *shadow.Temperature,
			Drift:    *shadow.Temperature - msg.Temperature,
		})
	}
	if shadow.Voltage != nil {
		twin.NumericDrifts = append(twin.NumericDrifts, NumericDrift{
			Property: "voltage",
			Reported: msg.Voltage,
			Desired:  *shadow.Voltage,
			Drift:    *shadow.Voltage - msg.Voltage,
		})
	}
	if shadow.DeviceState != nil {
		match := msg.DeviceState == *shadow.DeviceState
		twin.StatusComparisons = append(twin.StatusComparisons, StatusComparison{
			Property: "device_state",
			Reported: msg.DeviceState,
			Desired:  *shadow.DeviceState,
			Match:    match,
		})
	}
	if shadow.FirmwareVersion != nil {
		match := msg.FirmwareVersion == *shadow.FirmwareVersion
		twin.StatusComparisons = append(twin.StatusComparisons, StatusComparison{
			Property: "firmware_version",
			Reported: msg.FirmwareVersion,
			Desired:  *shadow.FirmwareVersion,
			Match:    match,
		})
	}

	if len(twin.NumericDrifts) == 0 && len(twin.StatusComparisons) == 0 {
		twin.SyncStatus = SyncStatusUnknown
		return twin
	}

	for _, drift := range twin.NumericDrifts {
		if drift.Drift != 0 {
			twin.SyncStatus = SyncStatusOutOfSync
			return twin
		}
	}
	for _, comparison := range twin.StatusComparisons {
		if !comparison.Match {
			twin.SyncStatus = SyncStatusOutOfSync
			return twin
		}
	}

	return twin
}

func (p *Processor) DecodeAndValidate(payload []byte) (TelemetryMessage, time.Time, error) {
	var msg TelemetryMessage
	if err := json.Unmarshal(payload, &msg); err != nil {
		return TelemetryMessage{}, time.Time{}, fmt.Errorf("%w: %v", ErrMalformedPayload, err)
	}

	if err := p.validate(msg); err != nil {
		return TelemetryMessage{}, time.Time{}, err
	}

	deviceTime, err := time.Parse(time.RFC3339, msg.Timestamp)
	if err != nil {
		return TelemetryMessage{}, time.Time{}, fmt.Errorf("%w: invalid timestamp: %v", ErrInvalidPayload, err)
	}

	return msg, deviceTime, nil
}

func (p *Processor) validate(msg TelemetryMessage) error {
	switch {
	case msg.SchemaVersion == "":
		return fmt.Errorf("%w: missing schema_version", ErrInvalidPayload)
	case msg.SchemaVersion != p.expectedSchemaVersion:
		return fmt.Errorf("%w: unexpected schema_version %q", ErrInvalidPayload, msg.SchemaVersion)
	case msg.DeviceID == "":
		return fmt.Errorf("%w: missing device_id", ErrInvalidPayload)
	case msg.DeviceState == "":
		return fmt.Errorf("%w: missing device_state", ErrInvalidPayload)
	case msg.SequenceNumber == 0:
		return fmt.Errorf("%w: missing sequence_number", ErrInvalidPayload)
	case msg.Timestamp == "":
		return fmt.Errorf("%w: missing timestamp", ErrInvalidPayload)
	default:
		return nil
	}
}

func rebootEvidence(msg TelemetryMessage, previous deviceState) bool {
	return msg.UptimeSeconds < previous.UptimeSeconds || msg.RebootReason != previous.RebootReason
}

func cloneFloat64(value *float64) *float64 {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

func cloneString(value *string) *string {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}
