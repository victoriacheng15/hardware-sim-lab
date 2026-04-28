package mqttingestor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"hardware-sim-lab/internal/telemetry"
)

const (
	DefaultBrokerURL         = "tcp://emqx.observability:1883"
	DefaultClientID          = "mqtt-ingestor"
	DefaultStaleAfter        = 30 * time.Second
	defaultMetricDescription = "Synthetic hardware telemetry signal"
)

var DefaultTopics = []string{
	"devices/+/telemetry",
	"devices/+/shadow/update",
	"sensors/thermal",
}

type client interface {
	Connect() mqtt.Token
	Disconnect(quiesce uint)
	SubscribeMultiple(filters map[string]byte, callback mqtt.MessageHandler) mqtt.Token
}

type RuntimeConfig struct {
	BrokerURL             string
	ClientID              string
	Environment           string
	ExpectedSchemaVersion string
	StaleAfter            time.Duration
	Topics                []string
}

type Runtime struct {
	config    RuntimeConfig
	processor *Processor
	metrics   runtimeMetrics
	newClient func(*mqtt.ClientOptions) client
	now       func() time.Time
}

type runtimeMetrics struct {
	messagesTotal        telemetry.Int64Counter
	invalidMessagesTotal telemetry.Int64Counter
	sequenceGapTotal     telemetry.Int64Counter
	duplicatesTotal      telemetry.Int64Counter
	messageAge           telemetry.Int64Histogram
	rebootTotal          telemetry.Int64Counter
	temperature          telemetry.Float64Histogram
	voltage              telemetry.Float64Histogram
	current              telemetry.Float64Histogram
	power                telemetry.Float64Histogram
	rssi                 telemetry.Float64Histogram
	snr                  telemetry.Float64Histogram
	packetLoss           telemetry.Float64Histogram
	freeHeap             telemetry.Int64Histogram
	loopTime             telemetry.Float64Histogram
	uptime               telemetry.Int64Histogram
	shadowDrift          telemetry.Float64Histogram
	shadowStatusMatch    telemetry.Int64Histogram
}

type messageEvent struct {
	name  string
	attrs []any
}

type messageResult struct {
	analysis Analysis
	invalid  bool
	metrics  bool
	err      error
	events   []messageEvent
}

type twinMetricRecord struct {
	name  string
	value float64
	attrs []telemetry.Attribute
}

type topicRoute int

const (
	topicRouteTelemetry topicRoute = iota
	topicRouteShadowUpdate
	topicRouteUnknown
)

type routedTopic struct {
	route    topicRoute
	deviceID string
}

func NewRuntime(cfg RuntimeConfig) (*Runtime, error) {
	cfg = cfg.withDefaults()

	metrics, err := newRuntimeMetrics()
	if err != nil {
		return nil, err
	}

	return &Runtime{
		config:    cfg,
		processor: NewProcessor(Config{ExpectedSchemaVersion: cfg.ExpectedSchemaVersion, StaleAfter: cfg.StaleAfter}),
		metrics:   metrics,
		newClient: func(opts *mqtt.ClientOptions) client { return mqtt.NewClient(opts) },
		now:       time.Now,
	}, nil
}

func (r *Runtime) Run(ctx context.Context) error {
	opts := mqtt.NewClientOptions().AddBroker(r.config.BrokerURL)
	opts.SetClientID(r.config.ClientID)
	opts.SetCleanSession(true)
	opts.SetConnectTimeout(10 * time.Second)
	opts.SetKeepAlive(30 * time.Second)
	opts.SetPingTimeout(5 * time.Second)
	opts.SetAutoReconnect(true)
	opts.SetOnConnectHandler(func(c mqtt.Client) {
		r.subscribe(c)
	})
	opts.SetConnectionLostHandler(func(_ mqtt.Client, err error) {
		telemetry.Warn("mqtt_connection_lost", "error", err)
	})

	mqttClient := r.newClient(opts)
	token := mqttClient.Connect()
	if token.Wait() && token.Error() != nil {
		return fmt.Errorf("connect mqtt client: %w", token.Error())
	}
	defer mqttClient.Disconnect(250)

	r.subscribe(mqttClient)

	telemetry.Info("mqtt_ingestor_started", "broker", r.config.BrokerURL, "topics", r.config.Topics)

	<-ctx.Done()
	telemetry.Info("mqtt_ingestor_stopped", "reason", ctx.Err())
	return nil
}

func (r *Runtime) subscribe(c client) {
	token := c.SubscribeMultiple(r.subscriptionFilters(), r.handleMQTTMessage)
	if token.Wait() && token.Error() != nil {
		telemetry.Error("mqtt_subscription_failed", "error", token.Error())
		return
	}
	telemetry.Info("mqtt_subscription_established", "topics", r.config.Topics)
}

func (r *Runtime) evaluateMessage(topic string, payload []byte, receivedAt time.Time) messageResult {
	route := parseTopicRoute(topic)
	if route.route == topicRouteShadowUpdate {
		shadow, err := r.processor.ProcessShadowUpdate(route.deviceID, payload, receivedAt)
		if err != nil {
			return invalidMessageResult(topic, err)
		}

		return messageResult{
			events: []messageEvent{{
				name: "digital_twin_shadow_updated",
				attrs: []any{
					"topic", topic,
					"device_id", route.deviceID,
					"properties", shadow.Properties(),
				},
			}},
		}
	}

	if route.route == topicRouteTelemetry && route.deviceID != "" {
		payloadDeviceID, ok, err := extractPayloadDeviceID(payload)
		if err != nil {
			return invalidMessageResult(topic, err)
		}
		if ok && payloadDeviceID != route.deviceID {
			return invalidMessageResult(
				topic,
				fmt.Errorf("%w: topic device_id %q does not match payload device_id %q", ErrInvalidPayload, route.deviceID, payloadDeviceID),
			)
		}
	}

	analysis, err := r.processor.Process(payload, receivedAt)
	if err != nil {
		if errors.Is(err, ErrMalformedPayload) || errors.Is(err, ErrInvalidPayload) {
			return invalidMessageResult(topic, err)
		}

		return messageResult{err: err}
	}

	result := messageResult{
		analysis: analysis,
		metrics:  true,
		events: []messageEvent{{
			name: "mqtt_message_received",
			attrs: []any{
				"topic", topic,
				"device_id", analysis.Message.DeviceID,
				"device_state", analysis.Message.DeviceState,
				"sequence_number", analysis.Message.SequenceNumber,
			},
		}},
	}

	if analysis.SequenceGap > 0 {
		result.events = append(result.events, messageEvent{
			name: "mqtt_sequence_gap_detected",
			attrs: []any{
				"topic", topic,
				"device_id", analysis.Message.DeviceID,
				"sequence_gap", analysis.SequenceGap,
				"sequence_number", analysis.Message.SequenceNumber,
			},
		})
	}
	if analysis.IsDuplicate {
		result.events = append(result.events, messageEvent{
			name: "mqtt_duplicate_sequence_detected",
			attrs: []any{
				"topic", topic,
				"device_id", analysis.Message.DeviceID,
				"sequence_number", analysis.Message.SequenceNumber,
			},
		})
	}
	if analysis.IsReboot {
		result.events = append(result.events, messageEvent{
			name: "device_reboot_detected",
			attrs: []any{
				"topic", topic,
				"device_id", analysis.Message.DeviceID,
				"reboot_reason", analysis.Message.RebootReason,
			},
		})
	}
	if analysis.StateChanged {
		result.events = append(result.events, messageEvent{
			name: "device_state_changed",
			attrs: []any{
				"topic", topic,
				"device_id", analysis.Message.DeviceID,
				"previous_state", analysis.PreviousDeviceState,
				"device_state", analysis.Message.DeviceState,
			},
		})
	}
	if analysis.IsStale {
		result.events = append(result.events, messageEvent{
			name: "device_message_stale",
			attrs: []any{
				"topic", topic,
				"device_id", analysis.Message.DeviceID,
				"message_age_ms", analysis.MessageAge.Milliseconds(),
			},
		})
	}
	result.events = append(result.events, digitalTwinSyncEvents(analysis)...)

	return result
}

func (r *Runtime) recordResult(ctx context.Context, result messageResult) {
	if result.err != nil && !result.invalid {
		telemetry.Error("mqtt_message_processing_failed", "error", result.err)
		return
	}

	for _, event := range result.events {
		if event.name == "mqtt_payload_invalid" {
			telemetry.Warn(event.name, event.attrs...)
			continue
		}
		telemetry.Info(event.name, event.attrs...)
	}

	if result.invalid {
		telemetry.AddInt64Counter(ctx, r.metrics.invalidMessagesTotal, 1, telemetry.StringAttribute("environment", r.config.Environment))
		return
	}
	if !result.metrics {
		return
	}

	analysis := result.analysis
	commonAttrs := []telemetry.Attribute{
		telemetry.StringAttribute("environment", r.config.Environment),
		telemetry.StringAttribute("device_id", analysis.Message.DeviceID),
		telemetry.StringAttribute("firmware_version", analysis.Message.FirmwareVersion),
		telemetry.StringAttribute("device_state", analysis.Message.DeviceState),
		telemetry.StringAttribute("topic", analysis.Message.TelemetryTopic),
	}

	telemetry.AddInt64Counter(ctx, r.metrics.messagesTotal, 1, commonAttrs...)
	telemetry.RecordInt64Histogram(ctx, r.metrics.messageAge, analysis.MessageAge.Milliseconds(), commonAttrs...)
	telemetry.RecordFloat64Histogram(ctx, r.metrics.temperature, analysis.Message.Temperature, commonAttrs...)
	telemetry.RecordFloat64Histogram(ctx, r.metrics.voltage, analysis.Message.Voltage, commonAttrs...)
	telemetry.RecordFloat64Histogram(ctx, r.metrics.current, analysis.Message.Current, commonAttrs...)
	telemetry.RecordFloat64Histogram(ctx, r.metrics.power, analysis.Message.PowerUsage, commonAttrs...)
	telemetry.RecordFloat64Histogram(ctx, r.metrics.rssi, analysis.Message.RSSI, commonAttrs...)
	telemetry.RecordFloat64Histogram(ctx, r.metrics.snr, analysis.Message.SNR, commonAttrs...)
	telemetry.RecordFloat64Histogram(ctx, r.metrics.packetLoss, analysis.Message.PacketLoss, commonAttrs...)
	telemetry.RecordInt64Histogram(ctx, r.metrics.freeHeap, int64(analysis.Message.FreeHeap), commonAttrs...)
	telemetry.RecordFloat64Histogram(ctx, r.metrics.loopTime, analysis.Message.LoopTimeMS, commonAttrs...)
	telemetry.RecordInt64Histogram(ctx, r.metrics.uptime, analysis.Message.UptimeSeconds, append(commonAttrs, telemetry.StringAttribute("reboot_reason", analysis.Message.RebootReason))...)

	if analysis.SequenceGap > 0 {
		telemetry.AddInt64Counter(ctx, r.metrics.sequenceGapTotal, int64(analysis.SequenceGap), commonAttrs...)
	}
	if analysis.IsDuplicate {
		telemetry.AddInt64Counter(ctx, r.metrics.duplicatesTotal, 1, commonAttrs...)
	}
	if analysis.IsReboot {
		telemetry.AddInt64Counter(
			ctx,
			r.metrics.rebootTotal,
			1,
			append(commonAttrs, telemetry.StringAttribute("reboot_reason", analysis.Message.RebootReason))...,
		)
	}
	for _, record := range digitalTwinMetricRecords(r.config.Environment, analysis) {
		switch record.name {
		case "hardware.shadow.drift":
			telemetry.RecordFloat64Histogram(ctx, r.metrics.shadowDrift, record.value, record.attrs...)
		case "hardware.shadow.status_match":
			telemetry.RecordInt64Histogram(ctx, r.metrics.shadowStatusMatch, int64(record.value), record.attrs...)
		}
	}
}

func (r *Runtime) handleMQTTMessage(_ mqtt.Client, msg mqtt.Message) {
	result := r.evaluateMessage(msg.Topic(), msg.Payload(), r.now().UTC())
	r.recordResult(context.Background(), result)
}

func (r *Runtime) subscriptionFilters() map[string]byte {
	filters := make(map[string]byte, len(r.config.Topics))
	for _, topic := range r.config.Topics {
		filters[topic] = 1
	}
	return filters
}

func parseTopicRoute(topic string) routedTopic {
	if topic == "sensors/thermal" {
		return routedTopic{route: topicRouteTelemetry}
	}

	parts := strings.Split(topic, "/")
	if len(parts) == 3 && parts[0] == "devices" && parts[1] != "" && parts[2] == "telemetry" {
		return routedTopic{route: topicRouteTelemetry, deviceID: parts[1]}
	}
	if len(parts) == 4 && parts[0] == "devices" && parts[1] != "" && parts[2] == "shadow" && parts[3] == "update" {
		return routedTopic{route: topicRouteShadowUpdate, deviceID: parts[1]}
	}

	return routedTopic{route: topicRouteUnknown}
}

func extractPayloadDeviceID(payload []byte) (string, bool, error) {
	var envelope struct {
		DeviceID string `json:"device_id"`
	}
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return "", false, fmt.Errorf("%w: %v", ErrMalformedPayload, err)
	}
	if envelope.DeviceID == "" {
		return "", false, nil
	}
	return envelope.DeviceID, true, nil
}

func invalidMessageResult(topic string, err error) messageResult {
	return messageResult{
		invalid: true,
		err:     err,
		events: []messageEvent{{
			name: "mqtt_payload_invalid",
			attrs: []any{
				"topic", topic,
				"error", err.Error(),
			},
		}},
	}
}

func digitalTwinSyncEvents(analysis Analysis) []messageEvent {
	if !analysis.Twin.ShadowFound {
		return nil
	}

	events := make([]messageEvent, 0, len(analysis.Twin.NumericDrifts)+len(analysis.Twin.StatusComparisons))
	for _, drift := range analysis.Twin.NumericDrifts {
		events = append(events, messageEvent{
			name: "digital_twin_sync_check",
			attrs: []any{
				"device_id", analysis.Message.DeviceID,
				"property", drift.Property,
				"reported", drift.Reported,
				"desired", drift.Desired,
				"drift", drift.Drift,
				"status", string(analysis.Twin.SyncStatus),
			},
		})
	}
	for _, comparison := range analysis.Twin.StatusComparisons {
		events = append(events, messageEvent{
			name: "digital_twin_sync_check",
			attrs: []any{
				"device_id", analysis.Message.DeviceID,
				"property", comparison.Property,
				"reported", comparison.Reported,
				"desired", comparison.Desired,
				"match", comparison.Match,
				"status", string(analysis.Twin.SyncStatus),
			},
		})
	}

	return events
}

func digitalTwinMetricRecords(environment string, analysis Analysis) []twinMetricRecord {
	if !analysis.Twin.ShadowFound {
		return nil
	}

	records := make([]twinMetricRecord, 0, len(analysis.Twin.NumericDrifts)+len(analysis.Twin.StatusComparisons))
	for _, drift := range analysis.Twin.NumericDrifts {
		records = append(records, twinMetricRecord{
			name:  "hardware.shadow.drift",
			value: math.Abs(drift.Drift),
			attrs: []telemetry.Attribute{
				telemetry.StringAttribute("environment", environment),
				telemetry.StringAttribute("device_id", analysis.Message.DeviceID),
				telemetry.StringAttribute("property", drift.Property),
			},
		})
	}
	for _, comparison := range analysis.Twin.StatusComparisons {
		value := 0.0
		if comparison.Match {
			value = 1
		}
		records = append(records, twinMetricRecord{
			name:  "hardware.shadow.status_match",
			value: value,
			attrs: []telemetry.Attribute{
				telemetry.StringAttribute("environment", environment),
				telemetry.StringAttribute("device_id", analysis.Message.DeviceID),
				telemetry.StringAttribute("property", comparison.Property),
			},
		})
	}

	return records
}

func (cfg RuntimeConfig) withDefaults() RuntimeConfig {
	if cfg.BrokerURL == "" {
		cfg.BrokerURL = DefaultBrokerURL
	}
	if cfg.ClientID == "" {
		cfg.ClientID = DefaultClientID
	}
	if cfg.Environment == "" {
		cfg.Environment = "unknown"
	}
	if cfg.StaleAfter <= 0 {
		cfg.StaleAfter = DefaultStaleAfter
	}
	if len(cfg.Topics) == 0 {
		cfg.Topics = append([]string(nil), DefaultTopics...)
	}
	return cfg
}

func newRuntimeMetrics() (runtimeMetrics, error) {
	meter := telemetry.GetMeter("mqtt-ingestor.runtime")

	messagesTotal, err := telemetry.NewInt64Counter(meter, "hardware.telemetry.messages.total", defaultMetricDescription)
	if err != nil {
		return runtimeMetrics{}, fmt.Errorf("create messages counter: %w", err)
	}
	invalidMessagesTotal, err := telemetry.NewInt64Counter(meter, "hardware.telemetry.invalid_messages.total", defaultMetricDescription)
	if err != nil {
		return runtimeMetrics{}, fmt.Errorf("create invalid messages counter: %w", err)
	}
	sequenceGapTotal, err := telemetry.NewInt64Counter(meter, "hardware.telemetry.sequence_gap.total", defaultMetricDescription)
	if err != nil {
		return runtimeMetrics{}, fmt.Errorf("create sequence gap counter: %w", err)
	}
	duplicatesTotal, err := telemetry.NewInt64Counter(meter, "hardware.telemetry.duplicates.total", defaultMetricDescription)
	if err != nil {
		return runtimeMetrics{}, fmt.Errorf("create duplicates counter: %w", err)
	}
	messageAge, err := telemetry.NewInt64Histogram(meter, "hardware.telemetry.message_age", defaultMetricDescription, "ms")
	if err != nil {
		return runtimeMetrics{}, fmt.Errorf("create message age histogram: %w", err)
	}
	rebootTotal, err := telemetry.NewInt64Counter(meter, "hardware.reboot.total", defaultMetricDescription)
	if err != nil {
		return runtimeMetrics{}, fmt.Errorf("create reboot counter: %w", err)
	}
	temperature, err := telemetry.NewFloat64Histogram(meter, "hardware.sensor.temperature", defaultMetricDescription, "C")
	if err != nil {
		return runtimeMetrics{}, fmt.Errorf("create temperature histogram: %w", err)
	}
	voltage, err := telemetry.NewFloat64Histogram(meter, "hardware.sensor.voltage", defaultMetricDescription, "V")
	if err != nil {
		return runtimeMetrics{}, fmt.Errorf("create voltage histogram: %w", err)
	}
	current, err := telemetry.NewFloat64Histogram(meter, "hardware.sensor.current", defaultMetricDescription, "A")
	if err != nil {
		return runtimeMetrics{}, fmt.Errorf("create current histogram: %w", err)
	}
	power, err := telemetry.NewFloat64Histogram(meter, "hardware.sensor.power", defaultMetricDescription, "W")
	if err != nil {
		return runtimeMetrics{}, fmt.Errorf("create power histogram: %w", err)
	}
	rssi, err := telemetry.NewFloat64Histogram(meter, "hardware.radio.rssi", defaultMetricDescription, "dBm")
	if err != nil {
		return runtimeMetrics{}, fmt.Errorf("create rssi histogram: %w", err)
	}
	snr, err := telemetry.NewFloat64Histogram(meter, "hardware.radio.snr", defaultMetricDescription, "dB")
	if err != nil {
		return runtimeMetrics{}, fmt.Errorf("create snr histogram: %w", err)
	}
	packetLoss, err := telemetry.NewFloat64Histogram(meter, "hardware.radio.packet_loss", defaultMetricDescription, "%")
	if err != nil {
		return runtimeMetrics{}, fmt.Errorf("create packet loss histogram: %w", err)
	}
	freeHeap, err := telemetry.NewInt64Histogram(meter, "hardware.runtime.free_heap", defaultMetricDescription, "By")
	if err != nil {
		return runtimeMetrics{}, fmt.Errorf("create free heap histogram: %w", err)
	}
	loopTime, err := telemetry.NewFloat64Histogram(meter, "hardware.runtime.loop_time", defaultMetricDescription, "ms")
	if err != nil {
		return runtimeMetrics{}, fmt.Errorf("create loop time histogram: %w", err)
	}
	uptime, err := telemetry.NewInt64Histogram(meter, "hardware.runtime.uptime", defaultMetricDescription, "s")
	if err != nil {
		return runtimeMetrics{}, fmt.Errorf("create uptime histogram: %w", err)
	}
	shadowDrift, err := telemetry.NewFloat64Histogram(meter, "hardware.shadow.drift", "Digital twin absolute desired versus reported numeric drift", "")
	if err != nil {
		return runtimeMetrics{}, fmt.Errorf("create shadow drift histogram: %w", err)
	}
	shadowStatusMatch, err := telemetry.NewInt64Histogram(meter, "hardware.shadow.status_match", "Digital twin desired versus reported status match", "")
	if err != nil {
		return runtimeMetrics{}, fmt.Errorf("create shadow status match histogram: %w", err)
	}

	return runtimeMetrics{
		messagesTotal:        messagesTotal,
		invalidMessagesTotal: invalidMessagesTotal,
		sequenceGapTotal:     sequenceGapTotal,
		duplicatesTotal:      duplicatesTotal,
		messageAge:           messageAge,
		rebootTotal:          rebootTotal,
		temperature:          temperature,
		voltage:              voltage,
		current:              current,
		power:                power,
		rssi:                 rssi,
		snr:                  snr,
		packetLoss:           packetLoss,
		freeHeap:             freeHeap,
		loopTime:             loopTime,
		uptime:               uptime,
		shadowDrift:          shadowDrift,
		shadowStatusMatch:    shadowStatusMatch,
	}, nil
}
