package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/kirpepa/incident-flow/internal/domain"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	streamName        = "INCIDENTFLOW"
	correlatorDurable = "incident-correlator-v1"
)

type Bus struct {
	connection *nats.Conn
	stream     jetstream.JetStream
	logger     *slog.Logger
}

func Open(ctx context.Context, url string, logger *slog.Logger) (*Bus, error) {
	connection, err := nats.Connect(
		url,
		nats.Name("incident-flow"),
		nats.Timeout(5*time.Second),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("connect to NATS: %w", err)
	}
	stream, err := jetstream.New(connection)
	if err != nil {
		connection.Close()
		return nil, fmt.Errorf("create JetStream client: %w", err)
	}
	bus := &Bus{connection: connection, stream: stream, logger: logger}
	if err := bus.ensureTopology(ctx); err != nil {
		connection.Close()
		return nil, err
	}
	return bus, nil
}

func (b *Bus) ensureTopology(ctx context.Context) error {
	if _, err := b.stream.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:        streamName,
		Description: "IncidentFlow domain events",
		Subjects:    []string{"alerts.>"},
		Retention:   jetstream.WorkQueuePolicy,
		MaxBytes:    512 << 20,
		Discard:     jetstream.DiscardNew,
		Storage:     jetstream.FileStorage,
		Replicas:    1,
		Duplicates:  10 * time.Minute,
	}); err != nil {
		return fmt.Errorf("ensure JetStream stream: %w", err)
	}
	if _, err := b.stream.CreateOrUpdateConsumer(ctx, streamName, jetstream.ConsumerConfig{
		Name:          correlatorDurable,
		Durable:       correlatorDurable,
		Description:   "Correlates raw alerts into incidents",
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       30 * time.Second,
		MaxDeliver:    -1,
		FilterSubject: domain.AlertReceivedTopic,
		MaxAckPending: 128,
	}); err != nil {
		return fmt.Errorf("ensure correlator consumer: %w", err)
	}
	return nil
}

func (b *Bus) Close() {
	_ = b.connection.Drain()
	b.connection.Close()
}

func (b *Bus) Healthy() bool {
	return b.connection.IsConnected()
}

func (b *Bus) Publish(ctx context.Context, event domain.OutboxEvent) error {
	message := nats.NewMsg(event.Topic)
	message.Data = event.Payload
	message.Header.Set("IncidentFlow-Tenant-ID", event.TenantID.String())
	message.Header.Set("IncidentFlow-Partition-Key", event.Key)
	if _, err := b.stream.PublishMsg(ctx, message, jetstream.WithMsgID(event.ID.String())); err != nil {
		return fmt.Errorf("publish %s: %w", event.ID, err)
	}
	return nil
}

func (b *Bus) ConsumeAlerts(ctx context.Context, handler func(context.Context, domain.EventEnvelope) error) error {
	consumer, err := b.stream.Consumer(ctx, streamName, correlatorDurable)
	if err != nil {
		return fmt.Errorf("open correlator consumer: %w", err)
	}
	consumeContext, err := consumer.Consume(func(message jetstream.Msg) {
		var envelope domain.EventEnvelope
		if err := json.Unmarshal(message.Data(), &envelope); err != nil {
			b.logger.Error("discarding malformed event", "error", err)
			_ = message.TermWithReason("malformed JSON envelope")
			return
		}
		if err := envelope.Validate(); err != nil {
			b.logger.Error("discarding invalid event envelope", "event_id", envelope.ID, "type", envelope.Type, "error", err)
			_ = message.TermWithReason("invalid event envelope")
			return
		}

		handlerContext, cancel := context.WithTimeout(ctx, 20*time.Second)
		defer cancel()
		if err := handler(handlerContext, envelope); err != nil {
			delay := redeliveryDelay(message)
			b.logger.Error("alert handler failed", "event_id", envelope.ID, "retry_after", delay, "error", err)
			_ = message.NakWithDelay(delay)
			return
		}
		if err := message.DoubleAck(handlerContext); err != nil {
			b.logger.Warn("JetStream ack confirmation failed", "event_id", envelope.ID, "error", err)
		}
	}, jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
		b.logger.Error("JetStream consumer error", "error", err)
	}))
	if err != nil {
		return fmt.Errorf("start alert consumer: %w", err)
	}

	<-ctx.Done()
	consumeContext.Stop()
	<-consumeContext.Closed()
	return nil
}

func redeliveryDelay(message jetstream.Msg) time.Duration {
	delivery := uint64(1)
	if metadata, err := message.Metadata(); err == nil && metadata.NumDelivered > 0 {
		delivery = metadata.NumDelivered
	}
	exponent := min(delivery-1, 5)
	return time.Duration(1<<exponent) * time.Second
}
