package kafka

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/shared/observe"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/notifier/internal/application"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/notifier/internal/metrics"
)

type Handler interface {
	Handle(ctx context.Context, value []byte) (application.Result, error)
}

type Consumer struct {
	client  *kgo.Client
	handler Handler
	metrics *metrics.Metrics
	retry   time.Duration
}

func NewConsumer(brokers []string, topic, group string, handler Handler, m *metrics.Metrics) (*Consumer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return nil, err
	}
	return &Consumer{client: client, handler: handler, metrics: m, retry: time.Second}, nil
}

func (c *Consumer) Close() { c.client.Close() }

// Run commits offsets only after every record of a polled batch has been durably handled, so a crash re-delivers records instead of losing them.
func (c *Consumer) Run(ctx context.Context) {
	for ctx.Err() == nil {
		fetches := c.client.PollRecords(ctx, 100)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return
		}
		recreated := map[string]bool{}
		hasErrors := false
		fetches.EachError(func(topic string, partition int32, err error) {
			hasErrors = true
			if errors.Is(err, kerr.UnknownTopicID) {
				recreated[topic] = true
			}
			c.metrics.ConsumerError("poll")
			slog.Warn("kafka fetch error", "topic", topic, "partition", partition, "error", err)
		})
		records := fetches.Records()
		for _, record := range records {
			if !c.process(ctx, record) {
				return
			}
		}
		if len(records) > 0 {
			if err := c.client.CommitRecords(ctx, records...); err != nil {
				if ctx.Err() != nil {
					return
				}
				hasErrors = true
				c.metrics.ConsumerError("commit")
				slog.Warn("kafka offset commit failed", "error", err)
			} else {
				c.metrics.Committed(time.Now())
			}
		}
		for topic := range recreated {
			c.client.PurgeTopicsFromClient(topic)
			c.client.AddConsumeTopics(topic)
			slog.Warn("rejoining recreated payment event topic", "topic", topic)
		}
		if hasErrors {
			select {
			case <-ctx.Done():
				return
			case <-time.After(c.retry):
			}
		}
	}
}

func (c *Consumer) process(ctx context.Context, record *kgo.Record) bool {
	headers := map[string]string{}
	for _, header := range record.Headers {
		key := strings.ToLower(header.Key)
		if key == "traceparent" || key == "tracestate" || key == "tracingspancontext" {
			headers[key] = string(header.Value)
		}
	}
	if raw, ok := headers["tracingspancontext"]; ok {
		ctx = observe.RestoreContext(ctx, &raw)
	}
	ctx, span := observe.StartConsumer(ctx, headers)
	defer span.End()
	for {
		result, err := c.handler.Handle(ctx, record.Value)
		switch {
		case err == nil:
			c.metrics.Consumed(resultLabel(result))
			c.metrics.EventAge(time.Since(record.Timestamp))
			return true
		case errors.Is(err, application.ErrMalformed):
			c.metrics.Consumed("malformed")
			slog.ErrorContext(ctx, "skipping malformed event", "partition", record.Partition, "offset", record.Offset)
			return true
		}
		if ctx.Err() != nil {
			return false
		}
		c.metrics.ConsumerError("handle")
		slog.ErrorContext(ctx, "handle event failed", "partition", record.Partition, "offset", record.Offset, "error", err)
		select {
		case <-ctx.Done():
			return false
		case <-time.After(c.retry):
		}
	}
}

func resultLabel(result application.Result) string {
	switch result {
	case application.ResultRecorded:
		return "recorded"
	case application.ResultDuplicate:
		return "duplicate"
	default:
		return "skipped"
	}
}
