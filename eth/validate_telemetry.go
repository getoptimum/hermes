package eth

import (
	"context"
	"encoding/hex"
	"log/slog"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/probe-lab/hermes/host"
	"github.com/probe-lab/hermes/tele"
)

// Distinct from HANDLE_MESSAGE so consumers can tell an observed message from a
// suppressed one.
const eventTypeWithheldMessage = "WITHHELD_MESSAGE"

// Nil until initValidationMetrics runs, so every record site is nil-checked.
type validationMeters struct {
	results         metric.Int64Counter
	duration        metric.Float64Histogram
	withheldDropped metric.Int64Counter
}

func (p *PubSub) initValidationMetrics(meter metric.Meter) error {
	if meter == nil {
		return nil
	}

	results, err := meter.Int64Counter(
		"validation_result_total",
		metric.WithDescription("Gossip validation outcomes by topic, result and reason"),
	)
	if err != nil {
		return err
	}

	duration, err := meter.Float64Histogram(
		"validation_duration_seconds",
		metric.WithDescription("Time spent in the gossip validator, which is time added to propagation"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return err
	}

	withheldDropped, err := meter.Int64Counter(
		"validation_withheld_dropped_total",
		metric.WithDescription("Withheld-message events dropped because the data stream was not keeping up"),
	)
	if err != nil {
		return err
	}

	p.meters = &validationMeters{
		results:         results,
		duration:        duration,
		withheldDropped: withheldDropped,
	}
	return nil
}

// localID is nil-safe so the validator can be exercised without a libp2p host.
func (p *PubSub) localID() peer.ID {
	if p.host == nil {
		return ""
	}
	return p.host.ID()
}

func validationResultLabel(result pubsub.ValidationResult) string {
	switch result {
	case pubsub.ValidationAccept:
		return "accept"
	case pubsub.ValidationReject:
		return "reject"
	case pubsub.ValidationIgnore:
		return "ignore"
	default:
		return "unknown"
	}
}

// finishValidation records the outcome, and still emits a trace event for
// anything withheld: suppressing a message must not erase the record of it.
func (p *PubSub) finishValidation(
	ctx context.Context,
	msg *pubsub.Message,
	result pubsub.ValidationResult,
	reason string,
	cause error,
	start time.Time,
) pubsub.ValidationResult {
	label := validationResultLabel(result)
	topic := msg.GetTopic()

	if p.meters != nil {
		attrs := metric.WithAttributes(
			attribute.String("topic", topic),
			attribute.String("result", label),
			attribute.String("reason", reason),
		)
		p.meters.results.Add(ctx, 1, attrs)
		p.meters.duration.Record(ctx, time.Since(start).Seconds(), metric.WithAttributes(
			attribute.String("topic", topic),
			attribute.String("result", label),
		))
	}

	if result != pubsub.ValidationAccept {
		p.emitWithheldMessage(ctx, msg, label, reason, cause)
	}

	return result
}

// withheldQueueSize bounds the backlog of withheld-message events. Small on
// purpose: this exists to keep the validator off the data stream's critical
// path, not to guarantee delivery.
const withheldQueueSize = 256

// serveWithheldEvents drains withheld-message events into the data stream. Its own
// goroutine because PutRecord can block indefinitely, and the validator runs on
// gossipsub's workers: a stalled sink there would starve validation on every topic.
func (p *PubSub) serveWithheldEvents(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case evt := <-p.withheldC:
			if err := p.cfg.DataStream.PutRecord(ctx, evt); err != nil {
				slog.Warn("failed putting withheld message event",
					"topic", evt.Topic, tele.LogAttrError(err))
			}
		}
	}
}

// Built from raw gossip metadata rather than the decoded object, because the most
// interesting case is the one that failed to decode.
func (p *PubSub) emitWithheldMessage(ctx context.Context, msg *pubsub.Message, result, reason string, cause error) {
	payload := map[string]any{
		"PeerID":  p.localID().String(),
		"Topic":   msg.GetTopic(),
		"MsgID":   hex.EncodeToString([]byte(msg.ID)),
		"MsgSize": len(msg.Data),
		"From":    msg.ReceivedFrom.String(),
		"Result":  result,
		"Reason":  reason,
	}
	if cause != nil {
		payload["Error"] = cause.Error()
	}

	evt := &host.TraceEvent{
		Type:      eventTypeWithheldMessage,
		Topic:     msg.GetTopic(),
		PeerID:    p.localID(),
		Timestamp: time.Now(),
		Payload:   payload,
	}

	// Non-blocking by design: see serveWithheldEvents. Dropping an observation is
	// strictly better than stalling a validation worker, and the drop is counted.
	select {
	case p.withheldC <- evt:
	default:
		if p.meters != nil {
			p.meters.withheldDropped.Add(ctx, 1, metric.WithAttributes(
				attribute.String("topic", msg.GetTopic()),
				attribute.String("reason", reason),
			))
		}
		slog.Debug("dropped withheld message event, queue full",
			"topic", msg.GetTopic(), "reason", reason)
	}
}
