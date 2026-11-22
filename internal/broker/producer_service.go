package broker

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	riftv1 "github.com/jayyy255/rift/api/gen/rift/v1"
	"github.com/jayyy255/rift/internal/logstore"
)

type producerService struct {
	riftv1.UnimplementedProducerServiceServer
	b *Broker
}

// Publish appends a batch. Records are grouped by partition (preserving
// request order within each partition), appended, and, for FSYNCED
// durability, fsynced before the acknowledgment is returned.
func (s *producerService) Publish(ctx context.Context, req *riftv1.PublishRequest) (*riftv1.PublishResponse, error) {
	start := time.Now()
	durability := req.GetDurability()
	if durability == riftv1.Durability_DURABILITY_UNSPECIFIED {
		durability = riftv1.Durability_DURABILITY_FSYNCED
	}
	dlabel := durabilityLabel(durability)
	resp, err := s.publish(ctx, req, durability)
	result := "ok"
	if err != nil {
		result = "error"
	}
	s.b.metrics.produceRequests.WithLabelValues(dlabel, result).Inc()
	if err == nil {
		s.b.metrics.produceLatency.WithLabelValues(dlabel).Observe(time.Since(start).Seconds())
	}
	return resp, err
}

func (s *producerService) reject(reason string, code codes.Code, msg string) error {
	s.b.metrics.rejected.WithLabelValues(reason).Inc()
	return status.Error(code, msg)
}

func (s *producerService) publish(ctx context.Context, req *riftv1.PublishRequest, durability riftv1.Durability) (*riftv1.PublishResponse, error) {
	cfg := s.b.cfg
	if durability != riftv1.Durability_DURABILITY_WRITTEN && durability != riftv1.Durability_DURABILITY_FSYNCED {
		return nil, s.reject("invalid", codes.InvalidArgument, fmt.Sprintf("unsupported durability %v", durability))
	}
	n := len(req.GetRecords())
	if n == 0 {
		return nil, s.reject("invalid", codes.InvalidArgument, "publish request has no records")
	}
	if n > maxBatchRecords {
		return nil, s.reject("invalid", codes.InvalidArgument, fmt.Sprintf("batch has %d records; the limit is %d", n, maxBatchRecords))
	}
	if s.b.disk.isCritical() {
		return nil, s.reject("disk_full", codes.ResourceExhausted, "disk usage is above the critical watermark; publishes are rejected")
	}
	t, err := s.b.topics.Get(req.GetTopic())
	if err != nil {
		return nil, toStatus(err)
	}

	type group struct {
		indexes []int
		records []logstore.Record
	}
	groups := map[int32]*group{}
	var order []int32
	total := 0
	for i, pr := range req.GetRecords() {
		rec := logstore.Record{Key: pr.Key, Value: pr.GetValue(), Headers: fromPBHeaders(pr.GetHeaders())}
		size := rec.EncodedSize()
		if size > cfg.MaxRecordBytes {
			return nil, s.reject("too_large", codes.InvalidArgument,
				fmt.Sprintf("record %d is %d bytes; the limit is %d", i, size, cfg.MaxRecordBytes))
		}
		total += size
		var p int32
		if pr.Partition != nil {
			p = pr.GetPartition()
			if p < 0 || p >= t.Partitions() {
				return nil, s.reject("invalid", codes.InvalidArgument,
					fmt.Sprintf("record %d: partition %d does not exist (topic has %d)", i, p, t.Partitions()))
			}
		} else {
			p = t.ChoosePartition(pr.Key)
		}
		g := groups[p]
		if g == nil {
			g = &group{}
			groups[p] = g
			order = append(order, p)
		}
		g.indexes = append(g.indexes, i)
		g.records = append(g.records, rec)
	}
	if total > cfg.MaxBatchBytes {
		return nil, s.reject("too_large", codes.InvalidArgument,
			fmt.Sprintf("batch is %d bytes; the limit is %d", total, cfg.MaxBatchBytes))
	}
	if err := ctx.Err(); err != nil {
		return nil, toStatus(err)
	}

	acks := make([]*riftv1.RecordAck, n)
	ends := make(map[int32]int64, len(order))
	for _, p := range order {
		g := groups[p]
		first, ts, err := t.Append(p, g.records)
		if err != nil {
			return nil, toStatus(err)
		}
		for j, idx := range g.indexes {
			acks[idx] = &riftv1.RecordAck{Partition: p, Offset: first + int64(j), TimestampMs: ts}
		}
		ends[p] = first + int64(len(g.records))
	}
	if durability == riftv1.Durability_DURABILITY_FSYNCED {
		for _, p := range order {
			syncStart := time.Now()
			if err := t.Sync(p, ends[p]); err != nil {
				return nil, toStatus(err)
			}
			s.b.metrics.fsyncLatency.Observe(time.Since(syncStart).Seconds())
		}
	}
	s.b.metrics.produceRecords.WithLabelValues(req.GetTopic()).Add(float64(n))
	s.b.metrics.produceBytes.WithLabelValues(req.GetTopic()).Add(float64(total))
	return &riftv1.PublishResponse{Acks: acks, Durability: durability}, nil
}

func durabilityLabel(d riftv1.Durability) string {
	switch d {
	case riftv1.Durability_DURABILITY_WRITTEN:
		return "written"
	case riftv1.Durability_DURABILITY_FSYNCED:
		return "fsynced"
	default:
		return "invalid"
	}
}

func fromPBHeaders(hs []*riftv1.Header) []logstore.Header {
	if len(hs) == 0 {
		return nil
	}
	out := make([]logstore.Header, len(hs))
	for i, h := range hs {
		out[i] = logstore.Header{Key: h.GetKey(), Value: h.GetValue()}
	}
	return out
}

func toPBHeaders(hs []logstore.Header) []*riftv1.Header {
	if len(hs) == 0 {
		return nil
	}
	out := make([]*riftv1.Header, len(hs))
	for i, h := range hs {
		out[i] = &riftv1.Header{Key: h.Key, Value: h.Value}
	}
	return out
}
