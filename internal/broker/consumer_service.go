package broker

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	riftv1 "github.com/jayyy255/rift/api/gen/rift/v1"
	"github.com/jayyy255/rift/internal/logstore"
	"github.com/jayyy255/rift/internal/names"
	"github.com/jayyy255/rift/internal/topics"
)

const (
	defaultFetchBytes   = 1 << 20
	defaultFetchRecords = 500
	maxFetchRecords     = 10000
)

type consumerService struct {
	riftv1.UnimplementedConsumerServiceServer
	b *Broker
}

// Fetch reads from the requested partitions. If none has records it waits,
// up to the requested (and capped) time, for an append to the topic.
func (s *consumerService) Fetch(ctx context.Context, req *riftv1.FetchRequest) (*riftv1.FetchResponse, error) {
	s.b.metrics.fetchRequests.Inc()
	t, err := s.b.topics.Get(req.GetTopic())
	if err != nil {
		return nil, toStatus(err)
	}
	if len(req.GetPartitions()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "fetch request names no partitions")
	}
	maxBytes := int(req.GetMaxBytes())
	if maxBytes <= 0 {
		maxBytes = defaultFetchBytes
	}
	maxBytes = min(maxBytes, s.b.cfg.MaxFetchBytes)
	maxRecords := int(req.GetMaxRecordsPerPartition())
	if maxRecords <= 0 {
		maxRecords = defaultFetchRecords
	}
	maxRecords = min(maxRecords, maxFetchRecords)
	wait := min(time.Duration(req.GetMaxWaitMs())*time.Millisecond, s.b.cfg.MaxFetchWait)

	var timer <-chan time.Time
	if wait > 0 {
		tm := time.NewTimer(wait)
		defer tm.Stop()
		timer = tm.C
	}
	for {
		changed := t.Changed() // taken before reading so no append is missed
		resp, n, err := s.readOnce(t, req, maxBytes, maxRecords)
		if err != nil {
			return nil, toStatus(err)
		}
		if n > 0 || wait <= 0 || hasPartitionError(resp) {
			s.b.metrics.fetchRecords.WithLabelValues(req.GetTopic()).Add(float64(n))
			return resp, nil
		}
		select {
		case <-changed:
			if t.Deleted() {
				return nil, toStatus(topics.ErrTopicNotFound)
			}
		case <-timer:
			return resp, nil
		case <-ctx.Done():
			return nil, toStatus(ctx.Err())
		}
	}
}

func (s *consumerService) readOnce(t *topics.Topic, req *riftv1.FetchRequest, maxBytes, maxRecords int) (*riftv1.FetchResponse, int, error) {
	resp := &riftv1.FetchResponse{}
	remaining, count := maxBytes, 0
	for _, pp := range req.GetPartitions() {
		pr := &riftv1.PartitionRecords{Partition: pp.GetPartition()}
		resp.Partitions = append(resp.Partitions, pr)
		l, err := t.Log(pp.GetPartition())
		if err != nil {
			pr.Error, pr.ErrorMessage = riftv1.PartitionError_PARTITION_ERROR_UNKNOWN_PARTITION, err.Error()
			continue
		}
		if remaining > 0 || count == 0 {
			recs, err := t.Read(pp.GetPartition(), pp.GetOffset(), max(remaining, 1), maxRecords)
			switch {
			case errors.Is(err, logstore.ErrOffsetOutOfRange):
				pr.Error, pr.ErrorMessage = riftv1.PartitionError_PARTITION_ERROR_OFFSET_OUT_OF_RANGE, err.Error()
			case errors.Is(err, logstore.ErrCorrupt):
				s.b.metrics.corruptReads.Inc()
				s.b.log.Error("corrupt record on read", "topic", req.GetTopic(), "partition", pp.GetPartition(),
					"offset", pp.GetOffset(), "error", err)
				pr.Error, pr.ErrorMessage = riftv1.PartitionError_PARTITION_ERROR_CORRUPT_DATA, err.Error()
			case err != nil:
				return nil, 0, err
			}
			for _, r := range recs {
				pr.Records = append(pr.Records, &riftv1.Record{
					Offset: r.Offset, TimestampMs: r.TimestampMs, Key: r.Key, Value: r.Value, Headers: toPBHeaders(r.Headers),
				})
				remaining -= r.EncodedSize()
			}
			count += len(recs)
		}
		pr.HighWatermark = l.EndOffset()
		pr.LogStartOffset = l.StartOffset()
	}
	return resp, count, nil
}

func hasPartitionError(resp *riftv1.FetchResponse) bool {
	for _, p := range resp.GetPartitions() {
		if p.GetError() != riftv1.PartitionError_PARTITION_ERROR_UNSPECIFIED {
			return true
		}
	}
	return false
}

func (s *consumerService) CommitOffsets(_ context.Context, req *riftv1.CommitOffsetsRequest) (*riftv1.CommitOffsetsResponse, error) {
	s.b.metrics.commits.Inc()
	if err := names.Validate("group", req.GetGroup()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	t, err := s.b.topics.Get(req.GetTopic())
	if err != nil {
		return nil, toStatus(err)
	}
	if len(req.GetOffsets()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "commit request has no offsets")
	}
	offs := make(map[int32]int64, len(req.GetOffsets()))
	for _, po := range req.GetOffsets() {
		l, err := t.Log(po.GetPartition())
		if err != nil {
			return nil, toStatus(err)
		}
		if end := l.EndOffset(); po.GetOffset() < 0 || po.GetOffset() > end {
			return nil, status.Error(codes.OutOfRange,
				fmt.Sprintf("partition %d: offset %d is outside [0, %d]", po.GetPartition(), po.GetOffset(), end))
		}
		offs[po.GetPartition()] = po.GetOffset()
	}
	if err := s.b.offsets.Commit(req.GetGroup(), req.GetTopic(), offs); err != nil {
		return nil, toStatus(err)
	}
	return &riftv1.CommitOffsetsResponse{}, nil
}

func (s *consumerService) GetCommittedOffsets(_ context.Context, req *riftv1.GetCommittedOffsetsRequest) (*riftv1.GetCommittedOffsetsResponse, error) {
	if err := names.Validate("group", req.GetGroup()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if _, err := s.b.topics.Get(req.GetTopic()); err != nil {
		return nil, toStatus(err)
	}
	resp := &riftv1.GetCommittedOffsetsResponse{}
	for p, off := range s.b.offsets.Get(req.GetGroup(), req.GetTopic()) {
		resp.Offsets = append(resp.Offsets, &riftv1.PartitionOffset{Partition: p, Offset: off})
	}
	sort.Slice(resp.Offsets, func(i, j int) bool { return resp.Offsets[i].Partition < resp.Offsets[j].Partition })
	return resp, nil
}
