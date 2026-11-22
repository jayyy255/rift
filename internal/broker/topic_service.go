package broker

import (
	"context"

	riftv1 "github.com/jayyy255/rift/api/gen/rift/v1"
	"github.com/jayyy255/rift/internal/topics"
)

type topicService struct {
	riftv1.UnimplementedTopicServiceServer
	b *Broker
}

func topicInfo(m topics.Metadata) *riftv1.TopicInfo {
	return &riftv1.TopicInfo{
		Name:              m.Name,
		Partitions:        m.Partitions,
		ReplicationFactor: m.ReplicationFactor,
		RetentionMs:       m.RetentionMs,
		RetentionBytes:    m.RetentionBytes,
		CreatedAtMs:       m.CreatedAtMs,
	}
}

func (s *topicService) CreateTopic(_ context.Context, req *riftv1.CreateTopicRequest) (*riftv1.CreateTopicResponse, error) {
	t, err := s.b.topics.Create(req.GetName(), topics.CreateOptions{
		Partitions:        req.GetPartitions(),
		ReplicationFactor: req.GetReplicationFactor(),
		RetentionMs:       req.GetRetentionMs(),
		RetentionBytes:    req.GetRetentionBytes(),
	})
	if err != nil {
		return nil, toStatus(err)
	}
	return &riftv1.CreateTopicResponse{Topic: topicInfo(t.Metadata())}, nil
}

func (s *topicService) DeleteTopic(_ context.Context, req *riftv1.DeleteTopicRequest) (*riftv1.DeleteTopicResponse, error) {
	if err := s.b.topics.Delete(req.GetName()); err != nil {
		return nil, toStatus(err)
	}
	if err := s.b.offsets.DeleteTopic(req.GetName()); err != nil {
		// The topic is gone; stale offsets are harmless and are not reported
		// for missing topics, so log rather than fail the request.
		s.b.log.Warn("could not remove consumer offsets of deleted topic", "topic", req.GetName(), "error", err)
	}
	return &riftv1.DeleteTopicResponse{}, nil
}

func (s *topicService) ListTopics(context.Context, *riftv1.ListTopicsRequest) (*riftv1.ListTopicsResponse, error) {
	resp := &riftv1.ListTopicsResponse{}
	for _, t := range s.b.topics.List() {
		resp.Topics = append(resp.Topics, topicInfo(t.Metadata()))
	}
	return resp, nil
}

func (s *topicService) DescribeTopic(_ context.Context, req *riftv1.DescribeTopicRequest) (*riftv1.DescribeTopicResponse, error) {
	t, err := s.b.topics.Get(req.GetName())
	if err != nil {
		return nil, toStatus(err)
	}
	resp := &riftv1.DescribeTopicResponse{Topic: topicInfo(t.Metadata())}
	for p := range t.Partitions() {
		l, err := t.Log(p)
		if err != nil {
			return nil, toStatus(err)
		}
		st := l.Stats()
		resp.Partitions = append(resp.Partitions, &riftv1.PartitionInfo{
			Partition:      p,
			LogStartOffset: st.StartOffset,
			HighWatermark:  st.EndOffset,
			SyncedOffset:   st.SyncedOffset,
			SizeBytes:      st.SizeBytes,
			Segments:       int32(st.Segments),
			Leader:         s.b.cfg.NodeID,
		})
	}
	return resp, nil
}
