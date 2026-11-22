package broker

import (
	"context"

	riftv1 "github.com/jayyy255/rift/api/gen/rift/v1"
	"github.com/jayyy255/rift/internal/version"
)

// clusterService answers cluster diagnostics. Until metadata consensus exists
// (milestone R2) the only member a broker knows about is itself.
type clusterService struct {
	riftv1.UnimplementedClusterServiceServer

	nodeID  string
	address string
}

func (s *clusterService) GetStatus(context.Context, *riftv1.GetStatusRequest) (*riftv1.GetStatusResponse, error) {
	return &riftv1.GetStatusResponse{
		NodeId:  s.nodeID,
		Version: version.Version,
		Brokers: []*riftv1.Broker{{
			NodeId:  s.nodeID,
			Address: s.address,
			State:   riftv1.BrokerState_BROKER_STATE_ALIVE,
		}},
	}, nil
}
