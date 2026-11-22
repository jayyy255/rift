package broker

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/jayyy255/rift/internal/logstore"
	"github.com/jayyy255/rift/internal/topics"
)

// toStatus maps internal errors onto gRPC status codes. Clients rely on the
// codes: Unavailable and ResourceExhausted are retryable; NotFound,
// InvalidArgument, OutOfRange and DataLoss are not.
func toStatus(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	var inv *topics.InvalidArgumentError
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return status.FromContextError(err).Err()
	case errors.Is(err, topics.ErrTopicNotFound), errors.Is(err, topics.ErrPartitionNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, topics.ErrTopicExists):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, topics.ErrReplicationUnavailable):
		return status.Error(codes.Unimplemented, err.Error())
	case errors.As(err, &inv), errors.Is(err, logstore.ErrRecordTooLarge):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, logstore.ErrOffsetOutOfRange):
		return status.Error(codes.OutOfRange, err.Error())
	case errors.Is(err, logstore.ErrCorrupt):
		return status.Error(codes.DataLoss, err.Error())
	case errors.Is(err, topics.ErrClosed), errors.Is(err, logstore.ErrClosed):
		return status.Error(codes.Unavailable, "broker is shutting down")
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
