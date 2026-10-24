package grpc

import (
	"context"
	"errors"
	"net"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/shared/observe"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/payments/internal/application"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/payments/internal/domain"
	paymentsv1 "github.com/phuchoang2603/payment-gateway-fraud-detection/shared/proto/payments/v1"
)

type Server struct {
	paymentsv1.UnimplementedPaymentServiceServer
	service *application.Service
}

func NewServer(service *application.Service) *Server {
	return &Server{service: service}
}

func (s *Server) CreatePayment(ctx context.Context, request *paymentsv1.CreatePaymentRequest) (*paymentsv1.Payment, error) {
	payment, err := s.service.CreatePayment(ctx, request)
	return payment, toStatus(err)
}

func (s *Server) StartAttempt(ctx context.Context, request *paymentsv1.StartAttemptRequest) (*paymentsv1.AttemptResult, error) {
	result, err := s.service.StartAttempt(ctx, request)
	return result, toStatus(err)
}

func (s *Server) GetPayment(ctx context.Context, request *paymentsv1.GetPaymentRequest) (*paymentsv1.Payment, error) {
	payment, err := s.service.GetPayment(ctx, request)
	return payment, toStatus(err)
}

func (s *Server) GetCheckoutSession(ctx context.Context, request *paymentsv1.GetCheckoutSessionRequest) (*paymentsv1.Payment, error) {
	payment, err := s.service.GetCheckoutSession(ctx, request)
	return payment, toStatus(err)
}

func toStatus(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok && status.Code(err) != codes.Unknown {
		return err
	}
	var netErr net.Error
	switch {
	case errors.Is(err, application.ErrInvalidRequest), errors.Is(err, domain.ErrInvalidSnapshot):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, application.ErrIdempotencyConflict), errors.Is(err, application.ErrOrderConflict):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, application.ErrNotFound):
		return status.Error(codes.NotFound, "not found")
	case errors.Is(err, application.ErrPaymentNotOpen), errors.Is(err, application.ErrAttemptActive):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, application.ErrAccessDenied):
		return status.Error(codes.PermissionDenied, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "deadline exceeded")
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "request canceled")
	case errors.As(err, &netErr):
		return status.Error(codes.Unavailable, "dependency unavailable")
	}
	return status.Error(codes.Internal, "internal error")
}

func CorrelationInterceptor() grpc.UnaryServerInterceptor {
	observeRequest := observe.UnaryServerInterceptor()
	return func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		return observeRequest(ctx, request, info, func(ctx context.Context, request any) (any, error) {
			return handler(application.WithCorrelationID(ctx, observe.RequestID(ctx)), request)
		})
	}
}
