package vault

import (
	"context"
	"fmt"
	"time"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/shared/observe"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/processor-simulator/internal/application"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/processor-simulator/internal/config"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/processor-simulator/internal/domain"
	vaultv1 "github.com/phuchoang2603/payment-gateway-fraud-detection/shared/proto/vault/v1"
)

type Client struct {
	conn    *grpc.ClientConn
	client  vaultv1.ProcessingServiceClient
	timeout time.Duration
}

func NewClient(cfg config.Vault) (*Client, error) {
	conn, err := grpc.NewClient(cfg.Address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithStatsHandler(otelgrpc.NewClientHandler(otelgrpc.WithFilter(observe.RPCFilter))), grpc.WithChainUnaryInterceptor(observe.UnaryClientInterceptor()))
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn, client: vaultv1.NewProcessingServiceClient(conn), timeout: cfg.Timeout}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) Resolve(ctx context.Context, integrationID, token string) (domain.Behavior, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	fixture, err := c.client.ResolveForProcessing(ctx, &vaultv1.ResolveRequest{IntegrationId: integrationID, Token: token})
	if err != nil {
		switch status.Code(err) {
		case codes.NotFound, codes.InvalidArgument:
			return "", application.ErrUnknownToken
		default:
			return "", fmt.Errorf("%w: %s", application.ErrVaultUnavailable, status.Code(err))
		}
	}
	switch fixture.GetBehavior() {
	case vaultv1.Behavior_BEHAVIOR_APPROVE:
		return domain.BehaviorApprove, nil
	case vaultv1.Behavior_BEHAVIOR_INSUFFICIENT_FUNDS:
		return domain.BehaviorInsufficientFunds, nil
	case vaultv1.Behavior_BEHAVIOR_ISSUER_DECLINE:
		return domain.BehaviorIssuerDecline, nil
	case vaultv1.Behavior_BEHAVIOR_INVALID:
		return domain.BehaviorInvalid, nil
	default:
		return "", fmt.Errorf("%w: unsupported behavior", application.ErrVaultUnavailable)
	}
}
