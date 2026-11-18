package vault

import (
	"context"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/edge/internal/adapters/grpcerr"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/edge/internal/application"
	vaultv1 "github.com/phuchoang2603/payment-gateway-fraud-detection/shared/proto/vault/v1"
	"google.golang.org/grpc"
)

type Client struct{ client vaultv1.VaultServiceClient }

func New(conn grpc.ClientConnInterface) *Client {
	return &Client{client: vaultv1.NewVaultServiceClient(conn)}
}

func (c *Client) Tokenize(ctx context.Context, integrationID, cardNumber string) (application.Token, error) {
	response, err := c.client.TokenizeCard(ctx, &vaultv1.TokenizeCardRequest{IntegrationId: integrationID, CardNumber: cardNumber})
	if err != nil {
		return application.Token{}, grpcerr.Map(err, "vault tokenize")
	}
	return application.Token{
		Value:  response.GetToken(),
		Method: application.PaymentMethod{Type: response.GetMetadata().GetType(), Fingerprint: response.GetMetadata().GetFingerprint()},
	}, nil
}

func (c *Client) Describe(ctx context.Context, integrationID, token string) (application.PaymentMethod, error) {
	response, err := c.client.DescribeMethod(ctx, &vaultv1.DescribeMethodRequest{IntegrationId: integrationID, Token: token})
	if err != nil {
		return application.PaymentMethod{}, grpcerr.Map(err, "vault describe")
	}
	return application.PaymentMethod{Type: response.GetMetadata().GetType(), Fingerprint: response.GetMetadata().GetFingerprint()}, nil
}
