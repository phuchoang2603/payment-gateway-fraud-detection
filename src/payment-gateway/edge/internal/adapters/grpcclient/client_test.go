package grpcclient_test

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/edge/internal/adapters/grpcclient"
	vaultadapter "github.com/phuchoang2603/payment-gateway-fraud-detection/edge/internal/adapters/vault"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/edge/internal/domain"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/edge/internal/requestid"
	vaultv1 "github.com/phuchoang2603/payment-gateway-fraud-detection/shared/proto/vault/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type recordingVault struct {
	vaultv1.UnimplementedVaultServiceServer
	requestID chan string
	block     bool
}

func (v recordingVault) DescribeMethod(ctx context.Context, _ *vaultv1.DescribeMethodRequest) (*vaultv1.DescribeMethodResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	v.requestID <- first(md.Get("x-request-id"))
	if v.block {
		<-ctx.Done()
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &vaultv1.DescribeMethodResponse{Metadata: &vaultv1.MethodMetadata{Type: "card", Fingerprint: "fp"}}, nil
}

func first(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func startVault(t *testing.T, vault recordingVault) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	vaultv1.RegisterVaultServiceServer(server, vault)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	return listener.Addr().String()
}

func TestVaultCallsPropagateTheRequestID(t *testing.T) {
	vault := recordingVault{requestID: make(chan string, 4)}
	address := startVault(t, vault)

	conn, err := grpcclient.Dial(address, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	client := vaultadapter.New(conn)
	method, err := client.Describe(requestid.With(context.Background(), "req-9"), "int", "tok")
	if err != nil || method.Fingerprint != "fp" {
		t.Fatalf("describe = %+v, %v", method, err)
	}
	if got := <-vault.requestID; got != "req-9" {
		t.Fatalf("request id = %q", got)
	}
}

func TestEveryOutboundCallHasAGatewaySideDeadline(t *testing.T) {
	vault := recordingVault{requestID: make(chan string, 4), block: true}
	address := startVault(t, vault)
	conn, err := grpcclient.Dial(address, 150*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	start := time.Now()
	_, err = vaultadapter.New(conn).Describe(context.Background(), "int", "tok")
	if err == nil || time.Since(start) > 3*time.Second {
		t.Fatalf("err = %v after %s", err, time.Since(start))
	}
	if !isTimeout(err) {
		t.Fatalf("err = %v, want a timeout", err)
	}
}

func isTimeout(err error) bool {
	return status.Code(err) == codes.DeadlineExceeded || errorsIs(err, domain.ErrTimeout)
}

func errorsIs(err, target error) bool { return errors.Is(err, target) }
