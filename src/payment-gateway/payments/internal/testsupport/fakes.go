package testsupport

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	fraudv1 "github.com/phuchoang2603/payment-gateway-fraud-detection/shared/proto/fraud/v1"
	paymentsv1 "github.com/phuchoang2603/payment-gateway-fraud-detection/shared/proto/payments/v1"
	processorv1 "github.com/phuchoang2603/payment-gateway-fraud-detection/shared/proto/processor/v1"
)

func listen(t testing.TB, register func(*grpc.Server)) (*grpc.ClientConn, string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	register(server)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn, listener.Addr().String()
}

type FakeFraud struct {
	fraudv1.UnimplementedFraudServiceServer
	Addr     string
	mu       sync.Mutex
	decide   func(ctx context.Context, request *fraudv1.DecideRequest) (*fraudv1.DecideResponse, error)
	requests []*fraudv1.DecideRequest
}

func Approve() *paymentsv1.RiskAssessment {
	return &paymentsv1.RiskAssessment{Outcome: paymentsv1.RiskOutcome_RISK_OUTCOME_APPROVE, ReasonCodes: []string{"ALLOW"}, PolicyVersion: "rules-v1", AssessmentId: "test-assessment", EvidenceSha256: strings.Repeat("a", 64)}
}

func Decline(reason string) *paymentsv1.RiskAssessment {
	return &paymentsv1.RiskAssessment{Outcome: paymentsv1.RiskOutcome_RISK_OUTCOME_DECLINE, ReasonCodes: []string{reason}, PolicyVersion: "rules-v1", AssessmentId: "test-assessment", EvidenceSha256: strings.Repeat("a", 64)}
}

func NewFakeFraud(t testing.TB) (*FakeFraud, *grpc.ClientConn) {
	t.Helper()
	fake := &FakeFraud{}
	fake.SetAssessment(Approve())
	conn, addr := listen(t, func(server *grpc.Server) { fraudv1.RegisterFraudServiceServer(server, fake) })
	fake.Addr = addr
	return fake, conn
}

func (f *FakeFraud) SetAssessment(assessment *paymentsv1.RiskAssessment) {
	f.SetHandler(func(context.Context, *fraudv1.DecideRequest) (*fraudv1.DecideResponse, error) {
		return &fraudv1.DecideResponse{Assessment: assessment, EvaluatedAt: timestamppb.Now()}, nil
	})
}

func (f *FakeFraud) SetHandler(handler func(ctx context.Context, request *fraudv1.DecideRequest) (*fraudv1.DecideResponse, error)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.decide = handler
}

func (f *FakeFraud) Requests() []*fraudv1.DecideRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*fraudv1.DecideRequest(nil), f.requests...)
}

func (f *FakeFraud) Decide(ctx context.Context, request *fraudv1.DecideRequest) (*fraudv1.DecideResponse, error) {
	f.mu.Lock()
	handler := f.decide
	f.requests = append(f.requests, request)
	f.mu.Unlock()
	return handler(ctx, request)
}

type SaleBehavior int

const (
	SaleApprove SaleBehavior = iota
	SaleDecline
	SaleTimeout
	SaleUnavailable
	SaleLoseResponse
)

type FakeProcessor struct {
	processorv1.UnimplementedProcessorServiceServer
	Addr       string
	mu         sync.Mutex
	behavior   SaleBehavior
	reason     string
	delay      time.Duration
	operations map[string]*processorv1.Operation
	saleCalls  map[string]int
	tokens     map[string]string
}

func NewFakeProcessor(t testing.TB) (*FakeProcessor, *grpc.ClientConn) {
	t.Helper()
	fake := &FakeProcessor{
		reason: "insufficient_funds", operations: map[string]*processorv1.Operation{},
		saleCalls: map[string]int{}, tokens: map[string]string{},
	}
	conn, addr := listen(t, func(server *grpc.Server) { processorv1.RegisterProcessorServiceServer(server, fake) })
	fake.Addr = addr
	return fake, conn
}

func (p *FakeProcessor) SetBehavior(behavior SaleBehavior) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.behavior = behavior
}

func (p *FakeProcessor) SetDelay(delay time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.delay = delay
}

func (p *FakeProcessor) SaleCalls(operationKey string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.saleCalls[operationKey]
}

func (p *FakeProcessor) TotalSaleCalls() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	total := 0
	for _, calls := range p.saleCalls {
		total += calls
	}
	return total
}

func (p *FakeProcessor) Operation(operationKey string) *processorv1.Operation {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.operations[operationKey]
}

func (p *FakeProcessor) TokenFor(operationKey string) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.tokens[operationKey]
}

func (p *FakeProcessor) Sale(ctx context.Context, request *processorv1.SaleRequest) (*processorv1.Operation, error) {
	p.mu.Lock()
	p.saleCalls[request.GetOperationKey()]++
	p.tokens[request.GetOperationKey()] = request.GetPaymentMethodToken()
	behavior, delay, reason := p.behavior, p.delay, p.reason
	if existing, ok := p.operations[request.GetOperationKey()]; ok {
		p.mu.Unlock()
		return existing, nil
	}
	p.mu.Unlock()

	if behavior == SaleUnavailable {
		return nil, status.Error(codes.Unavailable, "vault unavailable")
	}
	operation := &processorv1.Operation{
		OperationKey: request.GetOperationKey(), Outcome: processorv1.OperationOutcome_OPERATION_OUTCOME_APPROVED,
		ProcessorReference: "sim-" + request.GetOperationKey(), RecordedAt: timestamppb.Now(),
	}
	if behavior == SaleDecline {
		operation.Outcome = processorv1.OperationOutcome_OPERATION_OUTCOME_DECLINED
		operation.DeclineReason = reason
		operation.ProcessorReference = ""
	}
	p.mu.Lock()
	p.operations[request.GetOperationKey()] = operation
	p.mu.Unlock()
	if behavior == SaleTimeout || behavior == SaleLoseResponse {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(30 * time.Second):
		}
	}
	if delay > 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
	}
	return operation, nil
}

func (p *FakeProcessor) GetOperation(_ context.Context, request *processorv1.GetOperationRequest) (*processorv1.Operation, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if operation, ok := p.operations[request.GetOperationKey()]; ok {
		return operation, nil
	}
	return nil, status.Error(codes.NotFound, "unknown operation")
}
