package application_test

import (
	"strings"
	"testing"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/shared/testutil/telemetry"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/payments/internal/testsupport"
)

func TestFlushedRedisFallsBackToPostgresWithoutAnotherProcessorEffect(t *testing.T) {
	stack := newStack(t, true)
	payment := mustCreate(t, stack, "key-1", "order-1")
	first := mustStart(t, stack, payment, "attempt-1", "vault-token")
	events := stack.DB.CountEvents(t)

	stack.FlushRedis(t)
	if again := mustCreate(t, stack, "key-1", "order-1"); again.GetPaymentId() != payment.GetPaymentId() || again.GetCheckoutToken() != payment.GetCheckoutToken() {
		t.Fatalf("create after flush = %v", again)
	}
	if again := mustStart(t, stack, payment, "attempt-1", "vault-token"); again.String() != first.String() {
		t.Fatalf("attempt after flush = %v, want %v", again, first)
	}
	if stack.Processor.TotalSaleCalls() != 1 || stack.DB.CountEvents(t) != events {
		t.Fatalf("flush caused a new effect: sales = %d, events %d -> %d", stack.Processor.TotalSaleCalls(), events, stack.DB.CountEvents(t))
	}
	if len(stack.RedisEntries(t)) == 0 {
		t.Fatal("the database result was not written back to redis")
	}
}

func TestRedisOutageFallsBackToPostgresAndNeverFailsOpen(t *testing.T) {
	stack := testsupport.NewStack(t, testsupport.StackOptions{Config: shortConfig(), WithRedis: true, RedisAddr: "127.0.0.1:1"})
	payment := mustCreate(t, stack, "key-1", "order-1")
	first := mustStart(t, stack, payment, "attempt-1", "vault-token")
	if first.GetStatus().String() != "ATTEMPT_STATUS_SUCCEEDED" {
		t.Fatalf("result = %v", first)
	}
	if again := mustStart(t, stack, payment, "attempt-1", "vault-token"); again.String() != first.String() {
		t.Fatalf("retry during outage = %v, want %v", again, first)
	}
	if _, err := start(stack, payment, "attempt-1", "other-token"); err == nil || !strings.Contains(err.Error(), "different request") {
		t.Fatalf("conflict during outage err = %v", err)
	}
	if stack.Processor.TotalSaleCalls() != 1 {
		t.Fatalf("sale calls = %d, want 1", stack.Processor.TotalSaleCalls())
	}
}

func TestRedisRecordsNeverContainTokensCredentialsOrRequestBodies(t *testing.T) {
	stack := newStack(t, true)
	payment := mustCreate(t, stack, "key-secret", "order-secret-reference")
	mustStart(t, stack, payment, "attempt-1", "vault-token-SECRET")
	stack.Processor.SetBehavior(testsupport.SaleTimeout)
	pending := mustCreate(t, stack, "key-2", "order-2")
	mustStart(t, stack, pending, "attempt-1", "vault-token-SECRET-2")

	entries := stack.RedisEntries(t)
	if len(entries) < 4 {
		t.Fatalf("expected records for two creations and two attempts, got %d", len(entries))
	}
	forbidden := []string{
		"vault-token-SECRET", payment.GetCheckoutToken(), pending.GetCheckoutToken(), "order-secret-reference", "seller-01", "buyer-01",
		"94105", "shop.example", "fingerprint-1", "sku-phone-001",
	}
	for key, value := range entries {
		for _, secret := range forbidden {
			if strings.Contains(key, secret) {
				t.Errorf("key %q contains %q", key, secret)
			}
			if strings.Contains(value, secret) {
				t.Errorf("record %q contains %q: %s", key, secret, value)
			}
		}
	}
}

func TestRedisCountersTrackHitsMissesAndErrors(t *testing.T) {
	reader := telemetry.Metrics(t)
	stack := newStack(t, true)
	payment := mustCreate(t, stack, "key-1", "order-1")
	mustStart(t, stack, payment, "attempt-1", "vault-token")
	mustStart(t, stack, payment, "attempt-1", "vault-token")

	unreachable := testsupport.NewStack(t, testsupport.StackOptions{Config: shortConfig(), WithRedis: true, RedisAddr: "127.0.0.1:1"})
	mustCreate(t, unreachable, "key-1", "order-1")

	values := telemetry.Sums(t, reader)
	if values["payments.redis.idempotency.hits"] < 1 || values["payments.redis.idempotency.misses"] < 2 || values["payments.redis.idempotency.errors"] < 1 {
		t.Fatalf("Redis counters = %v", values)
	}
}

func TestRestartedRedisFallsBackToPostgresWithoutAnotherProcessorEffect(t *testing.T) {
	stack := newStack(t, true)
	payment := mustCreate(t, stack, "key-1", "order-1")
	first := mustStart(t, stack, payment, "attempt-1", "vault-token")
	events := stack.DB.CountEvents(t)

	address := stack.Redis.Restart(t)
	restarted := stack.Restart(t, testsupport.StackOptions{Config: shortConfig(), WithRedis: true, RedisAddr: address})
	if len(restarted.RedisEntries(t)) != 0 {
		t.Fatal("Redis retained entries despite persistence being disabled")
	}
	if again := mustCreate(t, restarted, "key-1", "order-1"); again.GetPaymentId() != payment.GetPaymentId() || again.GetCheckoutToken() != payment.GetCheckoutToken() {
		t.Fatalf("create after Redis restart = %v", again)
	}
	if again := mustStart(t, restarted, payment, "attempt-1", "vault-token"); again.String() != first.String() {
		t.Fatalf("attempt after Redis restart = %v, want %v", again, first)
	}
	if stack.Processor.TotalSaleCalls() != 1 || restarted.Processor.TotalSaleCalls() != 0 || stack.DB.CountEvents(t) != events {
		t.Fatal("Redis restart caused another processor effect or ledger event")
	}
}
