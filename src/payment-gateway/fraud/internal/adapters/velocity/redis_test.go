package velocity

import (
	"context"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/fraud/internal/application"
	testredis "github.com/phuchoang2603/payment-gateway-fraud-detection/shared/testutil/redis"
	"github.com/redis/go-redis/v9"
)

func setup(t *testing.T) (*Guard, *redis.Client) {
	t.Helper()
	server := testredis.SetupRedis(t)
	ctx := context.Background()
	admin := redis.NewClient(&redis.Options{Addr: server.Address})
	t.Cleanup(func() { _ = admin.Close() })
	if err := admin.ConfigSet(ctx, "requirepass", "guard-test").Err(); err != nil {
		t.Fatal(err)
	}
	client := redis.NewClient(&redis.Options{Addr: server.Address, Password: "guard-test"})
	t.Cleanup(func() { _ = client.Close() })
	guard, err := New(Config{Address: server.Address, Password: "guard-test", Epoch: "test", PolicyVersion: "observe-v1", RetryHorizon: 5 * time.Minute, Timeout: time.Second, Threshold10s: 2, Threshold60s: 10})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = guard.Close() })
	return guard, client
}

func input(attempt string) application.Request {
	return application.Request{Identity: application.Identity{IntegrationID: "integration", PaymentID: "payment", AttemptID: attempt}, CustomerID: "customer", MethodFingerprint: "method", Digest: hash(attempt)}
}

func TestConcurrentSubmissionsAndLostResponseRetry(t *testing.T) {
	guard, _ := setup(t)
	ctx := context.Background()
	first := time.Now()
	const submissions = 16
	counts := make(chan int64, submissions)
	var workers sync.WaitGroup
	for i := range submissions {
		workers.Go(func() {
			request := input(strconv.Itoa(i))
			customer, method := guard.Register(ctx, request, first)
			if customer.Status != "coverage_unknown" || method.Status != "coverage_unknown" || customer.Count10s == nil || method.Count10s == nil {
				t.Errorf("initial observations: %+v %+v", customer, method)
				return
			}
			counts <- *customer.Count10s
		})
	}
	workers.Wait()
	close(counts)
	ordered := make([]int, 0, submissions)
	for count := range counts {
		ordered = append(ordered, int(count))
	}
	sort.Ints(ordered)
	if len(ordered) != submissions {
		t.Fatalf("successful registrations=%d", len(ordered))
	}
	for i, count := range ordered {
		if count != i+1 {
			t.Fatalf("non-atomic counts: %v", ordered)
		}
	}
	original, _ := guard.Register(ctx, input("0"), first)
	retry, _ := guard.Register(ctx, input("0"), first)
	if retry.Count10s == nil || *retry.Count10s != submissions || !retry.RegisteredAt.Equal(*original.RegisteredAt) || !retry.ThresholdExceeded {
		t.Fatalf("retry refreshed registration: %+v", retry)
	}
	changed := input("0")
	changed.Digest = hash("conflicting content")
	conflict, _ := guard.Register(ctx, changed, first)
	if conflict.Status != "conflict" || conflict.Count10s != nil {
		t.Fatalf("content conflict: %+v", conflict)
	}
	expired, _ := guard.Register(ctx, input("old"), first.Add(-10*time.Minute))
	if expired.Status != "expired_retry" {
		t.Fatalf("unsafe retry: %+v", expired)
	}
}

func TestPartialRegistrationAndRecoveryCoverage(t *testing.T) {
	guard, client := setup(t)
	ctx := context.Background()
	request := input("attempt")
	first := time.Now()
	prefix := guard.prefix(request.IntegrationID, "customer", request.CustomerID)
	if err := client.Set(ctx, prefix+"window", "invalid-type", 0).Err(); err != nil {
		t.Fatal(err)
	}
	customer, method := guard.Register(ctx, request, first)
	if customer.Status != "error" || method.Count60s == nil || *method.Count60s != 1 {
		t.Fatalf("partial result: %+v %+v", customer, method)
	}
	if err := client.Del(ctx, prefix+"window").Err(); err != nil {
		t.Fatal(err)
	}
	customer, replay := guard.Register(ctx, request, first)
	if customer.Count60s == nil || *customer.Count60s != 1 || replay.Count60s == nil || *replay.Count60s != 1 || !method.RegisteredAt.Equal(*replay.RegisteredAt) {
		t.Fatalf("partial retry: %+v %+v", customer, replay)
	}
	clock, err := client.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	if err = client.HSet(ctx, prefix+"coverage", "since", clock.Add(-time.Minute).UnixMilli()).Err(); err != nil {
		t.Fatal(err)
	}
	customer, _ = guard.Register(ctx, request, first)
	if customer.Status != "registered" {
		t.Fatalf("warm coverage: %+v", customer)
	}
	if err = client.HSet(ctx, prefix+"coverage", "epoch", "previous-process").Err(); err != nil {
		t.Fatal(err)
	}
	customer, _ = guard.Register(ctx, request, first)
	if customer.Status != "coverage_unknown" {
		t.Fatalf("recovered process claimed complete coverage: %+v", customer)
	}
}
