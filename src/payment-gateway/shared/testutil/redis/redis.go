package redis

import (
	"context"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type Redis struct {
	Container testcontainers.Container
	Address   string
}

func SetupRedis(t testing.TB) *Redis {
	t.Helper()
	if testing.Short() {
		t.Skip("-short disables test containers")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	container, err := testcontainers.Run(ctx, "quay.io/opstree/redis:v7.2.16",
		testcontainers.WithExposedPorts("6379/tcp"),
		testcontainers.WithEntrypoint("redis-server"),
		testcontainers.WithCmd("--save", "", "--appendonly", "no"),
		testcontainers.WithWaitStrategy(wait.ForLog("Ready to accept connections"), wait.ForListeningPort("6379/tcp")),
	)
	testcontainers.CleanupContainer(t, container)
	if err != nil {
		t.Fatalf("start redis container: %v", err)
	}
	address, err := container.PortEndpoint(ctx, "6379/tcp", "")
	if err != nil {
		t.Fatal(err)
	}
	return &Redis{Container: container, Address: address}
}

func (r *Redis) Restart(t testing.TB) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := r.Container.Stop(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if err := r.Container.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := wait.ForListeningPort("6379/tcp").WaitUntilReady(ctx, r.Container); err != nil {
		t.Fatal(err)
	}
	address, err := r.Container.PortEndpoint(ctx, "6379/tcp", "")
	if err != nil {
		t.Fatal(err)
	}
	r.Address = address
	return address
}
