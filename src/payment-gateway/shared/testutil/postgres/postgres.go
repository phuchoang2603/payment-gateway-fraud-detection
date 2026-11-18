package postgres

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	pgmodule "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

var (
	postgresOnce sync.Once
	postgresURL  string
	postgresErr  error
)

func AdminURL(t testing.TB) string {
	t.Helper()
	if testing.Short() {
		t.Skip("-short disables test containers")
	}
	postgresOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		container, err := pgmodule.Run(ctx, "postgres:17.6-alpine",
			pgmodule.WithDatabase("postgres"), pgmodule.WithUsername("postgres"), pgmodule.WithPassword("postgres"),
			testcontainers.WithCmd("postgres", "-c", "fsync=off", "-c", "wal_level=logical"),
			testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(2*time.Minute)),
		)
		if err != nil {
			_ = testcontainers.TerminateContainer(container)
			postgresErr = err
			return
		}
		postgresURL, postgresErr = container.ConnectionString(ctx, "sslmode=disable")
	})
	if postgresErr != nil {
		t.Fatalf("start postgres container: %v", postgresErr)
	}
	return postgresURL
}

func SetupPostgres(t testing.TB) string {
	t.Helper()
	adminURL := AdminURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = admin.Close(ctx) }()
	suffix := make([]byte, 8)
	if _, err := rand.Read(suffix); err != nil {
		t.Fatal(err)
	}
	name := "test_" + hex.EncodeToString(suffix)
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		conn, err := pgx.Connect(ctx, adminURL)
		if err != nil {
			t.Error(err)
			return
		}
		defer func() { _ = conn.Close(ctx) }()
		if _, err := conn.Exec(ctx, "DROP DATABASE IF EXISTS "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)"); err != nil {
			t.Error(err)
		}
	})
	parsed, err := url.Parse(adminURL)
	if err != nil {
		t.Fatal(err)
	}
	parsed.Path = "/" + name
	return parsed.String()
}
