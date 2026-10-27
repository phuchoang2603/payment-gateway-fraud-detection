package testsupport

import (
	"context"
	"crypto/rand"
	"fmt"
	"net/url"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/fraud/internal/adapters/postgres"
	testpostgres "github.com/phuchoang2603/payment-gateway-fraud-detection/shared/testutil/postgres"
)

var runtimePassword = rand.Text()

func Database(t testing.TB) (*postgres.Store, *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	databaseURL := testpostgres.SetupPostgres(t)
	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	_, err = admin.Exec(ctx, fmt.Sprintf(`DO $$ BEGIN
 IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname='fraud_app') THEN
 CREATE ROLE fraud_app LOGIN PASSWORD '%s';
 END IF;
 END $$;`, runtimePassword))
	if err != nil {
		t.Fatal(err)
	}
	if err = postgres.Migrate(ctx, databaseURL); err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	parsed.User = url.UserPassword("fraud_app", runtimePassword)
	runtime, err := pgxpool.New(ctx, parsed.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runtime.Close)
	return postgres.New(runtime), admin
}
