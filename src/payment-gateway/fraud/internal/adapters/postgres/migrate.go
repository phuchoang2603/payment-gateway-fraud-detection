package postgres

import (
	"context"
	"database/sql"
	"io/fs"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/fraud/db"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
)

func Migrate(ctx context.Context, databaseURL string) error {
	connection, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return err
	}
	defer func() { _ = connection.Close() }()
	migrations, err := fs.Sub(db.Migrations, "migrations")
	if err != nil {
		return err
	}
	locker, err := lock.NewPostgresSessionLocker()
	if err != nil {
		return err
	}
	provider, err := goose.NewProvider(goose.DialectPostgres, connection, migrations, goose.WithSessionLocker(locker))
	if err != nil {
		return err
	}
	_, err = provider.Up(ctx)
	return err
}
