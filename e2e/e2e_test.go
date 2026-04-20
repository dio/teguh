// Copyright 2026 dio@rockybars.com. Apache-2.0 license.

package e2e_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	testDBPort     = 5455
	testDBUser     = "teguh_test"
	testDBPassword = "teguh_test"
	testDBName     = "teguh_test"
)

var testDSN = fmt.Sprintf(
	"host=localhost port=%d user=%s password=%s dbname=%s sslmode=disable",
	testDBPort, testDBUser, testDBPassword, testDBName,
)

var testPool *pgxpool.Pool

func TestMain(m *testing.M) {
	pg := embeddedpostgres.NewDatabase(
		embeddedpostgres.DefaultConfig().
			Username(testDBUser).
			Password(testDBPassword).
			Database(testDBName).
			Port(testDBPort),
	)
	if err := pg.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "embedded-postgres start: %v\n", err)
		os.Exit(1)
	}

	ctx := context.Background()

	var err error
	testPool, err = pgxpool.New(ctx, testDSN)
	if err != nil {
		_ = pg.Stop()
		fmt.Fprintf(os.Stderr, "pgxpool.New: %v\n", err)
		os.Exit(1)
	}

	// Install teguh schema. Run 'make fetch-schema' to copy sql/teguh.sql first.
	schema, err := os.ReadFile("testdata/teguh.sql")
	if errors.Is(err, os.ErrNotExist) {
		fmt.Fprintln(os.Stderr, "testdata/teguh.sql not found, run 'make fetch-schema' then re-run tests")
		testPool.Close()
		_ = pg.Stop()
		os.Exit(0) // skip rather than fail
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "read teguh.sql: %v\n", err)
		testPool.Close()
		_ = pg.Stop()
		os.Exit(1)
	}

	conn, err := testPool.Acquire(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "acquire conn: %v\n", err)
		testPool.Close()
		_ = pg.Stop()
		os.Exit(1)
	}
	if _, err := conn.Exec(ctx, string(schema)); err != nil {
		fmt.Fprintf(os.Stderr, "install teguh schema: %v\n", err)
		conn.Release()
		testPool.Close()
		_ = pg.Stop()
		os.Exit(1)
	}
	conn.Release()

	code := m.Run()

	testPool.Close()
	_ = pg.Stop()
	os.Exit(code)
}
