//go:build integration

package migrator

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"log"
	"os"
	"testing"

	_ "github.com/go-sql-driver/mysql" // mysql driver
	_ "github.com/jackc/pgx/v4/stdlib" // postgres driver
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

//go:embed testdata/0_bar.sql
var mig0bar string

var migrations = []interface{}{
	&Migration{
		Name: "Using tx, encapsulate two queries",
		Func: func(tx *sql.Tx) error {
			if _, err := tx.Exec("CREATE TABLE foo (id INT PRIMARY KEY)"); err != nil {
				return err
			}
			if _, err := tx.Exec("INSERT INTO foo (id) VALUES (1)"); err != nil {
				return err
			}
			return nil
		},
	},
	&MigrationNoTx{
		Name: "Using db, execute one query",
		Func: func(db *sql.DB) error {
			if _, err := db.Exec("INSERT INTO foo (id) VALUES (2)"); err != nil {
				return err
			}
			return nil
		},
	},
	&Migration{
		Name: "Using tx, one embedded query",
		Func: func(tx *sql.Tx) error {
			if _, err := tx.Exec(mig0bar); err != nil {
				return err
			}
			return nil
		},
	},
}

func migrateTest(driverName, url string) error {
	migrator, err := New(Migrations(migrations...))
	if err != nil {
		return err
	}

	// Migrate up
	db, err := sql.Open(driverName, url)
	if err != nil {
		return err
	}
	if err := migrator.Migrate(context.Background(), db); err != nil {
		return err
	}

	return nil
}

func mustMigrator(migrator *Migrator, err error) *Migrator {
	if err != nil {
		panic(err)
	}
	return migrator
}

func TestPostgres(t *testing.T) {
	if err := migrateTest("pgx", os.Getenv("POSTGRES_URL")); err != nil {
		t.Fatal(err)
	}
}

func TestMySQL(t *testing.T) {
	if err := migrateTest("mysql", os.Getenv("MYSQL_URL")); err != nil {
		t.Fatal(err)
	}
}

func TestMigrationNumber(t *testing.T) {
	db, err := sql.Open("pgx", os.Getenv("POSTGRES_URL"))
	if err != nil {
		t.Fatal(err)
	}
	count, err := countApplied(context.Background(), db, defaultTableName)
	if err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Fatal("db applied migration number should be 3")
	}
}

func TestDatabaseNotFound(t *testing.T) {
	migrator, err := New(Migrations(&Migration{}))
	if err != nil {
		t.Fatal(err)
	}
	db, _ := sql.Open("pgx", "")
	if err := migrator.Migrate(context.Background(), db); err == nil {
		t.Fatal(err)
	}
}

func TestBadMigrations(t *testing.T) {
	db, err := sql.Open("pgx", os.Getenv("POSTGRES_URL"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", defaultTableName))
	if err != nil {
		t.Fatal(err)
	}

	migrators := []struct {
		name  string
		input *Migrator
		want  error
	}{
		{
			name: "bad tx migration",
			input: mustMigrator(New(Migrations(&Migration{
				Name: "bad tx migration",
				Func: func(tx *sql.Tx) error {
					if _, err := tx.Exec("FAIL FAST"); err != nil {
						return err
					}
					return nil
				},
			}))),
		},
		{
			name: "bad db migration",
			input: mustMigrator(New(Migrations(&MigrationNoTx{
				Name: "bad db migration",
				Func: func(db *sql.DB) error {
					if _, err := db.Exec("FAIL FAST"); err != nil {
						return err
					}
					return nil
				},
			}))),
		},
	}

	for _, tt := range migrators {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.input.Migrate(context.Background(), db)
			if err == nil {
				t.Fatal("BAD MIGRATIONS should fail!")
			}
		})
	}
}

func TestBadMigrate(t *testing.T) {
	db, err := sql.Open("mysql", os.Getenv("MYSQL_URL"))
	if err != nil {
		t.Fatal(err)
	}
	if err := migrate(context.Background(), db, log.New(os.Stdout, "migrator: ", 0), "BAD INSERT VERSION", &Migration{Name: "bad insert version", Func: func(tx *sql.Tx) error {
		return nil
	}}); err == nil {
		t.Fatal("BAD INSERT VERSION should fail!")
	}
}

func TestBadMigrateNoTx(t *testing.T) {
	db, err := sql.Open("pgx", os.Getenv("POSTGRES_URL"))
	if err != nil {
		t.Fatal(err)
	}
	if err := migrateNoTx(context.Background(), db, log.New(os.Stdout, "migrator: ", 0), "BAD INSERT VERSION", &MigrationNoTx{Name: "bad migrate no tx", Func: func(db *sql.DB) error {
		return nil
	}}); err == nil {
		t.Fatal("BAD INSERT VERSION should fail!")
	}
}

func TestBadMigrationNumber(t *testing.T) {
	db, err := sql.Open("mysql", os.Getenv("MYSQL_URL"))
	if err != nil {
		t.Fatal(err)
	}
	migrator := mustMigrator(New(Migrations(
		&Migration{
			Name: "bad migration number",
			Func: func(tx *sql.Tx) error {
				if _, err := tx.Exec("CREATE TABLE bar (id INT PRIMARY KEY)"); err != nil {
					return err
				}
				return nil
			},
		},
	)))
	if err := migrator.Migrate(context.Background(), db); err == nil {
		t.Fatalf("BAD MIGRATION NUMBER should fail: %v", err)
	}
}

func TestPending(t *testing.T) {
	db, err := sql.Open("pgx", os.Getenv("POSTGRES_URL"))
	if err != nil {
		t.Fatal(err)
	}
	migrator := mustMigrator(New(Migrations(
		&Migration{
			Name: "Using tx, create baz table",
			Func: func(tx *sql.Tx) error {
				if _, err := tx.Exec("CREATE TABLE baz (id INT PRIMARY KEY)"); err != nil {
					return err
				}
				return nil
			},
		},
	)))
	pending, err := migrator.Pending(db)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 {
		t.Fatalf("pending migrations should be 1, got %d", len(pending))
	}
}

func TestTraces(t *testing.T) {
	// Create a test span recorder.
	sr := tracetest.NewSpanRecorder()
	tp := trace.NewTracerProvider(trace.WithSpanProcessor(sr))

	// Use the test tracer provider in the test.
	otel.SetTracerProvider(tp)
	defer tp.Shutdown(context.Background())

	// Call the function to be tested.
	ctx := context.Background()

	db, err := sql.Open("pgx", os.Getenv("POSTGRES_URL"))
	if err != nil {
		t.Fatal(err)
	}
	migrator := mustMigrator(New(Migrations(
		&Migration{
			Name: "testing trace",
			Func: func(tx *sql.Tx) error {
				if _, err := tx.Exec("CREATE TABLE trace (id INT PRIMARY KEY)"); err != nil {
					return err
				}
				return nil
			},
		},
	)))
	if err := migrator.Migrate(ctx, db); err != nil {
		t.Fatal(err)
	}

	// Retrieve the spans that were recorded.
	spans := sr.Ended()
	if len(spans) != 2 {
		t.Fatalf("Expected 2 spans, got %d", len(spans))
	}

	// Validate the spans.
	parentSpan := spans[1]
	if parentSpan.Name() != "migrate" {
		t.Fatalf("Expected parent span name to be 'migration', got '%s'", parentSpan.Name())
	}

	childSpan := spans[0]
	if childSpan.Name() != "migration" {
		t.Fatalf("Expected child span name to be 'migrate', got '%s'", childSpan.Name())
	}

	// Validate attributes.
	parentSpanAttributes := parentSpan.Attributes()
	if parentSpanAttributes[0].Key != attribute.Key("applied") {
		t.Fatalf("Expected parent span to have attribute 'applied'")
	}

	childSpanAttributes := childSpan.Attributes()
	if childSpanAttributes[0].Key != attribute.Key("type") {
		t.Fatalf("Expected child span to have attribute 'type'")
	}
	if childSpanAttributes[1].Key != attribute.Key("name") {
		t.Fatalf("Expected child span to have attribute 'name'")
	}
	if childSpanAttributes[2].Key != attribute.Key("number") {
		t.Fatalf("Expected child span to have attribute 'number'")
	}

	// Check parent-child relationship.
	if childSpan.Parent().SpanID() != parentSpan.SpanContext().SpanID() {
		t.Error("Child span does not have the correct parent span")
	}
}
