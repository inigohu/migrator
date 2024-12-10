package migrator

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

const defaultTableName = "migrations"

// Migrator is the migrator implementation
type Migrator struct {
	tableName  string
	logger     Logger
	migrations []interface{}
}

// Option sets options such migrations or table name.
type Option func(*Migrator)

// TableName creates an option to allow overriding the default table name
func TableName(tableName string) Option {
	return func(m *Migrator) {
		m.tableName = tableName
	}
}

// Logger interface
type Logger interface {
	Printf(string, ...interface{})
}

// LoggerFunc is a bridge between Logger and any third party logger
type LoggerFunc func(string, ...interface{})

// Printf implements Logger interface
func (f LoggerFunc) Printf(msg string, args ...interface{}) {
	f(msg, args...)
}

// WithLogger creates an option to allow overriding the stdout logging
func WithLogger(logger Logger) Option {
	return func(m *Migrator) {
		m.logger = logger
	}
}

// Migrations creates an option with provided migrations
func Migrations(migrations ...interface{}) Option {
	return func(m *Migrator) {
		m.migrations = migrations
	}
}

// New creates a new migrator instance
func New(opts ...Option) (*Migrator, error) {
	m := &Migrator{
		logger:    log.New(os.Stdout, "migrator: ", 0),
		tableName: defaultTableName,
	}
	for _, opt := range opts {
		opt(m)
	}

	if len(m.migrations) == 0 {
		return nil, errors.New("migrator: migrations must be provided")
	}

	for _, m := range m.migrations {
		switch m.(type) {
		case *Migration:
		case *MigrationNoTx:
		default:
			return nil, errors.New("migrator: invalid migration type")
		}
	}

	return m, nil
}

// Migrate applies all available migrations
func (m *Migrator) Migrate(ctx context.Context, db *sql.DB) error {
	tracer := otel.Tracer("")

	// create migrations table if doesn't exist
	ctx, rootSpan := tracer.Start(ctx, "migrate")
	defer rootSpan.End()
	_, err := db.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT8 NOT NULL,
			version VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		);
	`, m.tableName))
	if err != nil {
		return err
	}

	// count applied migrations
	count, err := countApplied(ctx, db, m.tableName)
	if err != nil {
		return err
	}
	rootSpan.SetAttributes(attribute.Int("applied", count))

	if count > len(m.migrations) {
		err := errors.New("migrator: applied migration number on db cannot be greater than the defined migration list")
		rootSpan.SetStatus(codes.Error, err.Error())
		return err
	}

	// plan migrations
	for idx, migration := range m.migrations[count:len(m.migrations)] {
		insertVersion := fmt.Sprintf("INSERT INTO %s (id, version) VALUES (%d, '%s')", m.tableName, idx+count, migration.(fmt.Stringer).String())
		ctx, span := tracer.Start(ctx, "migration")
		defer span.End()
		switch mig := migration.(type) {
		case *Migration:
			span.SetAttributes(attribute.String("type", "tx"))
			span.SetAttributes(attribute.String("name", mig.Name))
			if err := migrate(ctx, db, m.logger, insertVersion, mig); err != nil {
				span.SetStatus(codes.Error, err.Error())
				return fmt.Errorf("migrator: error while running migrations: %v", err)
			}
		case *MigrationNoTx:
			span.SetAttributes(attribute.String("type", "no-tx"))
			span.SetAttributes(attribute.String("name", mig.Name))
			if err := migrateNoTx(ctx, db, m.logger, insertVersion, mig); err != nil {
				span.SetStatus(codes.Error, err.Error())
				return fmt.Errorf("migrator: error while running migrations: %v", err)
			}
		}
		span.SetAttributes(attribute.Int("number", idx))
		span.SetStatus(codes.Ok, "")
	}

	rootSpan.SetStatus(codes.Ok, "migrations applied successfully")

	return nil
}

// Pending returns all pending (not yet applied) migrations
func (m *Migrator) Pending(db *sql.DB) ([]interface{}, error) {
	count, err := countApplied(context.Background(), db, m.tableName)
	if err != nil {
		return nil, err
	}
	return m.migrations[count:len(m.migrations)], nil
}

func countApplied(ctx context.Context, db *sql.DB, tableName string) (int, error) {
	// count applied migrations
	var count int
	rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT count(*) FROM %s", tableName))
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = rows.Close()
	}()
	for rows.Next() {
		if err := rows.Scan(&count); err != nil {
			return 0, err
		}
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	return count, nil
}

// Migration represents a single migration
type Migration struct {
	Name string
	Func func(*sql.Tx) error
}

// String returns a string representation of the migration
func (m *Migration) String() string {
	return m.Name
}

// MigrationNoTx represents a single not transactional migration
type MigrationNoTx struct {
	Name string
	Func func(*sql.DB) error
}

func (m *MigrationNoTx) String() string {
	return m.Name
}

func migrate(ctx context.Context, db *sql.DB, logger Logger, insertVersion string, migration *Migration) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			if errRb := tx.Rollback(); errRb != nil {
				err = fmt.Errorf("error rolling back: %s\n%s", errRb, err)
			}
			return
		}
		err = tx.Commit()
	}()
	logger.Printf("applying migration named '%s'...", migration.Name)
	if err = migration.Func(tx); err != nil {
		return fmt.Errorf("error executing golang migration: %s", err)
	}
	if _, err = tx.ExecContext(ctx, insertVersion); err != nil {
		return fmt.Errorf("error updating migration versions: %s", err)
	}
	logger.Printf("applied migration named '%s'", migration.Name)

	return err
}

func migrateNoTx(ctx context.Context, db *sql.DB, logger Logger, insertVersion string, migration *MigrationNoTx) error {
	logger.Printf("applying no tx migration named '%s'...", migration.Name)
	if err := migration.Func(db); err != nil {
		return fmt.Errorf("error executing golang migration: %s", err)
	}
	if _, err := db.ExecContext(ctx, insertVersion); err != nil {
		return fmt.Errorf("error updating migration versions: %s", err)
	}
	logger.Printf("applied no tx migration named '%s'", migration.Name)

	return nil
}
