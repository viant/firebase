package realtime

import (
	"context"
	"database/sql/driver"
	"errors"
	fb "firebase.google.com/go"
	"firebase.google.com/go/db"
	"fmt"
	"github.com/viant/sqlparser"
	"google.golang.org/api/option"
	"net/http"
	"sync"
)

type connection struct {
	cfg     *Config
	ctx     context.Context
	client  *db.Client
	mu      sync.Mutex
	closed  bool
	httpCli *http.Client
}

// newConnection initializes a new connection to the Firebase Realtime Database
func newConnection(cfg *Config) (*connection, error) {
	ctx := context.Background()

	var opts []option.ClientOption
	if cfg.hasCredentials() {
		opts = append(opts, cfg.options()...)
	} else {
		opts = append(opts, option.WithoutAuthentication())
	}

	conf := &fb.Config{
		DatabaseURL: cfg.DatabaseURL,
	}

	app, err := fb.NewApp(ctx, conf, opts...)
	if err != nil {
		return nil, fmt.Errorf("error initializing firebase app: %v", err)
	}

	dbClient, err := app.Database(ctx)
	if err != nil {
		return nil, fmt.Errorf("error initializing database client: %v", err)
	}

	return &connection{
		cfg:    cfg,
		ctx:    ctx,
		client: dbClient,
	}, nil
}

// Prepare returns a prepared statement, bound to this connection.
func (c *connection) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(c.ctx, query)
}

// PrepareContext prepares a statement with context.
func (c *connection) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	stmtKind := sqlparser.ParseKind(query)
	stmt := &Statement{
		SQL:  query,
		kind: stmtKind,
		conn: c,
		ctx:  ctx,
	}
	stmt.checkQueryParameters()
	return stmt, nil
}

// Close closes the connection.
func (c *connection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return driver.ErrBadConn
	}
	c.closed = true
	c.client = nil
	return nil
}

// Begin starts a transaction.
func (c *connection) Begin() (driver.Tx, error) {
	return c.BeginTx(c.ctx, driver.TxOptions{})
}

// BeginTx starts a transaction with options.
func (c *connection) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return &tx{conn: c}, nil
}

// Ping verifies a connection to the database is still alive.
func (c *connection) Ping(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return driver.ErrBadConn
	}
	// Implement a simple read to verify connectivity
	ref := c.client.NewRef(".info/connected")
	var connected bool
	if err := ref.Get(ctx, &connected); err != nil {
		return fmt.Errorf("ping failed: %v", err)
	}
	if !connected {
		return fmt.Errorf("not connected to Firebase")
	}
	return nil
}

// CheckNamedValue checks if the named value is supported.
func (c *connection) CheckNamedValue(nv *driver.NamedValue) error {
	// All types are accepted
	return nil
}

// ExecContext executes a query that doesn't return rows.
func (c *connection) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	stmt, err := c.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	if execer, ok := stmt.(driver.StmtExecContext); ok {
		return execer.ExecContext(ctx, args)
	} else {
		return nil, errors.New("prepared statement does not implement ExecContext")
	}
}

// QueryContext executes a query that may return rows.
func (c *connection) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	stmt, err := c.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	if queryer, ok := stmt.(driver.StmtQueryContext); ok {
		return queryer.QueryContext(ctx, args)
	} else {
		return nil, errors.New("prepared statement does not implement QueryContext")
	}
}
