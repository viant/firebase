package firestore

import (
	"context"
	"database/sql/driver"
	"errors"
	"sync"

	"cloud.google.com/go/firestore"
	"fmt"
	"github.com/viant/sqlparser"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type connection struct {
	cfg    *Config
	ctx    context.Context
	client *firestore.Client
	mu     sync.Mutex
	closed bool
}

// newConnection initializes a new connection to the Firestore
func newConnection(cfg *Config) (*connection, error) {
	ctx := context.Background()

	var opts []option.ClientOption
	if cfg.hasCredentials() {
		opts = append(opts, cfg.options()...)
	}

	client, err := firestore.NewClient(ctx, cfg.ProjectID, opts...)
	if err != nil {
		return nil, fmt.Errorf("error initializing Firestore client: %w", err)
	}

	return &connection{
		cfg:    cfg,
		ctx:    ctx,
		client: client,
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
	err := c.client.Close()
	c.client = nil
	return err
}

// Begin starts a transaction.
func (c *connection) Begin() (driver.Tx, error) {
	return c.BeginTx(c.ctx, driver.TxOptions{})
}

// BeginTx starts a transaction with options.
func (c *connection) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	// Firestore transactions are handled asynchronously via RunTransaction.
	// Return a tx wrapper that will manage this.
	return &tx{
		conn: c,
		ctx:  ctx,
		opts: opts,
	}, nil
}

// Ping verifies a connection to the database is still alive.
func (c *connection) Ping(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return driver.ErrBadConn
	}
	// Perform a simple operation to verify connectivity
	// For example, list collections (though this requires Firestore indexes to be set up)
	iter := c.client.Collections(ctx)
	_, err := iter.Next()
	if err != nil && err != iterator.Done {
		return fmt.Errorf("ping failed: %v", err)
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
	if execerCtx, ok := stmt.(driver.StmtExecContext); ok {
		return execerCtx.ExecContext(ctx, args)
	}
	return nil, errors.New("prepared statement does not implement ExecContext")
}

// QueryContext executes a query that may return rows.
func (c *connection) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	stmt, err := c.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	if queryerCtx, ok := stmt.(driver.StmtQueryContext); ok {
		return queryerCtx.QueryContext(ctx, args)
	}
	return nil, errors.New("prepared statement does not implement QueryContext")
}
