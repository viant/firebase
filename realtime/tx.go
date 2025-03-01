package realtime

import (
	"context"
	"database/sql/driver"
)

// tx represents a transaction
type tx struct {
	conn   *connection
	closed bool
	ctx    context.Context
}

// Commit commits the transaction
func (t *tx) Commit() error {
	if t.closed {
		return driver.ErrBadConn
	}
	t.closed = true
	// Firebase Realtime Database does not support traditional transactions;
	// Implement transaction logic if needed using Firebase transactions
	return nil
}

// Rollback rolls back the transaction
func (t *tx) Rollback() error {
	if t.closed {
		return driver.ErrBadConn
	}
	t.closed = true
	// Firebase Realtime Database does not support traditional transactions;
	// Implement rollback logic if needed
	return nil
}
