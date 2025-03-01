package firestore

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
)

func init() {
	sql.Register("firestore", &Driver{})
}

// Driver is the Firestore driver structure
type Driver struct{}

// Open establishes a new connection to the Firestore database
func (d *Driver) Open(dsn string) (driver.Conn, error) {
	cfg, err := ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse DSN: %v", err)
	}
	conn, err := newConnection(cfg)
	if err != nil {
		return nil, err
	}
	return conn, nil
}
