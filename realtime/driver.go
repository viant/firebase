package realtime

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
)

// Register the driver under the name "firebase"
func init() {
	sql.Register("firebase", &Driver{})
}

// Driver is the Firebase driver structure
type Driver struct{}

// Open establishes a new connection to the Firebase database
func (d *Driver) Open(dsn string) (driver.Conn, error) {
	cfg, err := ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse DSN: %v", err)
	}
	conn := &connector{
		cfg: cfg,
	}
	return conn.Connect(context.Background())
}

// connector implements driver.Connector
type connector struct {
	cfg *Config
}

// Connect establishes a connection using the connector's configuration
func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	return newConnection(c.cfg)
}

// Driver returns the underlying Driver of the connector
func (c *connector) Driver() driver.Driver {
	return &Driver{}
}
