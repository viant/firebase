package realtime

// Result implements the driver.Result interface
type Result struct {
	rowsAffected int64
	insertID     string
}

// LastInsertId returns the ID of the last inserted row
func (r *Result) LastInsertId() (int64, error) {
	// Firebase Realtime Database uses string keys; convert if possible
	return 0, nil
}

// RowsAffected returns the number of rows affected by the query
func (r *Result) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}
