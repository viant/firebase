package firestore

type Result struct {
	rowsAffected int64
	insertID     string
}

func (r *Result) LastInsertId() (int64, error) {
	// Firestore uses string IDs; we can return 0 or parse if possible
	return 0, nil
}

func (r *Result) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}
