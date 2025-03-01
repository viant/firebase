package firestore

import (
	"context"
	"database/sql/driver"
	"fmt"

	"github.com/viant/sqlparser"
)

// Implementation of create collection operation
func (s *Statement) execCreateTable(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	// Parse the CREATE TABLE statement
	createStmt, err := sqlparser.ParseCreateTable(s.SQL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse create table statement: %v", err)
	}

	collectionName := createStmt.Spec.Name

	// Firestore collections are created implicitly when documents are added
	// Here, we can add a placeholder document to create the collection
	collectionRef := s.conn.client.Collection(collectionName)

	_, _, err = collectionRef.Add(ctx, map[string]interface{}{
		"_created": true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create collection %s: %v", collectionName, err)
	}

	return &Result{
		rowsAffected: 1,
	}, nil
}

// Implementation of drop collection operation
func (s *Statement) execDropTable(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	// Parse the DROP TABLE statement
	dropStmt, err := sqlparser.ParseDropTable(s.SQL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse drop table statement: %v", err)
	}

	collectionName := dropStmt.Name

	collectionRef := s.conn.client.Collection(collectionName)

	// Firestore does not have a delete collection operation
	// We need to delete all documents within the collection
	batch := s.conn.client.Batch()

	iter := collectionRef.Documents(ctx)
	defer iter.Stop()

	deletedCount := int64(0)

	for {
		doc, err := iter.Next()
		if err != nil {
			break
		}
		batch.Delete(doc.Ref)
		deletedCount++
	}

	_, err = batch.Commit(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to drop collection %s: %v", collectionName, err)
	}

	return &Result{
		rowsAffected: deletedCount,
	}, nil
}
