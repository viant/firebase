package realtime

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/viant/sqlparser"
)

// Implementation of create table (collection) operation
func (s *Statement) execCreateTable(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	// Parse the CREATE TABLE statement
	createStmt, err := sqlparser.ParseCreateTable(s.SQL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse create table statement: %v", err)
	}

	tableName := createStmt.Spec.Name

	ref := s.conn.client.NewRef(tableName)

	// For Firebase Realtime Database, creating a collection is just ensuring that the key (table) exists.
	// We can set an empty object at that reference.

	err = ref.Set(ctx, map[string]interface{}{}) // Initialize with empty data
	if err != nil {
		return nil, fmt.Errorf("failed to create collection/table %s: %v", tableName, err)
	}

	return &Result{
		rowsAffected: 1,
	}, nil
}

// Implementation of drop table (collection) operation
func (s *Statement) execDropTable(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	// Parse the DROP TABLE statement
	dropStmt, err := sqlparser.ParseDropTable(s.SQL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse drop table statement: %v", err)
	}

	tableName := dropStmt.Name

	ref := s.conn.client.NewRef(tableName)

	// For Firebase Realtime Database, dropping a collection is deleting the reference
	err = ref.Delete(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to drop collection/table %s: %v", tableName, err)
	}

	return &Result{
		rowsAffected: 1,
	}, nil
}

// Implementation of create index operation
func (s *Statement) execCreateIndex(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	// Parse the CREATE INDEX statement
	createIndexStmt, err := sqlparser.ParseCreateIndex(s.SQL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse create index statement: %v", err)
	}

	// Extract information from the createIndexStmt
	tableName := createIndexStmt.Table
	indexedColumns := make([]string, 0, len(createIndexStmt.Columns))
	for _, col := range createIndexStmt.Columns {
		indexedColumns = append(indexedColumns, col.Name)
	}

	// Read existing rules
	rules, err := s.getDatabaseRules(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get existing database rules: %v", err)
	}

	// Modify rules to include the index
	err = addIndexToRules(rules, tableName, indexedColumns)
	if err != nil {
		return nil, fmt.Errorf("failed to add index to rules: %v", err)
	}

	// Update rules
	err = s.updateDatabaseRules(ctx, rules)
	if err != nil {
		return nil, fmt.Errorf("failed to update database rules: %v", err)
	}

	return &Result{
		rowsAffected: 1,
	}, nil
}

// Implementation of drop index operation
func (s *Statement) execDropIndex(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	// Parse the DROP INDEX statement
	dropIndexStmt, err := sqlparser.ParseDropIndex(s.SQL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse drop index statement: %v", err)
	}

	// Extract information from the dropIndexStmt
	indexName := dropIndexStmt.Name
	tableName := dropIndexStmt.Table

	// Read existing rules
	rules, err := s.getDatabaseRules(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get existing database rules: %v", err)
	}

	// Modify rules to remove the index
	err = removeIndexFromRules(rules, tableName, indexName)
	if err != nil {
		return nil, fmt.Errorf("failed to remove index from rules: %v", err)
	}

	// Update rules
	err = s.updateDatabaseRules(ctx, rules)
	if err != nil {
		return nil, fmt.Errorf("failed to update database rules: %v", err)
	}

	return &Result{
		rowsAffected: 1,
	}, nil
}

// Helper function to read the existing database rules
func (s *Statement) getDatabaseRules(ctx context.Context) (map[string]interface{}, error) {
	rulesURL := fmt.Sprintf("%s/.settings/rules.json", strings.TrimRight(s.conn.cfg.DatabaseURL, "/"))

	req, err := http.NewRequestWithContext(ctx, "GET", rulesURL, nil)
	if err != nil {
		return nil, err
	}

	// If authentication is needed
	if s.conn.httpCli == nil {
		s.conn.httpCli = &http.Client{}
	}
	resp, err := s.conn.httpCli.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var rules map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&rules); err != nil {
		return nil, err
	}

	return rules, nil
}

// Helper function to update the database rules
func (s *Statement) updateDatabaseRules(ctx context.Context, rules map[string]interface{}) error {
	rulesURL := fmt.Sprintf("%s/.settings/rules.json", strings.TrimRight(s.conn.cfg.DatabaseURL, "/"))

	rulesData, err := json.Marshal(rules)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "PUT", rulesURL, strings.NewReader(string(rulesData)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	if s.conn.httpCli == nil {
		s.conn.httpCli = &http.Client{}
	}
	resp, err := s.conn.httpCli.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf("failed to update rules: %s", string(bodyBytes))
	}

	return nil
}

// Helper function to add index to rules
func addIndexToRules(rules map[string]interface{}, tableName string, indexedColumns []string) error {
	// Navigate to the proper location in the rules
	// Assuming rules["rules"][tableName][".indexOn"] is the correct path
	rulesSection, ok := rules["rules"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("invalid rules format")
	}

	tableRules, ok := rulesSection[tableName].(map[string]interface{})
	if !ok {
		tableRules = make(map[string]interface{})
		rulesSection[tableName] = tableRules
	}

	indexOn, ok := tableRules[".indexOn"].([]interface{})
	if !ok {
		indexOn = []interface{}{}
	}

	// Add the indexed columns
	for _, col := range indexedColumns {
		if !contains(indexOn, col) {
			indexOn = append(indexOn, col)
		}
	}

	tableRules[".indexOn"] = indexOn

	return nil
}

// Helper function to remove index from rules
func removeIndexFromRules(rules map[string]interface{}, tableName string, indexName string) error {
	// Navigate to the proper location in the rules
	// Assuming rules["rules"][tableName][".indexOn"] is the correct path
	rulesSection, ok := rules["rules"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("invalid rules format")
	}

	tableRules, ok := rulesSection[tableName].(map[string]interface{})
	if !ok {
		// Nothing to remove
		return nil
	}

	indexOn, ok := tableRules[".indexOn"].([]interface{})
	if !ok {
		// No indexes present
		return nil
	}

	// Remove the index
	newIndexOn := []interface{}{}
	for _, col := range indexOn {
		if colStr, ok := col.(string); ok && colStr != indexName {
			newIndexOn = append(newIndexOn, col)
		}
	}

	// Update or remove .indexOn
	if len(newIndexOn) > 0 {
		tableRules[".indexOn"] = newIndexOn
	} else {
		delete(tableRules, ".indexOn")
	}

	return nil
}

// Helper function to check if a slice contains a string
func contains(slice []interface{}, item string) bool {
	for _, elem := range slice {
		if elemStr, ok := elem.(string); ok && elemStr == item {
			return true
		}
	}
	return false
}
