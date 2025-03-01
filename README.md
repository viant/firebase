# Firebase Go SQL Driver

[![GoDoc](https://godoc.org/github.com/viant/firebase?status.svg)](https://godoc.org/github.com/viant/firebase)

Firebase Go SQL Driver provides a `database/sql` driver implementation for both Firebase Realtime Database and Cloud Firestore, allowing Go developers to interact with Firebase databases using the standard `database/sql` package.

## Features

- **Support for Firebase Realtime Database and Cloud Firestore**: Choose the database that fits your application's needs.
- **Standard SQL Interface**: Utilize familiar SQL syntax for querying and manipulating data.
- **DML Operations**: Execute `SELECT`, `INSERT`, `UPDATE`, and `DELETE` statements.
- **DDL Operations**: Execute `CREATE TABLE` and `DROP TABLE` statements where applicable.
- **Prepared Statements**: Use parameterized queries to prevent SQL injection.

## Installation

```bash
go get -u github.com/viant/firebase
```

## Usage

### Importing the Driver

#### For Firebase Realtime Database:

```go
import (
    "database/sql"
    _ "github.com/viant/firebase/realtime"
)
```

#### For Cloud Firestore:

```go
import (
    "database/sql"
    _ "github.com/viant/firebase/firestore"
)
```

### Connecting to the Database

#### Firebase Realtime Database

Provide the DSN in the format:

```
firebase://<databaseName>/[?key1=value1&key2=value2...]
```

Example:

```go
dsn := "firebase://my-firebase-project/?database_url=https://my-firebase-project.firebaseio.com"
db, err := sql.Open("firebase", dsn)
if err != nil {
    // handle error
}
defer db.Close()
```

#### Cloud Firestore

Provide the DSN in the format:

```
firestore://<project-id>/?[key1=value1&key2=value2...]
```

Example:

```go
dsn := "firestore://my-firestore-project"
db, err := sql.Open("firestore", dsn)
if err != nil {
    // handle error
}
defer db.Close()
```

### Executing Queries

#### Inserting Data

```go
stmt, err := db.Prepare("INSERT INTO users (name, email) VALUES (?, ?)")
if err != nil {
    // handle error
}
res, err := stmt.Exec("Jane Doe", "jane@example.com")
if err != nil {
    // handle error
}
affectedRows, _ := res.RowsAffected()
fmt.Printf("Inserted %d row(s)\n", affectedRows)
```

#### Selecting Data

```go
rows, err := db.Query("SELECT name, email FROM users WHERE name = ?", "Jane Doe")
if err != nil {
    // handle error
}
defer rows.Close()

for rows.Next() {
    var name, email string
    if err := rows.Scan(&name, &email); err != nil {
        // handle error
    }
    fmt.Printf("Name: %s, Email: %s\n", name, email)
}
```

#### Updating Data

```go
stmt, err := db.Prepare("UPDATE users SET email = ? WHERE name = ?")
if err != nil {
    // handle error
}
res, err := stmt.Exec("newemail@example.com", "Jane Doe")
if err != nil {
    // handle error
}
affectedRows, _ := res.RowsAffected()
fmt.Printf("Updated %d row(s)\n", affectedRows)
```

#### Deleting Data

```go
stmt, err := db.Prepare("DELETE FROM users WHERE name = ?")
if err != nil {
    // handle error
}
res, err := stmt.Exec("Jane Doe")
if err != nil {
    // handle error
}
affectedRows, _ := res.RowsAffected()
fmt.Printf("Deleted %d row(s)\n", affectedRows)
```

### Transactions

Transactions are supported where applicable.

```go
tx, err := db.Begin()
if err != nil {
    // handle error
}

// Perform transactional operations
_, err = tx.Exec("INSERT INTO users (name, email) VALUES (?, ?)", "John Smith", "john@example.com")
if err != nil {
    tx.Rollback()
    // handle error
}

err = tx.Commit()
if err != nil {
    // handle error
}
```

## Configuration

### Authentication

Firebase SDK requires authentication to interact with your Firebase project. Ensure that you have:

- A service account key JSON file for your Firebase project.
- Set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to point to your service account key file.

Alternatively, you can provide authentication credentials using the DSN parameters.

### DSN Parameters

- `credJSON`: Base64 encoded JSON string of your service account credentials.
- `credURL`: Path to your service account credentials file.

Example with credentials in DSN:

```go
dsn := "firebase://my-firebase-database/?credURL=/path/to/credentials.json"
```

## Limitations

- **Firebase Realtime Database** does not support SQL joins or complex queries. The driver translates SQL queries to Firebase queries where possible.
- **Cloud Firestore** has different querying capabilities and limitations. Ensure your queries are supported by Firestore.

## License

This library is distributed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).

## Credits

Developed and maintained by [Viant](https://github.com/viant).
