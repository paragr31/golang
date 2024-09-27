package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/thda/tds" // Import the TDS driver
)

func main() {
	// Database connection string
	connString := "your_user:my%40password@tcp(your_server:port)/your_database"

	// Open a connection to the database
	db, err := sql.Open("tds", connString)
	if err != nil {
		log.Fatal("Error opening connection: ", err)
	}
	defer db.Close()

	// Query the data from the table
	dataQuery := "SELECT * FROM your_table"
	dataRows, err := db.Query(dataQuery)
	if err != nil {
		log.Fatal("Error querying data: ", err)
	}
	defer dataRows.Close()

	// Get column names for data reading
	columns, err := dataRows.Columns()
	if err != nil {
		log.Fatal("Error getting columns: ", err)
	}

	// Create a slice to hold the values
	values := make([]interface{}, len(columns))
	for i := range values {
		var v interface{}
		values[i] = &v // Store pointers to interface{} to scan into
	}

	// Iterate through the rows
	for dataRows.Next() {
		// Scan the row into the values slice
		if err := dataRows.Scan(values...); err != nil {
			log.Fatal("Error scanning data row: ", err)
		}

		// Process the values
		for i, v := range values {
			// Use type assertion to handle the value
			switch v := *(v.(*interface{})).(type) {
			case nil:
				fmt.Printf("Column %s: NULL\n", columns[i])
			default:
				fmt.Printf("Column %s: %v\n", columns[i], v)
			}
		}
		fmt.Println() // Print a newline for better readability
	}

	if err := dataRows.Err(); err != nil {
		log.Fatal("Error with data rows: ", err)
	}
}
