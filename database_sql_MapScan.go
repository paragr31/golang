package dbutil

import (
    "database/sql"
    "fmt"
)

// MapScan reads the current row into a map[columnName] = value
func MapScan(rows *sql.Rows) (map[string]interface{}, error) {
    columns, err := rows.Columns()
    if err != nil {
        return nil, err
    }

    // Prepare a slice of interface{} to receive each column value
    values := make([]interface{}, len(columns))
    valuePtrs := make([]interface{}, len(columns))
    for i := range values {
        valuePtrs[i] = &values[i]
    }

    // Scan the current row
    if err := rows.Scan(valuePtrs...); err != nil {
        return nil, err
    }

    // Build a map
    rowMap := make(map[string]interface{}, len(columns))
    for i, col := range columns {
        val := values[i]
        switch v := val.(type) {
        case []byte:
            rowMap[col] = string(v)
        default:
            rowMap[col] = v
        }
    }
    return rowMap, nil
}
