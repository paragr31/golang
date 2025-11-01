package dbutil

import (
    "database/sql"
)

// MapScan reads one row into a map[columnName] = value,
// optionally filtering to only wantedCols.
func MapScan(rows *sql.Rows, wantedCols []string) (map[string]interface{}, error) {
    columns, err := rows.Columns()
    if err != nil {
        return nil, err
    }

    wanted := make(map[string]bool)
    if len(wantedCols) > 0 {
        for _, c := range wantedCols {
            wanted[c] = true
        }
    }

    values := make([]interface{}, len(columns))
    valuePtrs := make([]interface{}, len(columns))
    for i := range values {
        valuePtrs[i] = &values[i]
    }

    if err := rows.Scan(valuePtrs...); err != nil {
        return nil, err
    }

    rowMap := make(map[string]interface{})
    for i, col := range columns {
        if len(wanted) > 0 && !wanted[col] {
            continue
        }

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

// MapAllRows reads all rows and returns a map keyed by a given column.
// Example: keyColumn = "COLUMN_NAME" → rows["OrderID"] = {...}
func MapAllRows(rows *sql.Rows, wantedCols []string, keyColumn string) (map[string]map[string]interface{}, error) {
    defer rows.Close()

    allRows := make(map[string]map[string]interface{})
    for rows.Next() {
        rowMap, err := MapScan(rows, wantedCols)
        if err != nil {
            return nil, err
        }

        // Get key value
        keyVal, ok := rowMap[keyColumn]
        if !ok || keyVal == nil {
            continue
        }
        keyStr := fmt.Sprintf("%v", keyVal)

        allRows[keyStr] = rowMap
    }

    if err := rows.Err(); err != nil {
        return nil, err
    }

    return allRows, nil
}
