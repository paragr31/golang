package dbutil

import (
    "database/sql"
)

// MapScan reads the current row into a map[columnName] = value,
// but only includes columns listed in wantedCols (if provided).
// If wantedCols is nil or empty, all columns are included.
func MapScan(rows *sql.Rows, wantedCols []string) (map[string]interface{}, error) {
    columns, err := rows.Columns()
    if err != nil {
        return nil, err
    }

    // Prepare a set for quick lookup of wanted columns
    wanted := make(map[string]bool)
    if len(wantedCols) > 0 {
        for _, c := range wantedCols {
            wanted[c] = true
        }
    }

    // Prepare slices for scanning
    values := make([]interface{}, len(columns))
    valuePtrs := make([]interface{}, len(columns))
    for i := range values {
        valuePtrs[i] = &values[i]
    }

    // Scan current row
    if err := rows.Scan(valuePtrs...); err != nil {
        return nil, err
    }

    // Build map with only wanted columns
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
