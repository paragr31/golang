package main

import (
	"compress/gzip"
	"context"
	"crypto/rsa"
	"crypto/x509"
	"database/sql"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	gosnowflake "github.com/snowflakedb/gosnowflake"
	_ "github.com/snowflakedb/gosnowflake"
)

type Config struct {
	Snowflake struct {
		Account        string `json:"account"`
		User           string `json:"user"`
		PrivateKeyPath string `json:"privateKeyPath"`
		Role           string `json:"role"`
		Warehouse      string `json:"warehouse"`
		Database       string `json:"database"`
	} `json:"snowflake"`
	Output struct {
		OutputDir        string `json:"outputDir"`
		FieldSeparator   string `json:"fieldSeparator"`
		LineSeparator    string `json:"lineSeparator"`
		NullString       string `json:"nullString"`
		EscapeSequence   string `json:"escapeSequence"`
		NewlineReplacement string `json:"newlineReplacement"`
		BooleanAsInt     bool   `json:"booleanAsInt"`
		DateFormat       string `json:"dateFormat"`
		TimestampFormat  string `json:"timestampFormat"`
	} `json:"output"`
	Tables []struct {
		Schema string `json:"schema"`
		Name   string `json:"name"`
	} `json:"tables"`
}

// loadPrivateKey reads PEM and supports PKCS1 and PKCS8 RSA private keys.
func loadPrivateKey(path string) (*rsa.PrivateKey, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	block, _ := pem.Decode(b)
	if block == nil {
		return nil, fmt.Errorf("failed to parse PEM")
	}

	// try PKCS8
	if key, err := x509.ParsePKCS8PrivateKey(block.Bytes); err == nil {
		if rsaKey, ok := key.(*rsa.PrivateKey); ok {
			return rsaKey, nil
		}
	}

	// try PKCS1
	if rsaKey, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		return rsaKey, nil
	}

	return nil, fmt.Errorf("unsupported private key format")
}

// escapeSQL single quotes for embedding string literals in SQL
func escapeSQL(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// buildColumnExpression returns the SQL expression that formats & sanitizes a column for export.
// It returns something like: COALESCE(REPLACE(REPLACE(TO_VARCHAR("COL", 'fmt'), '\r', ' '), '\n', ' '), '\N') AS "COL"
func buildColumnExpression(colName string, dataType string, outCfg ConfigOutputHelpers) string {
	ident := fmt.Sprintf(`"%s"`, colName)
	upper := strings.ToUpper(dataType)

	// base expression (string-producing)
	var base string
	if strings.Contains(upper, "TIMESTAMP") {
		// timestamp with configured format
		base = fmt.Sprintf("TO_VARCHAR(%s, '%s')", ident, escapeSQL(outCfg.TimestampFormat))
	} else if strings.Contains(upper, "DATE") && !strings.Contains(upper, "TIME") {
		base = fmt.Sprintf("TO_VARCHAR(%s, '%s')", ident, escapeSQL(outCfg.DateFormat))
	} else if strings.Contains(upper, "BOOLEAN") || strings.Contains(upper, "BOOL") {
		if outCfg.BooleanAsInt {
			// map true->'1', false->'0'
			base = fmt.Sprintf("IFF(%s, '1', '0')", ident)
		} else {
			base = fmt.Sprintf("TO_VARCHAR(%s)", ident)
		}
	} else if strings.Contains(upper, "VARIANT") || strings.Contains(upper, "OBJECT") || strings.Contains(upper, "ARRAY") {
		base = fmt.Sprintf("TO_JSON(%s)", ident)
	} else {
		// numeric / varchar / other: use TO_VARCHAR()
		base = fmt.Sprintf("TO_VARCHAR(%s)", ident)
	}

	// sanitize newlines and carriage returns
	base = fmt.Sprintf("REPLACE(REPLACE(%s, '\\r', '%s'), '\\n', '%s')", base, escapeSQL(outCfg.NewlineReplacement), escapeSQL(outCfg.NewlineReplacement))
	// replace occurrences of the field separator inside the value
	base = fmt.Sprintf("REPLACE(%s, '%s', '%s')", base, escapeSQL(outCfg.FieldSeparator), escapeSQL(outCfg.EscapeSequence))
	// coalesce null to configured token
	expr := fmt.Sprintf("COALESCE(%s, '%s') AS %s", base, escapeSQL(outCfg.NullString), ident)
	return expr
}

// small helper struct (to avoid long names inside function)
type ConfigOutputHelpers struct {
	FieldSeparator    string
	LineSeparator     string
	NullString        string
	EscapeSequence    string
	NewlineReplacement string
	BooleanAsInt      bool
	DateFormat        string
	TimestampFormat   string
}

func main() {
	// load config.json (same dir)
	f, err := os.Open("config.json")
	if err != nil {
		log.Fatalf("open config.json: %v", err)
	}
	defer f.Close()

	var cfg Config
	if err := json.NewDecoder(f).Decode(&cfg); err != nil {
		log.Fatalf("decode config: %v", err)
	}

	// create output dir
	if err := os.MkdirAll(cfg.Output.OutputDir, 0o755); err != nil {
		log.Fatalf("create output dir: %v", err)
	}

	// load private key
	priv, err := loadPrivateKey(cfg.Snowflake.PrivateKeyPath)
	if err != nil {
		log.Fatalf("load private key: %v", err)
	}

	// build DSN for keypair (JWT) auth
	sfCfg := gosnowflake.Config{
		Account:       cfg.Snowflake.Account,
		User:          cfg.Snowflake.User,
		Role:          cfg.Snowflake.Role,
		Warehouse:     cfg.Snowflake.Warehouse,
		Database:      cfg.Snowflake.Database,
		Authenticator: gosnowflake.AuthTypeJwt,
		PrivateKey:    priv,
	}
	dsn, err := gosnowflake.DSN(&sfCfg)
	if err != nil {
		log.Fatalf("build dsn: %v", err)
	}

	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer db.Close()

	outHelper := ConfigOutputHelpers{
		FieldSeparator:    cfg.Output.FieldSeparator,
		LineSeparator:     cfg.Output.LineSeparator,
		NullString:        cfg.Output.NullString,
		EscapeSequence:    cfg.Output.EscapeSequence,
		NewlineReplacement: cfg.Output.NewlineReplacement,
		BooleanAsInt:      cfg.Output.BooleanAsInt,
		DateFormat:        cfg.Output.DateFormat,
		TimestampFormat:   cfg.Output.TimestampFormat,
	}

	ctx := context.Background()

	for _, t := range cfg.Tables {
		schema := t.Schema
		table := t.Name

		// fetch column metadata and types, ordered by ordinal_position
		metaQ := `
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = ? AND table_name = ?
ORDER BY ordinal_position
`
		rows, err := db.QueryContext(ctx, metaQ, schema, table)
		if err != nil {
			log.Printf("metadata query failed for %s.%s: %v", schema, table, err)
			continue
		}
		cols := []string{}
		types := []string{}
		for rows.Next() {
			var col, dtype string
			if err := rows.Scan(&col, &dtype); err != nil {
				log.Printf("scan meta: %v", err)
				continue
			}
			cols = append(cols, col)
			types = append(types, dtype)
		}
		rows.Close()

		if len(cols) == 0 {
			log.Printf("no columns found for %s.%s, skipping", schema, table)
			continue
		}

		// build SELECT with formatting expressions
		exprs := make([]string, len(cols))
		for i := range cols {
			exprs[i] = buildColumnExpression(cols[i], types[i], outHelper)
		}
		selectList := strings.Join(exprs, ", ")
		query := fmt.Sprintf(`SELECT %s FROM "%s"."%s"`, selectList, schema, table)

		log.Printf("Exporting %s.%s -> SQL: (first 200 chars) %.200s", schema, table, query)
		r, err := db.QueryContext(ctx, query)
		if err != nil {
			log.Printf("query error for %s.%s: %v", schema, table, err)
			continue
		}

		// open gz file
		outPath := filepath.Join(cfg.Output.OutputDir, fmt.Sprintf("%s_%s.gz", schema, table))
		fout, err := os.Create(outPath)
		if err != nil {
			r.Close()
			log.Printf("create out file: %v", err)
			continue
		}
		gw := gzip.NewWriter(fout)

		// stream rows
		colNames, _ := r.Columns()
		colCount := len(colNames)

		// allocate scan pointers (each a distinct *interface{})
		valuePtrs := make([]interface{}, colCount)

		for i := 0; i < colCount; i++ {
			var v interface{}
			valuePtrs[i] = &v
		}

		for r.Next() {
			if err := r.Scan(valuePtrs...); err != nil {
				log.Printf("scan row error: %v", err)
				break
			}
			fields := make([]string, colCount)
			for i := 0; i < colCount; i++ {
				raw := *(valuePtrs[i].(*interface{}))
				var s string
				if raw == nil {
					s = cfg.Output.NullString
				} else {
					// because we forced TO_VARCHAR/COALESCE on the SQL side, driver will usually return []byte for strings
					switch v := raw.(type) {
					case []byte:
						s = string(v)
					case string:
						s = v
					default:
						// fallback
						s = fmt.Sprintf("%v", v)
					}
				}
				fields[i] = s
			}
			line := strings.Join(fields, cfg.Output.FieldSeparator) + cfg.Output.LineSeparator
			if _, err := io.WriteString(gw, line); err != nil {
				log.Printf("write gz error: %v", err)
				break
			}
		}

		// close resources
		r.Close()
		if err := gw.Close(); err != nil {
			log.Printf("close gzip writer: %v", err)
		}
		if err := fout.Close(); err != nil {
			log.Printf("close file: %v", err)
		}
		log.Printf("Exported %s.%s -> %s", schema, table, outPath)
	}

	log.Printf("done")
}
