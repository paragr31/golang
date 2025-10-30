package config

import (
    "fmt"
    "strings"

    "gopkg.in/ini.v1"
)

type SnowflakeConfig struct {
    Account              string
    User                 string
    PrivateKeyPath       string
    Role                 string
    Warehouse            string
    Database             string
    TableList            []string
    ReplaceNewlineFields map[string][]string
    TrimDateFields       map[string][]string
}

func parseMapOfLists(value string) map[string][]string {
    result := make(map[string][]string)
    pairs := strings.Split(value, ";")
    for _, pair := range pairs {
        pair = strings.TrimSpace(pair)
        if pair == "" {
            continue
        }
        parts := strings.SplitN(pair, ":", 2)
        if len(parts) != 2 {
            continue
        }
        key := strings.TrimSpace(parts[0])
        values := strings.Split(parts[1], ",")
        for i := range values {
            values[i] = strings.TrimSpace(values[i])
        }
        result[key] = values
    }
    return result
}

func LoadSnowflakeConfig(path string) (*SnowflakeConfig, error) {
    cfg, err := ini.Load(path)
    if err != nil {
        return nil, err
    }

    s := cfg.Section("SNOWFLAKE")
    sc := &SnowflakeConfig{
        Account:        s.Key("account").String(),
        User:           s.Key("user").String(),
        PrivateKeyPath: s.Key("privateKeyPath").String(),
        Role:           s.Key("role").String(),
        Warehouse:      s.Key("warehouse").String(),
        Database:       s.Key("database").String(),
    }

    // Parse list of tables
    sc.TableList = splitAndTrim(s.Key("table_list").String(), ",")

    // Parse map-of-lists
    sc.ReplaceNewlineFields = parseMapOfLists(s.Key("replace_newline_fields").String())
    sc.TrimDateFields = parseMapOfLists(s.Key("trim_date_fields").String())

    return sc, nil
}

func splitAndTrim(s string, sep string) []string {
    if s == "" {
        return nil
    }
    parts := strings.Split(s, sep)
    for i := range parts {
        parts[i] = strings.TrimSpace(parts[i])
    }
    return parts
}
