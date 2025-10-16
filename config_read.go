package config

import (
    "gopkg.in/ini.v1"
)

func LoadDynamicConfig(path string) (map[string]map[string]string, error) {
    cfg, err := ini.Load(path)
    if err != nil {
        return nil, err
    }

    configMap := make(map[string]map[string]string)

    for _, section := range cfg.Sections() {
        sectionName := section.Name()
        if sectionName == ini.DEFAULT_SECTION {
            continue
        }

        sectionMap := make(map[string]string)
        for _, key := range section.Keys() {
            sectionMap[key.Name()] = key.Value()
        }
        configMap[sectionName] = sectionMap
    }

    return configMap, nil
}
