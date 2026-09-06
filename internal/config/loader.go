package config

import (
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// LoadProxy loads the global proxy configuration
func LoadProxy(path string) (*ProxyConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg ProxyConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// LoadTarget loads a single target configuration from a file
func LoadTarget(path string) (*TargetConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var target TargetConfig
	if err := yaml.Unmarshal(data, &target); err != nil {
		return nil, err
	}

	return &target, nil
}

// LoadTargets loads all target configurations from a directory
func LoadTargets(dir string) ([]TargetConfig, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var targets []TargetConfig
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		ext := filepath.Ext(entry.Name())
		if ext != ".yaml" && ext != ".yml" {
			continue
		}

		path := filepath.Join(dir, entry.Name())
		target, err := LoadTarget(path)
		if err != nil {
			return nil, err
		}
		targets = append(targets, *target)
	}

	if len(targets) == 0 {
		return nil, ErrNoTargetFiles
	}

	return targets, nil
}

var ErrNoTargetFiles = &configError{"no target config files found"}
