package service

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/cinvat/peretum/internal/config"
	"gopkg.in/yaml.v3"
)

// defaultTargetDir is the directory where target configs are stored.
var defaultTargetDir = "config.d"

// SetDefaultTargetDir sets the directory used by the target service for
// testing / override purposes.
func SetDefaultTargetDir(dir string) {
	defaultTargetDir = dir
}

// ListTargets reads all target configuration files from the configured directory
// and returns them.
func ListTargets() ([]*config.TargetConfig, error) {
	entries, err := os.ReadDir(defaultTargetDir)
	if err != nil {
		return nil, fmt.Errorf("read target dir %s: %w", defaultTargetDir, err)
	}

	var targets []*config.TargetConfig
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		ext := filepath.Ext(entry.Name())
		if ext != ".yaml" && ext != ".yml" {
			continue
		}

		path := filepath.Join(defaultTargetDir, entry.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", path, err)
		}

		var t config.TargetConfig
		if err := yaml.Unmarshal(data, &t); err != nil {
			return nil, fmt.Errorf("unmarshal %s: %w", path, err)
		}

		targets = append(targets, &t)
	}

	if len(targets) == 0 {
		return nil, config.ErrNoTargetFiles
	}

	return targets, nil
}

// CreateTarget writes a target configuration file named <server_name>.yaml
// in the configured directory. If a file with that name already exists, it
// returns an error.
func CreateTarget(t *config.TargetConfig) error {
	if t == nil {
		return fmt.Errorf("target config is nil")
	}

	serverName := t.ServerName
	if serverName == "" {
		return fmt.Errorf("server_name is required to determine filename")
	}

	filename := serverName + ".yaml"
	path := filepath.Join(defaultTargetDir, filename)

	if _, err := os.Stat(path); err == nil {
		return fmt.Errorf("target %s already exists", filename)
	}

	data, err := yaml.Marshal(t)
	if err != nil {
		return fmt.Errorf("marshal target: %w", err)
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("write %s: %w", filename, err)
	}

	return nil
}

// UpdateTarget writes/replaces a target configuration file named <server_name>.yaml
// in the configured directory. The server_name path parameter is used as the filename.
func UpdateTarget(serverName string, t *config.TargetConfig) error {
	if t == nil {
		return fmt.Errorf("target config is nil")
	}

	if serverName == "" {
		return fmt.Errorf("server_name is required to determine filename")
	}

	// Ensure the target's server_name matches the filename we're writing,
	// so the structure is consistent.
	t.ServerName = serverName

	filename := serverName + ".yaml"
	path := filepath.Join(defaultTargetDir, filename)

	data, err := yaml.Marshal(t)
	if err != nil {
		return fmt.Errorf("marshal target: %w", err)
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("write %s: %w", filename, err)
	}

	return nil
}

// DeleteTarget removes the target configuration file <server_name>.yaml
// from the configured directory.
func DeleteTarget(serverName string) error {
	if serverName == "" {
		return fmt.Errorf("server_name is required to determine filename")
	}

	filename := serverName + ".yaml"
	path := filepath.Join(defaultTargetDir, filename)

	if err := os.Remove(path); err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("target %s does not exist", filename)
		}
		return fmt.Errorf("remove %s: %w", filename, err)
	}

	return nil
}
