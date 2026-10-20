package workerpreemption

import (
	"fmt"
	"strings"

	"gopkg.in/yaml.v2"
	"volcano.sh/volcano/pkg/scheduler/conf"
)

// ValidateConfig keeps the extension opt-in and excludes competing eviction paths.
// Call it both at startup and before accepting a hot-reloaded configuration.
func ValidateConfig(data string, enabled bool) error {
	var cfg conf.SchedulerConfiguration
	if err := yaml.Unmarshal([]byte(data), &cfg); err != nil {
		// Leave YAML syntax errors to the upstream configuration loader.
		return nil
	}
	customAction, customPlugin, alternate := false, false, false
	queueAccess := false
	for _, a := range strings.Split(cfg.Actions, ",") {
		switch strings.TrimSpace(a) {
		case "worker-type-preempt":
			customAction = true
		case "preempt", "reclaim":
			alternate = true
		}
	}
	for _, tier := range cfg.Tiers {
		for _, p := range tier.Plugins {
			if p.Name == "queue-access" {
				queueAccess = true
			}
			if p.Name == name {
				customPlugin = true
			}
		}
	}
	if !customAction && !customPlugin && !queueAccess {
		return nil
	}
	if !enabled {
		return fmt.Errorf("worker-type preemption requires --enable-worker-type-preemption=true")
	}
	if alternate && (customAction || customPlugin) {
		return fmt.Errorf("worker-type preemption cannot be combined with native preempt/reclaim")
	}
	if customAction && !customPlugin {
		return fmt.Errorf("worker-type-preempt requires worker-type-preemption plugin")
	}
	for _, a := range cfg.Configurations {
		if customAction && a.Name == "worker-type-preempt" && a.Arguments["queueAccess"] == true && (!queueAccess || a.Arguments["pipelineBudgets"] != true) {
			return fmt.Errorf("queueAccess requires queue-access plugin and pipelineBudgets")
		}
	}
	if queueAccess && customAction {
		found := false
		for _, a := range cfg.Configurations {
			if a.Name == "worker-type-preempt" {
				found = a.Arguments["queueAccess"] == true && a.Arguments["pipelineBudgets"] == true
			}
		}
		if !found {
			return fmt.Errorf("queue-access with custom preemption requires queueAccess and pipelineBudgets action arguments")
		}
	}
	return nil
}
