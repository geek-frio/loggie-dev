/*
Copyright 2021 Loggie Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package queue

import (
	"github.com/loggie-io/loggie/pkg/core/cfg"
)

type Config struct {
	Enabled    *bool         `yaml:"enabled,omitempty"`
	Name       string        `yaml:"name,omitempty"`
	Type       string        `yaml:"type,omitempty" validate:"required"`
	Properties cfg.CommonCfg `yaml:",inline"`
	BatchSize  int           `yaml:"batchSize,omitempty" default:"2048" validate:"required,gte=1"`
}

func (c *Config) Merge(from *Config) {
	if from == nil {
		return
	}
	if c.Name != from.Name || c.Type != from.Type {
		return
	}

	if c.BatchSize == 0 {
		c.BatchSize = from.BatchSize
	}

	c.Properties = cfg.MergeCommonCfg(c.Properties, from.Properties, false)
}

func (c *Config) DeepCopy() *Config {
	if c == nil {
		return nil
	}

	out := new(Config)
	out.Enabled = c.Enabled
	out.Name = c.Name
	out.Type = c.Type
	out.Properties = c.Properties.DeepCopy()
	out.BatchSize = c.BatchSize

	return out
}
