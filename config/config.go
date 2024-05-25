package config

import (
	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
	"github.com/siddontang/go-log/log"
	"path/filepath"
)

type Config struct {
	Name             string
	InputConfig      InputConfig       `toml:"input"`
	OutputConfig     OutputConfig      `toml:"output"`
	TransformsConfig []TransformConfig `toml:"transforms"`
	FileName         *string
}

type InputConfig struct {
	Type   string                 `toml:"type"`
	Config map[string]interface{} `toml:"config"`
}

type OutputConfig struct {
	Type   string                 `toml:"type"`
	Config map[string]interface{} `toml:"config"`
}

type TransformConfig struct {
	Type   string                 `toml:"type"`
	Config map[string]interface{} `toml:"config"`
}

func NewConfig(fileName *string) (c *Config) {
	c = &Config{}
	fileNamePath, err := filepath.Abs(*fileName)
	if err != nil {
		log.Fatal(err)
	}
	c.FileName = &fileNamePath
	err = c.ReadConfig()
	if err != nil {
		log.Fatal(err)
	}
	return c
}

func (c *Config) ReadConfig() error {
	var err error
	if _, err = toml.DecodeFile(*c.FileName, c); err != nil {
		return errors.Trace(err)
	}
	return err
}
