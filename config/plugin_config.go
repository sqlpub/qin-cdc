package config

type MysqlConfig struct {
	Host     string
	Port     int
	UserName string
	Password string
	Options  map[string]interface{}
}

type StarrocksConfig struct {
	Host     string
	Port     int
	LoadPort int `mapstructure:"load-port"`
	UserName string
	Password string
	Options  struct {
		BatchSize       int `toml:"batch-size"`
		BatchIntervalMs int `toml:"batch-interval-ms"`
	}
}
