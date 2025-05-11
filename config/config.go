package config

import (
	"github.com/spf13/viper"
)

type Config struct {
	GRPC struct {
		Port string `mapstructure:"port"`
	} `mapstructure:"grpc"`
}

func LoadConfig(path string) (*Config, error) {
	viper.AddConfigPath(path)
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")

	if err := viper.ReadInConfig(); err != nil {
		return nil, err
	}

	var cfg Config

	if err := viper.Unmarshal(&cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}
