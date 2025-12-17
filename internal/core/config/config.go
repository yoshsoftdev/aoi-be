package config

import (
	"fmt"
	"os"

	"github.com/spf13/viper"
)

type Config struct {
	Server ServerConfig
	DB     DBConfig
	Redis  RedisConfig
	Log    LogConfig
	App    AppConfig
}

type ServerConfig struct {
	Port string
	Host string
}

type DBConfig struct {
	Path string
}

type RedisConfig struct {
	Enabled  bool
	Host     string
	Port     string
	Password string
	DB       int
}

type LogConfig struct {
	Level  string
	Format string
}

type AppConfig struct {
	Name string
	Env  string
}

var AppConfigInstance *Config

func Load() (*Config, error) {
	viper.SetConfigName(".env")
	viper.SetConfigType("env")
	viper.AddConfigPath(".")
	viper.AddConfigPath("..")
	viper.AddConfigPath("../..")

	// Set defaults
	viper.SetDefault("SERVER_PORT", "8080")
	viper.SetDefault("SERVER_HOST", "0.0.0.0")
	viper.SetDefault("DB_PATH", "data/aoi.db")
	viper.SetDefault("REDIS_ENABLED", false)
	viper.SetDefault("REDIS_HOST", "localhost")
	viper.SetDefault("REDIS_PORT", "6379")
	viper.SetDefault("REDIS_PASSWORD", "")
	viper.SetDefault("REDIS_DB", 0)
	viper.SetDefault("LOG_LEVEL", "info")
	viper.SetDefault("LOG_FORMAT", "json")
	viper.SetDefault("APP_NAME", "AOI System")
	viper.SetDefault("APP_ENV", "development")

	// Read from environment variables
	viper.AutomaticEnv()

	// Try to read .env file (optional)
	if err := viper.ReadInConfig(); err != nil {
		// .env file is optional, continue with defaults
		fmt.Println("No .env file found, using defaults and environment variables")
	}

	config := &Config{
		Server: ServerConfig{
			Port: viper.GetString("SERVER_PORT"),
			Host: viper.GetString("SERVER_HOST"),
		},
		DB: DBConfig{
			Path: viper.GetString("DB_PATH"),
		},
		Redis: RedisConfig{
			Enabled:  viper.GetBool("REDIS_ENABLED"),
			Host:     viper.GetString("REDIS_HOST"),
			Port:     viper.GetString("REDIS_PORT"),
			Password: viper.GetString("REDIS_PASSWORD"),
			DB:       viper.GetInt("REDIS_DB"),
		},
		Log: LogConfig{
			Level:  viper.GetString("LOG_LEVEL"),
			Format: viper.GetString("LOG_FORMAT"),
		},
		App: AppConfig{
			Name: viper.GetString("APP_NAME"),
			Env:  viper.GetString("APP_ENV"),
		},
	}

	// Create data directory if it doesn't exist
	if err := os.MkdirAll("data", 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	AppConfigInstance = config
	return config, nil
}

func Get() *Config {
	if AppConfigInstance == nil {
		panic("config not loaded, call Load() first")
	}
	return AppConfigInstance
}

