// Package config provides runtime configuration loaded from environment variables.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config holds all runtime configuration for the server.
// It is created once at startup by Load and injected into every component.
type Config struct {
	Port            string
	LogLevel        string
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	IdleTimeout     time.Duration
	MaxMessageBytes int64
	PingInterval    time.Duration
	PongWait        time.Duration
	WriteWait       time.Duration
}

// Load reads configuration from environment variables, applying defaults where absent.
// Returns a descriptive error wrapping which field failed validation.
func Load() (*Config, error) {
	cfg := &Config{
		Port:     getEnv("PORT", "8080"),
		LogLevel: getEnv("LOG_LEVEL", "info"),
	}

	var err error
	if cfg.ReadTimeout, err = getDuration("READ_TIMEOUT", "10s"); err != nil {
		return nil, err
	}
	if cfg.WriteTimeout, err = getDuration("WRITE_TIMEOUT", "10s"); err != nil {
		return nil, err
	}
	if cfg.IdleTimeout, err = getDuration("IDLE_TIMEOUT", "120s"); err != nil {
		return nil, err
	}
	if cfg.MaxMessageBytes, err = getInt64("MAX_MESSAGE_BYTES", 4096); err != nil {
		return nil, err
	}
	if cfg.PingInterval, err = getDuration("PING_INTERVAL", "30s"); err != nil {
		return nil, err
	}
	if cfg.PongWait, err = getDuration("PONG_WAIT", "60s"); err != nil {
		return nil, err
	}
	if cfg.WriteWait, err = getDuration("WRITE_WAIT", "10s"); err != nil {
		return nil, err
	}

	port, err := strconv.Atoi(cfg.Port)
	if err != nil || port < 1 || port > 65535 {
		return nil, fmt.Errorf("config: PORT %q is not a valid port number (1-65535)", cfg.Port)
	}
	return cfg, nil
}

func getEnv(key, defaultValue string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultValue
}

func getDuration(key, defaultValue string) (time.Duration, error) {
	s := getEnv(key, defaultValue)
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("config: %s=%q is not a valid duration: %w", key, s, err)
	}
	return d, nil
}

func getInt64(key string, defaultValue int64) (int64, error) {
	s := os.Getenv(key)
	if s == "" {
		return defaultValue, nil
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("config: %s=%q is not a valid integer: %w", key, s, err)
	}
	return v, nil
}
