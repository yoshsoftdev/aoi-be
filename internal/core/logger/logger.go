package logger

import (
	"aoi-backend/internal/core/config"
	"os"

	"github.com/sirupsen/logrus"
)

var Log *logrus.Logger

func Init() {
	Log = logrus.New()
	cfg := config.Get()

	// Set output to stdout (terminal) explicitly
	Log.SetOutput(os.Stdout)

	// Set log level - default to Debug for development, Info for production
	level, err := logrus.ParseLevel(cfg.Log.Level)
	if err != nil {
		// Default to Debug level for better visibility
		if cfg.App.Env == "development" {
			level = logrus.DebugLevel
		} else {
			level = logrus.InfoLevel
		}
	}
	Log.SetLevel(level)

	// Set log format - default to text format for better readability in terminal
	if cfg.Log.Format == "json" {
		Log.SetFormatter(&logrus.JSONFormatter{})
	} else {
		Log.SetFormatter(&logrus.TextFormatter{
			FullTimestamp:   true,
			TimestampFormat: "2006-01-02 15:04:05",
			ForceColors:     true,
			DisableColors:   false,
		})
	}
}

func GetLogger() *logrus.Logger {
	if Log == nil {
		Init()
	}
	return Log
}
