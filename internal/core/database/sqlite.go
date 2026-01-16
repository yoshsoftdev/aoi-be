package database

import (
	"fmt"
	"os"
	"path/filepath"

	"aoi-backend/internal/core/config"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var DB *gorm.DB

func Init() error {
	cfg := config.Get()
	
	dbPath := cfg.DB.Path
	dir := filepath.Dir(dbPath)
	
	// Create directory if it doesn't exist
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create database directory: %w", err)
	}

	// Enable WAL mode for better concurrency and foreign key constraints
	dsn := fmt.Sprintf("%s?_journal_mode=WAL&_foreign_keys=1", dbPath)
	
	var logLevel logger.LogLevel
	if cfg.App.Env == "development" {
		logLevel = logger.Info
	} else {
		logLevel = logger.Error
	}

	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logLevel),
	})
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}

	// Enable foreign key constraints (required for CASCADE to work)
	if err := db.Exec("PRAGMA foreign_keys = ON").Error; err != nil {
		return fmt.Errorf("failed to enable foreign keys: %w", err)
	}

	DB = db
	return nil
}

func GetDB() *gorm.DB {
	if DB == nil {
		panic("database not initialized, call Init() first")
	}
	return DB
}

