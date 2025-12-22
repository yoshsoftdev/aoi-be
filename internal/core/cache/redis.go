package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"aoi-backend/internal/core/config"

	"github.com/redis/go-redis/v9"
)

var (
	RedisClient *redis.Client
	ctx         = context.Background()
)

type Cache interface {
	Set(key string, value interface{}, ttl time.Duration) error
	Get(key string, dest interface{}) error
	Delete(key string) error
	Exists(key string) (bool, error)
}

type RedisCache struct {
	client *redis.Client
}

type InMemoryCache struct {
	data map[string]string
	mu   sync.RWMutex // Protects concurrent access to map
}

func Init() (Cache, error) {
	cfg := config.Get()

	if !cfg.Redis.Enabled {
		// Use in-memory cache as fallback
		return &InMemoryCache{
			data: make(map[string]string),
		}, nil
	}

	client := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%s", cfg.Redis.Host, cfg.Redis.Port),
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	})

	// Test connection
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to redis: %w", err)
	}

	RedisClient = client
	return &RedisCache{client: client}, nil
}

func (r *RedisCache) Set(key string, value interface{}, ttl time.Duration) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return r.client.Set(ctx, key, data, ttl).Err()
}

func (r *RedisCache) Get(key string, dest interface{}) error {
	data, err := r.client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return fmt.Errorf("key not found: %s", key)
	}
	if err != nil {
		return err
	}
	return json.Unmarshal(data, dest)
}

func (r *RedisCache) Delete(key string) error {
	return r.client.Del(ctx, key).Err()
}

func (r *RedisCache) Exists(key string) (bool, error) {
	count, err := r.client.Exists(ctx, key).Result()
	return count > 0, err
}

func (m *InMemoryCache) Set(key string, value interface{}, ttl time.Duration) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.data[key] = string(data)
	m.mu.Unlock()
	// Note: In-memory cache doesn't support TTL, but it's fine for edge device
	return nil
}

func (m *InMemoryCache) Get(key string, dest interface{}) error {
	m.mu.RLock()
	data, ok := m.data[key]
	m.mu.RUnlock()
	if !ok {
		return fmt.Errorf("key not found: %s", key)
	}
	return json.Unmarshal([]byte(data), dest)
}

func (m *InMemoryCache) Delete(key string) error {
	m.mu.Lock()
	delete(m.data, key)
	m.mu.Unlock()
	return nil
}

func (m *InMemoryCache) Exists(key string) (bool, error) {
	m.mu.RLock()
	_, ok := m.data[key]
	m.mu.RUnlock()
	return ok, nil
}
