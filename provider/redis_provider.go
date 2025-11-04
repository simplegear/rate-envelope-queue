package provider

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"

	"github.com/go-redis/redis/v8"
)

type RedisConfig struct {
	Address  string
	Username string
	Password string
	DB       int
}

type RedisProvider struct {
	redisClient                    *redis.Client
	processingTable, fallbackTable string
}

func NewRedisProvider(ctx context.Context, redisCfg RedisConfig, processingTable, fallbackTable string) (*RedisProvider, error) {
	redisClient := redis.NewClient(&redis.Options{
		Addr:     redisCfg.Address,
		Username: redisCfg.Username,
		Password: redisCfg.Password,
	})

	if err := redisClient.Ping(ctx).Err(); err != nil {
		log.Printf("RedisProvider: can't connect to redis: %v", err)
		return nil, err
	}

	return &RedisProvider{
		redisClient:     redisClient,
		processingTable: processingTable,
		fallbackTable:   fallbackTable,
	}, nil
}

func (rp *RedisProvider) SaveOne(ctx context.Context, data Kv) error {
	bytes, err := json.Marshal(data.Value)
	if err != nil {
		return fmt.Errorf("marshal error: %w", err)
	}

	err = rp.redisClient.Set(ctx, string(data.Key), bytes, 0).Err()
	if err != nil {
		return fmt.Errorf("RedisProvider: SaveOne: can't set key %s: %w", data.Key, err)
	}
	return nil
}

func (rp *RedisProvider) TakeOne(ctx context.Context, data Kv) (any, error) {
	val, err := rp.redisClient.Get(ctx, data.Key).Result()
	if err != nil {
		return nil, fmt.Errorf("RedisProvider: TakeOne: can't get key %s: %w", data.Key, err)
	}

	var out any
	if err := json.Unmarshal([]byte(val), &out); err != nil {
		return nil, fmt.Errorf("unmarshal error: %w", err)
	}

	return out, nil
}

func (rp *RedisProvider) GenerateProcessingPK() (string, error) {
	b := RandomBytes(16)
	if b == nil {
		return "", fmt.Errorf("RedisProvider: GeneratePK: can't generate random bytes")
	}
	return rp.processingTable + hex.EncodeToString(b), nil
}

func (rp *RedisProvider) GenerateFallbackPK(pk string) (string, error) {
	return rp.fallbackTable + pk, nil
}

func (rp *RedisProvider) FinishOne(ctx context.Context, data Kv) error {
	err := rp.redisClient.Del(ctx, string(data.Key)).Err()
	if err != nil {
		return fmt.Errorf("RedisProvider: FinishOne: can't delete key %s: %w", data.Key, err)
	}
	return nil
}

func (rp *RedisProvider) NextPosition() error {
	return nil
}

func (rp *RedisProvider) Close() error {
	return rp.redisClient.Close()
}
