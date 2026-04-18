package invalidator

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/Tkach360/tk_cdc/internal/config"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var TIMEOUT time.Duration = 60 * time.Second

func TestInvalidator_Invalidate(t *testing.T) {
	tests := []struct {
		name           string
		keysInvalidate []string
		keysExpected   []struct {
			key      string
			isExists bool
		}
	}{
		{
			name:           "delete single existing key",
			keysInvalidate: []string{"user:1"},
			keysExpected: []struct {
				key      string
				isExists bool
			}{
				{key: "user:1", isExists: false},
				{key: "user:2", isExists: true},
			},
		},
		{
			name:           "delete multiple existing keys",
			keysInvalidate: []string{"user:1", "user:2", "user:3"},
			keysExpected: []struct {
				key      string
				isExists bool
			}{
				{key: "user:1", isExists: false},
				{key: "user:2", isExists: false},
				{key: "user:3", isExists: false},
				{key: "user:4", isExists: true},
			},
		},
		{
			name:           "delete non-existent key",
			keysInvalidate: []string{"non-existent-key"},
			keysExpected: []struct {
				key      string
				isExists bool
			}{
				{key: "user:1", isExists: true},
				{key: "non-existent-key", isExists: false},
			},
		},
		{
			name:           "delete mix of existing and non-existent keys",
			keysInvalidate: []string{"user:1", "missing:1", "user:3", "missing:2"},
			keysExpected: []struct {
				key      string
				isExists bool
			}{
				{key: "user:1", isExists: false},
				{key: "user:2", isExists: true},
				{key: "user:3", isExists: false},
				{key: "missing:1", isExists: false},
				{key: "missing:2", isExists: false},
			},
		},
		{
			name:           "delete with empty slice",
			keysInvalidate: []string{},
			keysExpected: []struct {
				key      string
				isExists bool
			}{
				{key: "user:1", isExists: true},
				{key: "user:2", isExists: true},
			},
		},
		{
			name:           "delete with nil slice",
			keysInvalidate: nil,
			keysExpected: []struct {
				key      string
				isExists bool
			}{
				{key: "user:1", isExists: true},
				{key: "user:2", isExists: true},
			},
		},
		{
			name:           "delete duplicate keys in request",
			keysInvalidate: []string{"user:1", "user:1", "user:2", "user:1"},
			keysExpected: []struct {
				key      string
				isExists bool
			}{
				{key: "user:1", isExists: false},
				{key: "user:2", isExists: false},
				{key: "user:3", isExists: true},
			},
		},
		{
			name:           "delete keys with special characters",
			keysInvalidate: []string{"user:with:colon", "user:with space", "user:with@special"},
			keysExpected: []struct {
				key      string
				isExists bool
			}{
				{key: "user:with:colon", isExists: false},
				{key: "user:with space", isExists: false},
				{key: "user:with@special", isExists: false},
				{key: "normal:key", isExists: true},
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), TIMEOUT)
	defer cancel()

	redisContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "redis:7-alpine",
			ExposedPorts: []string{"6379/tcp"},
			WaitingFor:   wait.ForLog("Ready to accept connections"),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("failed to start redis: %v", err)
	}
	defer redisContainer.Terminate(ctx)

	redisHost, _ := redisContainer.Host(ctx)
	redisPort, _ := redisContainer.MappedPort(ctx, "6379")
	redisAddr := fmt.Sprintf("%s:%s", redisHost, redisPort.Port())

	client := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})

	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping redis: %v", err)
	}

	cfg := config.RedisConfig{Addr: redisAddr}
	logger := slog.New(slog.DiscardHandler)
	inv, err := New(&cfg, logger)
	if err != nil {
		t.Fatalf("invalidator init error: %v", err)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, key := range tt.keysExpected {
				err = client.Set(ctx, key.key, "some data", 0).Err()
				if err != nil {
					t.Fatalf("execution SET: %v", err)
				}
			}

			err = inv.Invalidate(ctx, tt.keysInvalidate)
			if err != nil {
				t.Fatalf("invalidate: %v", err)
			}

			for _, key := range tt.keysExpected {
				count, err := client.Exists(ctx, key.key).Result()
				if err != nil {
					t.Fatalf("existence check: %v", err)
				}

				assert.Equal(t, key.isExists, count != 0, fmt.Sprintf("existing key \"%s\"", key.key))
			}
		})
	}
}
