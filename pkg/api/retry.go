package api

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"time"
)

// RetryConfig defines the parameters for a retry operation.
type RetryConfig struct {
	MaxAttempts       int
	InitialDelay      time.Duration
	MaxDelay          time.Duration
	Multiplier        float64
	Jitter            bool
	EnableIdempotency bool
}

// DefaultRetryConfigs provides standard retry settings for various API operations.
var DefaultRetryConfigs = map[string]RetryConfig{
	"PUT": {
		MaxAttempts:       3,
		InitialDelay:      100 * time.Millisecond,
		MaxDelay:          5 * time.Second,
		Multiplier:        2.0,
		Jitter:            true,
		EnableIdempotency: true,
	},
	"DELETE": {
		MaxAttempts:       3,
		InitialDelay:      100 * time.Millisecond,
		MaxDelay:          5 * time.Second,
		Multiplier:        2.0,
		Jitter:            true,
		EnableIdempotency: true,
	},
	"GET": {
		MaxAttempts:       2,
		InitialDelay:      50 * time.Millisecond,
		MaxDelay:          2 * time.Second,
		Multiplier:        2.0,
		Jitter:            true,
		EnableIdempotency: false,
	},
}

// RetryableFunc is a function signature that fits into the Retrier's execution loop.
type RetryableFunc func(ctx context.Context) error

// Retrier implements automated retry logic with exponential backoff.
type Retrier struct {
	config RetryConfig
}

// NewRetrier initializes a new Retrier with the specified configuration.
func NewRetrier(config RetryConfig) *Retrier {
	return &Retrier{config: config}
}

// ExecuteWithRetry runs the provided function and retries it upon failure
// based on the Retrier's configuration.
func (r *Retrier) ExecuteWithRetry(ctx context.Context, fn RetryableFunc) error {
	var lastErr error

	for attempt := 0; attempt < r.config.MaxAttempts; attempt++ {
		err := fn(ctx)
		if err == nil {
			return nil // Success!
		}

		lastErr = err

		if !r.isRetryableError(err) {
			return err // Don't retry
		}

		if attempt < r.config.MaxAttempts-1 {
			delay := r.calculateBackoff(attempt)

			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	return fmt.Errorf("max retry attempts (%d) exceeded: %w",
		r.config.MaxAttempts, lastErr)
}

func (r *Retrier) calculateBackoff(attempt int) time.Duration {
	delay := float64(r.config.InitialDelay) * math.Pow(r.config.Multiplier, float64(attempt))

	if delay > float64(r.config.MaxDelay) {
		delay = float64(r.config.MaxDelay)
	}

	if r.config.Jitter {
		jitter := rand.Float64() * delay * 0.3
		delay = delay + jitter - (delay * 0.15)
	}

	return time.Duration(delay)
}

func (r *Retrier) isRetryableError(err error) bool {
	if err == nil {
		return false
	}

	errStr := strings.ToLower(err.Error())

	retryable := []string{
		"timeout",
		"no leader",
		"election in progress",
		"leader unavailable",
		"network",
		"connection refused",
		"eof",
		"deadline exceeded",
	}

	for _, pattern := range retryable {
		if strings.Contains(errStr, pattern) {
			return true
		}
	}

	nonRetryable := []string{
		"invalid",
		"bad request",
		"not found",
		"conflict",
	}

	for _, pattern := range nonRetryable {
		if strings.Contains(errStr, pattern) {
			return false
		}
	}

	return true
}
