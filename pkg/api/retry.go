package api

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"time"
)

// RetryConfig holds configuration for retry logic
type RetryConfig struct {
	MaxAttempts       int
	InitialDelay      time.Duration
	MaxDelay          time.Duration
	Multiplier        float64
	Jitter            bool
	EnableIdempotency bool
}

// DefaultRetryConfigs provides sensible defaults for different operations
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

// RetryableFunc is a function that can be retried
type RetryableFunc func(ctx context.Context) error

// Retrier implements retry logic with exponential backoff
type Retrier struct {
	config RetryConfig
}

// NewRetrier creates a new Retrier with given configuration
func NewRetrier(config RetryConfig) *Retrier {
	return &Retrier{config: config}
}

// ExecuteWithRetry executes a function with retry logic
func (r *Retrier) ExecuteWithRetry(ctx context.Context, fn RetryableFunc) error {
	var lastErr error

	for attempt := 0; attempt < r.config.MaxAttempts; attempt++ {
		// Execute the function
		err := fn(ctx)
		if err == nil {
			return nil // Success!
		}

		lastErr = err

		// Check if error is retryable
		if !r.isRetryableError(err) {
			return err // Don't retry
		}

		// Don't sleep after last attempt
		if attempt < r.config.MaxAttempts-1 {
			delay := r.calculateBackoff(attempt)

			select {
			case <-time.After(delay):
				// Continue to next attempt
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	return fmt.Errorf("max retry attempts (%d) exceeded: %w",
		r.config.MaxAttempts, lastErr)
}

// calculateBackoff calculates the backoff delay with exponential backoff and jitter
func (r *Retrier) calculateBackoff(attempt int) time.Duration {
	// Exponential backoff: initialDelay * (multiplier ^ attempt)
	delay := float64(r.config.InitialDelay) * math.Pow(r.config.Multiplier, float64(attempt))

	// Cap at max delay
	if delay > float64(r.config.MaxDelay) {
		delay = float64(r.config.MaxDelay)
	}

	// Add jitter to prevent thundering herd
	if r.config.Jitter {
		jitter := rand.Float64() * delay * 0.3 // ±30% jitter
		delay = delay + jitter - (delay * 0.15)
	}

	return time.Duration(delay)
}

// isRetryableError classifies errors as retryable or non-retryable
func (r *Retrier) isRetryableError(err error) bool {
	if err == nil {
		return false
	}

	errStr := strings.ToLower(err.Error())

	// Retryable errors
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

	// Non-retryable errors
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

	// Default: retry on unknown errors
	return true
}
