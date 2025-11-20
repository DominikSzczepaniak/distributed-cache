package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Client is a distributed cache client with automatic redirect support
type Client struct {
	httpClient   *http.Client
	maxRedirects int
}

// NewClient creates a new cache client
func NewClient() *Client {
	return &Client{
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				// Don't auto-follow redirects - we handle them manually
				return http.ErrUseLastResponse
			},
		},
		maxRedirects: 3,
	}
}

// NewClientWithOptions creates a client with custom options
func NewClientWithOptions(timeout time.Duration, maxRedirects int) *Client {
	return &Client{
		httpClient: &http.Client{
			Timeout: timeout,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		maxRedirects: maxRedirects,
	}
}

// PutRequest represents a PUT request
type PutRequest struct {
	Key   int `json:"key"`
	Value int `json:"value"`
}

// PutResponse represents a PUT response
type PutResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

// GetResponse represents a GET response
type GetResponse struct {
	Key   int `json:"key"`
	Value int `json:"value"`
}

// RedirectResponse represents a redirect error response
type RedirectResponse struct {
	Error       string `json:"error"`
	Message     string `json:"message"`
	NodeID      string `json:"nodeId"`
	Address     string `json:"address"`
	PartitionID uint16 `json:"partitionId"`
}

// Put stores a key-value pair, automatically following redirects
func (c *Client) Put(nodeAddr string, key, value int) error {
	url := fmt.Sprintf("%s/kv", nodeAddr)
	reqBody := PutRequest{Key: key, Value: value}

	for i := 0; i < c.maxRedirects; i++ {
		bodyBytes, err := json.Marshal(reqBody)
		if err != nil {
			return fmt.Errorf("failed to marshal request: %w", err)
		}

		req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(bodyBytes))
		if err != nil {
			return fmt.Errorf("failed to create request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("request failed: %w", err)
		}

		if resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			return nil // Success
		}

		if resp.StatusCode == 307 { // Temporary Redirect
			// Parse redirect response
			var redirectResp RedirectResponse
			bodyBytes, readErr := io.ReadAll(resp.Body)
			resp.Body.Close()

			if readErr != nil {
				return fmt.Errorf("failed to read redirect response: %w", readErr)
			}

			if err := json.Unmarshal(bodyBytes, &redirectResp); err != nil {
				return fmt.Errorf("failed to parse redirect response: %w", err)
			}

			if redirectResp.Address == "" {
				return fmt.Errorf("redirect without valid address")
			}

			// Update URL to point to correct node
			url = fmt.Sprintf("%s/kv", redirectResp.Address)
			continue // Retry at new location
		}

		// Other error
		bodyBytes, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return fmt.Errorf("unexpected status %d (failed to read body: %v)", resp.StatusCode, readErr)
		}
		return fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	return fmt.Errorf("too many redirects (max: %d)", c.maxRedirects)
}

// Get retrieves a value by key, automatically following redirects
func (c *Client) Get(nodeAddr string, key int) (int, error) {
	url := fmt.Sprintf("%s/kv/%d", nodeAddr, key)

	for i := 0; i < c.maxRedirects; i++ {
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			return 0, fmt.Errorf("failed to create request: %w", err)
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return 0, fmt.Errorf("request failed: %w", err)
		}

		if resp.StatusCode == http.StatusOK {
			var getResp GetResponse
			bodyBytes, readErr := io.ReadAll(resp.Body)
			resp.Body.Close()

			if readErr != nil {
				return 0, fmt.Errorf("failed to read response: %w", readErr)
			}

			if err := json.Unmarshal(bodyBytes, &getResp); err != nil {
				return 0, fmt.Errorf("failed to parse response: %w", err)
			}

			return getResp.Value, nil
		}

		if resp.StatusCode == 307 { // Temporary Redirect
			var redirectResp RedirectResponse
			bodyBytes, readErr := io.ReadAll(resp.Body)
			resp.Body.Close()

			if readErr != nil {
				return 0, fmt.Errorf("failed to read redirect response: %w", readErr)
			}

			if err := json.Unmarshal(bodyBytes, &redirectResp); err != nil {
				return 0, fmt.Errorf("failed to parse redirect response: %w", err)
			}

			if redirectResp.Address == "" {
				return 0, fmt.Errorf("redirect without valid address")
			}

			// Update URL
			url = fmt.Sprintf("%s/kv/%d", redirectResp.Address, key)
			continue
		}

		bodyBytes, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return 0, fmt.Errorf("unexpected status %d (failed to read body: %v)", resp.StatusCode, readErr)
		}
		return 0, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	return 0, fmt.Errorf("too many redirects (max: %d)", c.maxRedirects)
}

// Delete removes a key, automatically following redirects
func (c *Client) Delete(nodeAddr string, key int) error {
	url := fmt.Sprintf("%s/kv/%d", nodeAddr, key)

	for i := 0; i < c.maxRedirects; i++ {
		req, err := http.NewRequest(http.MethodDelete, url, nil)
		if err != nil {
			return fmt.Errorf("failed to create request: %w", err)
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("request failed: %w", err)
		}

		if resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			return nil
		}

		if resp.StatusCode == 307 {
			var redirectResp RedirectResponse
			bodyBytes, readErr := io.ReadAll(resp.Body)
			resp.Body.Close()

			if readErr != nil {
				return fmt.Errorf("failed to read redirect response: %w", readErr)
			}

			if err := json.Unmarshal(bodyBytes, &redirectResp); err != nil {
				return fmt.Errorf("failed to parse redirect response: %w", err)
			}

			if redirectResp.Address == "" {
				return fmt.Errorf("redirect without valid address")
			}

			url = fmt.Sprintf("%s/kv/%d", redirectResp.Address, key)
			continue
		}

		bodyBytes, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return fmt.Errorf("unexpected status %d (failed to read body: %v)", resp.StatusCode, readErr)
		}
		return fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	return fmt.Errorf("too many redirects (max: %d)", c.maxRedirects)
}

// Health checks if a node is healthy
func (c *Client) Health(nodeAddr string) error {
	url := fmt.Sprintf("%s/health", nodeAddr)
	resp, err := c.httpClient.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unhealthy: status %d", resp.StatusCode)
	}
	return nil
}

// GetStatus retrieves node status information
func (c *Client) GetStatus(nodeAddr string) (map[string]interface{}, error) {
	url := fmt.Sprintf("%s/status", nodeAddr)
	resp, err := c.httpClient.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status check failed: %d", resp.StatusCode)
	}

	var status map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return nil, fmt.Errorf("failed to parse status: %w", err)
	}

	return status, nil
}

// GetLeader finds the current leader node
func (c *Client) GetLeader(nodeAddr string) (string, error) {
	url := fmt.Sprintf("%s/leader", nodeAddr)
	resp, err := c.httpClient.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("leader check failed: %d", resp.StatusCode)
	}

	var leader map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&leader); err != nil {
		return "", fmt.Errorf("failed to parse leader info: %w", err)
	}

	if addr, ok := leader["address"].(string); ok {
		return addr, nil
	}

	return "", fmt.Errorf("no leader address in response")
}
