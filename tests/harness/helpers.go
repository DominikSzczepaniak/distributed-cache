package harness

import (
	"io"
	"net/http"
	"net/url"
	"time"
)

// RawGet reads a key straight from a datanode (bypassing the SmartClient and
// any routing) and returns the HTTP status and body.
func RawGet(baseURL, key string) (int, string, error) {
	hc := &http.Client{Timeout: 3 * time.Second}
	resp, err := hc.Get(baseURL + "/data?key=" + url.QueryEscape(key))
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b), nil
}

// DataNodeIndexFor returns the index of the datanode that is primary for key
// according to the client's current topology, or -1.
func (c *Cluster) DataNodeIndexFor(routeURL string) int {
	for i := range c.dns {
		if c.DataNodeURL(i) == routeURL {
			return i
		}
	}
	return -1
}
