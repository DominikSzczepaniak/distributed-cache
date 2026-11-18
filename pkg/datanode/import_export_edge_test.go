package datanode

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/dominikszczepaniak/distributed-cache/pkg/metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func shardOf(key string, total int) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	s := int(h.Sum32()) % total
	if s < 0 {
		s = -s
	}
	return s
}

func export(t *testing.T, f *fixture, shard string) (*httptest.ResponseRecorder, map[string]string) {
	t.Helper()
	w := postJSON(t, f.srv.HandleExport, "GET", "/internal/export?shard="+shard, nil)
	var data map[string]string
	if w.Code == http.StatusOK {
		require.NoError(t, json.NewDecoder(w.Body).Decode(&data))
	}
	return w, data
}

func TestHandleExport_Validation(t *testing.T) {
	f := newFixture(t, metadata.NewClusterConfig(4))
	f.cache.Put("k", "v")

	w, _ := export(t, f, "")
	assert.Equal(t, http.StatusBadRequest, w.Code, "missing shard")
	w, _ = export(t, f, "abc")
	assert.Equal(t, http.StatusBadRequest, w.Code, "non numeric shard")

	for _, s := range []string{"99", "-1"} {
		w, data := export(t, f, s)
		assert.Equal(t, http.StatusOK, w.Code, s)
		assert.Empty(t, data, "out-of-range shard %s exports nothing (never panics)", s)
	}

	unconfigured := newFixture(t, nil)
	w, _ = export(t, unconfigured, "0")
	assert.Equal(t, http.StatusServiceUnavailable, w.Code, "TotalShards == 0 must not divide by zero")
}

func TestExportImport_RoundTripMovesExactlyThatShard(t *testing.T) {
	const total = 4
	src := newFixture(t, metadata.NewClusterConfig(total))
	want := map[int]map[string]string{}
	for i := 0; i < 200; i++ {
		k := fmt.Sprintf("key-%d", i)
		src.cache.Put(k, "v"+k)
		s := shardOf(k, total)
		if want[s] == nil {
			want[s] = map[string]string{}
		}
		want[s][k] = "v" + k
	}

	dst := newFixture(t, metadata.NewClusterConfig(total))
	for s := 0; s < total; s++ {
		w, data := export(t, src, fmt.Sprint(s))
		require.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, want[s], data, "shard %d export must contain exactly its keys", s)
	}

	_, data := export(t, src, "2")
	require.Equal(t, http.StatusOK, postJSON(t, dst.srv.HandleImport, "POST", "/internal/import", data).Code)
	for k, v := range want[2] {
		assert.Equal(t, v, dst.cache.Get(k))
	}
	for s, keys := range want {
		if s == 2 {
			continue
		}
		for k := range keys {
			assert.Empty(t, dst.cache.Get(k), "key %s of shard %d leaked into import of shard 2", k, s)
		}
	}
}

func TestHandleImport_Edge(t *testing.T) {
	f := newFixture(t, metadata.NewClusterConfig(4))

	assert.Equal(t, http.StatusBadRequest, postJSON(t, f.srv.HandleImport, "POST", "/i", "{nope").Code)
	assert.Equal(t, http.StatusBadRequest, postJSON(t, f.srv.HandleImport, "POST", "/i", `["a"]`).Code)
	assert.Equal(t, http.StatusOK, postJSON(t, f.srv.HandleImport, "POST", "/i", `{}`).Code, "empty import is a no-op")

	// Conflict: imported data overwrites existing local values (source is authoritative).
	f.cache.Put("k", "local")
	f.cache.Put("untouched", "stay")
	postJSON(t, f.srv.HandleImport, "POST", "/i", map[string]string{"k": "imported"})
	assert.Equal(t, "imported", f.cache.Get("k"))
	assert.Equal(t, "stay", f.cache.Get("untouched"), "import never deletes local-only keys")

	// Characterisation: import does not validate shard ownership of keys.
	wrong := "wrong-shard-key"
	postJSON(t, f.srv.HandleImport, "POST", "/i", map[string]string{wrong: "x"})
	assert.Equal(t, "x", f.cache.Get(wrong))
}

func TestHandlePull(t *testing.T) {
	newSource := func(t *testing.T, h http.HandlerFunc) string {
		s := httptest.NewServer(h)
		t.Cleanup(s.Close)
		return "http://" + s.Listener.Addr().String()
	}
	pull := func(f *fixture, src, shard string) *httptest.ResponseRecorder {
		q := url.Values{}
		if src != "" {
			q.Set("source", src)
		}
		if shard != "" {
			q.Set("shard", shard)
		}
		return postJSON(t, f.srv.HandlePull, "POST", "/internal/pull?"+q.Encode(), nil)
	}

	t.Run("missing params", func(t *testing.T) {
		f := newFixture(t, metadata.NewClusterConfig(2))
		assert.Equal(t, http.StatusBadRequest, pull(f, "", "0").Code)
		assert.Equal(t, http.StatusBadRequest, pull(f, "http://x", "").Code)
	})
	t.Run("success imports and overwrites conflicts but keeps local extras", func(t *testing.T) {
		src := newSource(t, func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, "/internal/export", r.URL.Path)
			assert.Equal(t, "3", r.URL.Query().Get("shard"))
			json.NewEncoder(w).Encode(map[string]string{"a": "src-a", "b": "src-b"})
		})
		f := newFixture(t, metadata.NewClusterConfig(2))
		f.cache.Put("a", "stale-local")
		f.cache.Put("extra", "keep")
		w := pull(f, src, "3")
		require.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, "src-a", f.cache.Get("a"))
		assert.Equal(t, "src-b", f.cache.Get("b"))
		assert.Equal(t, "keep", f.cache.Get("extra"))
	})
	t.Run("source returns error", func(t *testing.T) {
		src := newSource(t, func(w http.ResponseWriter, r *http.Request) { http.Error(w, "boom", 500) })
		f := newFixture(t, metadata.NewClusterConfig(2))
		f.cache.Put("a", "local")
		assert.Equal(t, http.StatusBadGateway, pull(f, src, "0").Code)
		assert.Equal(t, "local", f.cache.Get("a"), "failed pull changes nothing")
	})
	t.Run("source unreachable", func(t *testing.T) {
		s := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
		u := "http://" + s.Listener.Addr().String()
		s.Close()
		f := newFixture(t, metadata.NewClusterConfig(2))
		assert.Equal(t, http.StatusBadGateway, pull(f, u, "0").Code)
	})
	t.Run("source returns garbage", func(t *testing.T) {
		src := newSource(t, func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("<html>")) })
		f := newFixture(t, metadata.NewClusterConfig(2))
		f.cache.Put("a", "local")
		assert.Equal(t, http.StatusInternalServerError, pull(f, src, "0").Code)
		assert.Equal(t, "local", f.cache.Get("a"))
	})
}

// A real source datanode -> real target datanode pull.
func TestHandlePull_FromRealSourceDatanode(t *testing.T) {
	const total = 3
	srcF := newFixture(t, metadata.NewClusterConfig(total))
	mux := http.NewServeMux()
	mux.HandleFunc("/internal/export", srcF.srv.HandleExport)
	addr := newServerFromMux(t, mux)

	for i := 0; i < 60; i++ {
		srcF.cache.Put(fmt.Sprintf("k%d", i), "v")
	}
	dst := newFixture(t, metadata.NewClusterConfig(total))

	w := postJSON(t, dst.srv.HandlePull, "POST", "/internal/pull?source=http://"+addr+"&shard=1", nil)
	require.Equal(t, http.StatusOK, w.Code)

	for i := 0; i < 60; i++ {
		k := fmt.Sprintf("k%d", i)
		if shardOf(k, total) == 1 {
			assert.Equal(t, "v", dst.cache.Get(k))
		} else {
			assert.Empty(t, dst.cache.Get(k))
		}
	}
}
