// Package harness starts real controller/datanode subprocesses on free ports so
// that end-to-end, fault-injection and benchmark tests can run as ordinary Go
// tests (no docker, no root).
//
// Binaries are built once per test process by RunMain (call it from TestMain).
// Under `go test -short` nothing is built and every heavy test must call
// RequireHeavy, which skips it.
package harness

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

var (
	binDir        string // temp dir holding the built binaries
	controllerBin string
	datanodeBin   string
)

// RunMain is meant to be the body of TestMain: it builds the controller and
// datanode binaries (skipped under -short), runs the tests and removes the
// binaries afterwards.
func RunMain(m *testing.M) {
	flag.Parse()
	if testing.Short() {
		os.Exit(m.Run())
	}

	dir, err := os.MkdirTemp("", "dc-harness-bin-")
	if err != nil {
		fmt.Fprintf(os.Stderr, "harness: mkdtemp: %v\n", err)
		os.Exit(1)
	}
	binDir = dir
	if err := buildBinaries(); err != nil {
		fmt.Fprintf(os.Stderr, "harness: %v\n", err)
		os.RemoveAll(dir)
		os.Exit(1)
	}

	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

// RequireHeavy skips the test under -short.
func RequireHeavy(t testing.TB) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping cluster test in -short mode")
	}
	if controllerBin == "" {
		t.Fatal("harness: binaries not built; call harness.RunMain from TestMain")
	}
}

func buildBinaries() error {
	root, err := moduleRoot()
	if err != nil {
		return err
	}
	controllerBin = filepath.Join(binDir, "controller")
	datanodeBin = filepath.Join(binDir, "datanode")
	for _, b := range []struct{ out, pkg string }{
		{controllerBin, "./cmd/controller"},
		{datanodeBin, "./cmd/datanode"},
	} {
		args := []string{"build", "-o", b.out}
		if raceEnabled {
			args = append(args, "-race")
		}
		args = append(args, b.pkg)
		cmd := exec.Command("go", args...)
		cmd.Dir = root
		if out, err := cmd.CombinedOutput(); err != nil {
			return fmt.Errorf("go build %s: %v\n%s", b.pkg, err, out)
		}
	}
	return nil
}

func moduleRoot() (string, error) {
	out, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}").Output()
	if err != nil {
		return "", fmt.Errorf("go list -m: %w", err)
	}
	return strings.TrimSpace(string(out)), nil
}
