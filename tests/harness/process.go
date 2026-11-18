package harness

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"
)

// logBuffer is a goroutine-safe, size-capped capture of a process's output.
type logBuffer struct {
	mu  sync.Mutex
	buf []byte
}

const maxLogBytes = 4 << 20

func (l *logBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.buf = append(l.buf, p...)
	if len(l.buf) > maxLogBytes {
		l.buf = append([]byte(nil), l.buf[len(l.buf)-maxLogBytes:]...)
	}
	return len(p), nil
}

func (l *logBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return string(l.buf)
}

// tail returns at most the last n lines.
func (l *logBuffer) tail(n int) string {
	b := []byte(l.String())
	idx := len(b)
	for ; n > 0 && idx > 0; n-- {
		i := bytes.LastIndexByte(b[:idx-1], '\n')
		if i < 0 {
			idx = 0
			break
		}
		idx = i + 1
	}
	return string(b[idx:])
}

// proc is one subprocess in its own process group, restartable with the same
// environment (and therefore the same ports and state files).
type proc struct {
	name string
	bin  string
	env  []string
	logs *logBuffer

	mu   sync.Mutex
	cmd  *exec.Cmd
	done chan struct{}
	pids []int // every pid ever started (for leak checks)
}

func newProc(name, bin string, env []string) *proc {
	return &proc{name: name, bin: bin, env: env, logs: &logBuffer{}}
}

func (p *proc) start() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cmd != nil {
		return fmt.Errorf("%s already running", p.name)
	}
	cmd := exec.Command(p.bin)
	cmd.Env = append(os.Environ(), p.env...)
	cmd.Stdout = p.logs
	cmd.Stderr = p.logs
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	fmt.Fprintf(p.logs, "=== harness: starting %s ===\n", p.name)
	if err := cmd.Start(); err != nil {
		return err
	}
	done := make(chan struct{})
	p.cmd, p.done = cmd, done
	p.pids = append(p.pids, cmd.Process.Pid)
	go func() {
		cmd.Wait()
		close(done)
	}()
	return nil
}

// running reports whether the process is started and has not exited.
func (p *proc) running() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cmd == nil {
		return false
	}
	select {
	case <-p.done:
		return false
	default:
		return true
	}
}

// exitedUnexpectedly reports a started process that terminated by itself.
func (p *proc) exitedUnexpectedly() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cmd == nil {
		return false
	}
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}

// kill SIGKILLs the whole process group (simulating a crash) and reaps it.
func (p *proc) kill() { p.signal(syscall.SIGKILL) }

func (p *proc) signal(sig syscall.Signal) {
	p.mu.Lock()
	cmd, done := p.cmd, p.done
	p.cmd, p.done = nil, nil
	p.mu.Unlock()
	if cmd == nil {
		return
	}
	// A negative pid targets the whole process group.
	_ = syscall.Kill(-cmd.Process.Pid, sig)
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		<-done
	}
	fmt.Fprintf(p.logs, "=== harness: %s stopped (%v) ===\n", p.name, sig)
}
