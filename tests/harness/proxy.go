package harness

import (
	"math/rand"
	"net"
	"sync"
	"time"
)

// Proxy is a userspace TCP proxy for one directed Raft edge (from -> to). It
// lets tests inject faults without root: Cut, Delay, Drop and Blackhole.
//
//   - Cut:       existing connections are closed, new ones are accepted and
//     closed immediately (peer sees a dead link).
//   - Blackhole: bytes are read and silently discarded (peer sees a hang).
//   - Delay:     every forwarded chunk is held back for the given duration.
//   - Drop:      each forwarded chunk kills the connection with the given
//     probability (lost message => broken stream => reconnect).
type Proxy struct {
	name   string
	ln     net.Listener
	target string

	mu        sync.Mutex
	cut       bool
	blackhole bool
	delay     time.Duration
	drop      float64
	closed    bool
	conns     map[net.Conn]struct{}
	wg        sync.WaitGroup
}

func newProxy(name, target string) (*Proxy, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	p := &Proxy{name: name, ln: ln, target: target, conns: make(map[net.Conn]struct{})}
	p.wg.Add(1)
	go p.acceptLoop()
	return p, nil
}

// Addr is the address peers must dial instead of the real target.
func (p *Proxy) Addr() string { return p.ln.Addr().String() }

func (p *Proxy) acceptLoop() {
	defer p.wg.Done()
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		p.wg.Add(1)
		go p.handle(c)
	}
}

func (p *Proxy) track(c net.Conn) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || p.cut {
		return false
	}
	p.conns[c] = struct{}{}
	return true
}

func (p *Proxy) closeConn(c net.Conn) {
	p.mu.Lock()
	delete(p.conns, c)
	p.mu.Unlock()
	c.Close()
}

func (p *Proxy) handle(client net.Conn) {
	defer p.wg.Done()
	if !p.track(client) {
		client.Close()
		return
	}
	server, err := net.DialTimeout("tcp", p.target, time.Second)
	if err != nil {
		p.closeConn(client)
		return
	}
	if !p.track(server) {
		p.closeConn(client)
		server.Close()
		return
	}

	done := make(chan struct{}, 2)
	go func() { p.pump(client, server); done <- struct{}{} }()
	go func() { p.pump(server, client); done <- struct{}{} }()
	<-done
	p.closeConn(client)
	p.closeConn(server)
	<-done
}

func (p *Proxy) pump(src, dst net.Conn) {
	buf := make([]byte, 32*1024)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			p.mu.Lock()
			delay, blackhole, drop := p.delay, p.blackhole, p.drop
			p.mu.Unlock()
			if drop > 0 && rand.Float64() < drop {
				return
			}
			if delay > 0 {
				time.Sleep(delay)
			}
			if !blackhole {
				if _, werr := dst.Write(buf[:n]); werr != nil {
					return
				}
			}
		}
		if err != nil {
			return
		}
	}
}

// SetCut cuts (true) or restores (false) the link. Cutting kills live
// connections.
func (p *Proxy) SetCut(cut bool) {
	p.mu.Lock()
	p.cut = cut
	var victims []net.Conn
	if cut {
		for c := range p.conns {
			victims = append(victims, c)
		}
	}
	p.mu.Unlock()
	for _, c := range victims {
		c.Close()
	}
}

func (p *Proxy) SetDelay(d time.Duration) { p.mu.Lock(); p.delay = d; p.mu.Unlock() }
func (p *Proxy) SetDrop(rate float64)     { p.mu.Lock(); p.drop = rate; p.mu.Unlock() }
func (p *Proxy) SetBlackhole(b bool)      { p.mu.Lock(); p.blackhole = b; p.mu.Unlock() }

// Heal removes every fault from this edge.
func (p *Proxy) Heal() {
	p.mu.Lock()
	p.cut, p.blackhole, p.delay, p.drop = false, false, 0, 0
	p.mu.Unlock()
}

func (p *Proxy) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	var victims []net.Conn
	for c := range p.conns {
		victims = append(victims, c)
	}
	p.mu.Unlock()
	p.ln.Close()
	for _, c := range victims {
		c.Close()
	}
	p.wg.Wait()
}
