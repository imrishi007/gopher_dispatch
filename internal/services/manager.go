package services

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"sync"
	"time"
)

type Spec struct {
	Name    string   `json:"name"`
	Command string   `json:"command"`
	Args    []string `json:"args"`
	Port    int      `json:"port"`
}

type Status struct {
	Name      string    `json:"name"`
	Port      int       `json:"port"`
	PID       int       `json:"pid,omitempty"`
	Running   bool      `json:"running"`
	Healthy   bool      `json:"healthy"`
	StartedAt time.Time `json:"startedAt,omitempty"`
	Error     string    `json:"error,omitempty"`
}

type Event struct {
	Type    string    `json:"type"`
	Service string    `json:"service"`
	At      time.Time `json:"at"`
	Detail  string    `json:"detail,omitempty"`
}

type process struct {
	spec  Spec
	cmd   *exec.Cmd
	start time.Time
	err   string
}

type Manager struct {
	mu        sync.RWMutex
	specs     map[string]Spec
	processes map[string]*process
	subs      map[chan Event]struct{}
}

func Load(path string) ([]Spec, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var specs []Spec
	if err := json.Unmarshal(b, &specs); err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	for _, s := range specs {
		if s.Name == "" || s.Command == "" || s.Port < 1 || s.Port > 65535 || seen[s.Name] {
			return nil, fmt.Errorf("invalid service %q", s.Name)
		}
		seen[s.Name] = true
	}
	return specs, nil
}

func New(specs []Spec) *Manager {
	m := &Manager{specs: map[string]Spec{}, processes: map[string]*process{}, subs: map[chan Event]struct{}{}}
	for _, s := range specs {
		m.specs[s.Name] = s
	}
	return m
}

func (m *Manager) emit(e Event) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for ch := range m.subs {
		select {
		case ch <- e:
		default:
		}
	}
}

func (m *Manager) Subscribe() <-chan Event {
	ch := make(chan Event, 32)
	m.mu.Lock()
	m.subs[ch] = struct{}{}
	m.mu.Unlock()
	return ch
}

func (m *Manager) Start(name string) error {
	m.mu.Lock()
	spec, ok := m.specs[name]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("unknown service %q", name)
	}
	if p := m.processes[name]; p != nil && p.cmd.Process != nil {
		m.mu.Unlock()
		return nil
	}
	cmd := exec.Command(spec.Command, spec.Args...)
	if err := cmd.Start(); err != nil {
		m.mu.Unlock()
		return err
	}
	p := &process{spec: spec, cmd: cmd, start: time.Now()}
	m.processes[name] = p
	m.mu.Unlock()
	m.emit(Event{Type: "started", Service: name, At: p.start, Detail: fmt.Sprintf("pid=%d", cmd.Process.Pid)})
	go func() {
		err := cmd.Wait()
		m.mu.Lock()
		if m.processes[name] == p {
			p.err = errorText(err)
			delete(m.processes, name)
		}
		m.mu.Unlock()
		m.emit(Event{Type: "stopped", Service: name, At: time.Now(), Detail: errorText(err)})
	}()
	return nil
}

func (m *Manager) Stop(name string) error {
	m.mu.RLock()
	p := m.processes[name]
	_, known := m.specs[name]
	m.mu.RUnlock()
	if !known {
		return fmt.Errorf("unknown service %q", name)
	}
	if p == nil || p.cmd.Process == nil {
		return nil
	}
	if runtime.GOOS == "windows" {
		return exec.Command("taskkill", "/PID", strconv.Itoa(p.cmd.Process.Pid), "/T", "/F").Run()
	}
	if err := p.cmd.Process.Signal(os.Interrupt); err != nil {
		_ = p.cmd.Process.Kill()
		return nil
	}
	time.AfterFunc(2*time.Second, func() { _ = p.cmd.Process.Kill() })
	return nil
}

func (m *Manager) StartAll() []error {
	m.mu.RLock()
	names := make([]string, 0, len(m.specs))
	for name := range m.specs {
		names = append(names, name)
	}
	m.mu.RUnlock()
	var errs []error
	for _, name := range names {
		if err := m.Start(name); err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}

func (m *Manager) Statuses() []Status {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]Status, 0, len(m.specs))
	for name, spec := range m.specs {
		s := Status{Name: name, Port: spec.Port, Healthy: reachable(spec.Port)}
		if p := m.processes[name]; p != nil && p.cmd.Process != nil {
			s.Running, s.PID, s.StartedAt, s.Error = true, p.cmd.Process.Pid, p.start, p.err
		}
		result = append(result, s)
	}
	return result
}

func (m *Manager) Close() {
	m.mu.RLock()
	names := make([]string, 0, len(m.processes))
	for n := range m.processes {
		names = append(names, n)
	}
	m.mu.RUnlock()
	for _, n := range names {
		_ = m.Stop(n)
	}
}

func reachable(port int) bool {
	c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 250*time.Millisecond)
	if err == nil {
		_ = c.Close()
		return true
	}
	return false
}
func errorText(err error) string {
	if err == nil || errors.Is(err, os.ErrProcessDone) {
		return ""
	}
	return err.Error()
}
