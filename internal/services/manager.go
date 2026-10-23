package services

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"sync"
	"time"
)

type State string

const (
	Stopped   State = "stopped"
	Starting  State = "starting"
	Running   State = "running"
	Unhealthy State = "unhealthy"
	Stopping  State = "stopping"
	Failed    State = "failed"
)

type RestartPolicy struct {
	OnFailure        bool `json:"onFailure"`
	MaxAttempts      int  `json:"maxAttempts"`
	InitialBackoffMS int  `json:"initialBackoffMs"`
}
type Spec struct {
	Name             string        `json:"name"`
	Command          string        `json:"command"`
	Args             []string      `json:"args"`
	Port             int           `json:"port"`
	Dependencies     []string      `json:"dependencies,omitempty"`
	StartupTimeoutMS int           `json:"startupTimeoutMs,omitempty"`
	Restart          RestartPolicy `json:"restart"`
}
type Status struct {
	Name         string    `json:"name"`
	Port         int       `json:"port"`
	PID          int       `json:"pid,omitempty"`
	State        State     `json:"state"`
	Running      bool      `json:"running"`
	Healthy      bool      `json:"healthy"`
	StartedAt    time.Time `json:"startedAt,omitempty"`
	RestartCount int       `json:"restartCount"`
	Error        string    `json:"error,omitempty"`
}
type Event struct {
	Type    string    `json:"type"`
	Service string    `json:"service"`
	At      time.Time `json:"at"`
	Detail  string    `json:"detail,omitempty"`
}
type serviceRuntime struct {
	cmd      *exec.Cmd
	state    State
	started  time.Time
	err      string
	restarts int
	logs     []string
}
type Manager struct {
	mu        sync.RWMutex
	specs     map[string]Spec
	runtime   map[string]*serviceRuntime
	subs      map[chan Event]struct{}
	done      chan struct{}
	closeOnce sync.Once
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
	return specs, Validate(specs)
}
func specByName(specs []Spec) map[string]Spec {
	result := make(map[string]Spec, len(specs))
	for _, s := range specs {
		result[s.Name] = s
	}
	return result
}
func Validate(specs []Spec) error {
	known := specByName(specs)
	if len(known) != len(specs) {
		return fmt.Errorf("service names must be unique")
	}
	for _, s := range specs {
		if s.Name == "" || s.Command == "" || s.Port < 1 || s.Port > 65535 {
			return fmt.Errorf("invalid service %q", s.Name)
		}
		for _, dep := range s.Dependencies {
			if dep == s.Name || known[dep].Name == "" {
				return fmt.Errorf("service %q has invalid dependency %q", s.Name, dep)
			}
		}
	}
	visiting, visited := map[string]bool{}, map[string]bool{}
	var walk func(string) error
	walk = func(name string) error {
		if visiting[name] {
			return fmt.Errorf("dependency cycle includes %q", name)
		}
		if visited[name] {
			return nil
		}
		visiting[name] = true
		for _, dep := range known[name].Dependencies {
			if err := walk(dep); err != nil {
				return err
			}
		}
		delete(visiting, name)
		visited[name] = true
		return nil
	}
	for _, s := range specs {
		if err := walk(s.Name); err != nil {
			return err
		}
	}
	return nil
}
func New(specs []Spec) *Manager {
	m := &Manager{specs: specByName(specs), runtime: map[string]*serviceRuntime{}, subs: map[chan Event]struct{}{}, done: make(chan struct{})}
	for _, s := range specs {
		m.runtime[s.Name] = &serviceRuntime{state: Stopped}
	}
	go m.monitor()
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

func (m *Manager) Start(name string) error { return m.start(name, map[string]bool{}) }
func (m *Manager) start(name string, chain map[string]bool) error {
	if chain[name] {
		return fmt.Errorf("dependency cycle includes %q", name)
	}
	m.mu.RLock()
	spec, known := m.specs[name]
	state := Stopped
	if r := m.runtime[name]; r != nil {
		state = r.state
	}
	m.mu.RUnlock()
	if !known {
		return fmt.Errorf("unknown service %q", name)
	}
	if state == Running {
		return nil
	}
	if state == Starting {
		return m.awaitHealthy(name, spec)
	}
	if state == Stopping {
		return fmt.Errorf("service %q is stopping", name)
	}
	chain[name] = true
	for _, dep := range spec.Dependencies {
		if err := m.start(dep, chain); err != nil {
			return fmt.Errorf("start %q: %w", name, err)
		}
		if err := m.awaitHealthy(dep, m.specs[dep]); err != nil {
			return fmt.Errorf("start %q: dependency %q: %w", name, dep, err)
		}
	}
	delete(chain, name)
	return m.launch(name)
}
func (m *Manager) launch(name string) error {
	m.mu.Lock()
	spec := m.specs[name]
	r := m.runtime[name]
	if r.state == Running || r.state == Starting {
		m.mu.Unlock()
		return nil
	}
	if r.state == Stopping {
		m.mu.Unlock()
		return fmt.Errorf("service %q is stopping", name)
	}
	r.state, r.err = Starting, ""
	m.mu.Unlock()
	cmd := exec.Command(spec.Command, spec.Args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		m.fail(name, err)
		return err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		m.fail(name, err)
		return err
	}
	if err := cmd.Start(); err != nil {
		m.fail(name, err)
		return err
	}
	m.mu.Lock()
	r = m.runtime[name]
	r.cmd, r.started, r.state = cmd, time.Now(), Starting
	m.mu.Unlock()
	m.emit(Event{Type: "starting", Service: name, At: time.Now(), Detail: fmt.Sprintf("pid=%d", cmd.Process.Pid)})
	go m.capture(name, "stdout", stdout)
	go m.capture(name, "stderr", stderr)
	go m.wait(name, cmd)
	return m.awaitHealthy(name, spec)
}
func (m *Manager) wait(name string, cmd *exec.Cmd) {
	err := cmd.Wait()
	detail := errorText(err)
	restart := false
	var delay time.Duration
	m.mu.Lock()
	r := m.runtime[name]
	if r.cmd == cmd {
		r.cmd = nil
		if r.state == Stopping {
			r.state, r.err = Stopped, ""
		} else {
			r.state, r.err = Failed, detail
			policy := m.specs[name].Restart
			if policy.OnFailure && r.restarts < policy.MaxAttempts {
				r.restarts++
				restart, delay = true, backoff(policy, r.restarts)
			}
		}
	}
	state := r.state
	m.mu.Unlock()
	m.emit(Event{Type: string(state), Service: name, At: time.Now(), Detail: detail})
	if restart {
		m.emit(Event{Type: "restart_scheduled", Service: name, At: time.Now(), Detail: delay.String()})
		time.AfterFunc(delay, func() { _ = m.Start(name) })
	}
}
func (m *Manager) fail(name string, err error) {
	m.mu.Lock()
	r := m.runtime[name]
	r.state, r.err = Failed, err.Error()
	m.mu.Unlock()
	m.emit(Event{Type: string(Failed), Service: name, At: time.Now(), Detail: err.Error()})
}
func backoff(policy RestartPolicy, attempt int) time.Duration {
	base := policy.InitialBackoffMS
	if base <= 0 {
		base = 500
	}
	power := attempt - 1
	if power > 6 {
		power = 6
	}
	return time.Duration(base*(1<<power)) * time.Millisecond
}
func (m *Manager) awaitHealthy(name string, spec Spec) error {
	timeout := spec.StartupTimeoutMS
	if timeout <= 0 {
		timeout = 10000
	}
	deadline := time.NewTimer(time.Duration(timeout) * time.Millisecond)
	defer deadline.Stop()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		m.mu.RLock()
		state, failure := m.runtime[name].state, m.runtime[name].err
		m.mu.RUnlock()
		if state == Running {
			return nil
		}
		if state == Failed {
			return fmt.Errorf("%s failed: %s", name, failure)
		}
		select {
		case <-deadline.C:
			return fmt.Errorf("%s did not become healthy within %dms", name, timeout)
		case <-tick.C:
		}
	}
}

func (m *Manager) Stop(name string) error { return m.stop(name, map[string]bool{}) }
func (m *Manager) stop(name string, seen map[string]bool) error {
	if seen[name] {
		return nil
	}
	seen[name] = true
	m.mu.RLock()
	_, known := m.specs[name]
	dependents := []string{}
	for other, spec := range m.specs {
		for _, dep := range spec.Dependencies {
			if dep == name {
				dependents = append(dependents, other)
			}
		}
	}
	m.mu.RUnlock()
	if !known {
		return fmt.Errorf("unknown service %q", name)
	}
	for _, dependent := range dependents {
		if err := m.stop(dependent, seen); err != nil {
			return err
		}
	}
	m.mu.Lock()
	r := m.runtime[name]
	if r.cmd == nil {
		r.state = Stopped
		m.mu.Unlock()
		return nil
	}
	r.state = Stopping
	cmd := r.cmd
	m.mu.Unlock()
	m.emit(Event{Type: string(Stopping), Service: name, At: time.Now()})
	if runtime.GOOS == "windows" {
		return exec.Command("taskkill", "/PID", strconv.Itoa(cmd.Process.Pid), "/T", "/F").Run()
	}
	if err := cmd.Process.Signal(os.Interrupt); err != nil {
		return cmd.Process.Kill()
	}
	time.AfterFunc(2*time.Second, func() { _ = cmd.Process.Kill() })
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
		r := m.runtime[name]
		s := Status{Name: name, Port: spec.Port, State: r.state, RestartCount: r.restarts, Error: r.err, StartedAt: r.started, Running: r.cmd != nil, Healthy: r.state == Running}
		if r.cmd != nil {
			s.PID = r.cmd.Process.Pid
		}
		result = append(result, s)
	}
	return result
}
func (m *Manager) Logs(name string) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	r, ok := m.runtime[name]
	if !ok {
		return nil, fmt.Errorf("unknown service %q", name)
	}
	return append([]string(nil), r.logs...), nil
}
func (m *Manager) capture(name, stream string, reader io.Reader) {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 4096), 64*1024)
	for scanner.Scan() {
		line := stream + ": " + scanner.Text()
		m.mu.Lock()
		r := m.runtime[name]
		r.logs = append(r.logs, line)
		if len(r.logs) > 200 {
			r.logs = r.logs[len(r.logs)-200:]
		}
		m.mu.Unlock()
		m.emit(Event{Type: "log", Service: name, At: time.Now(), Detail: line})
	}
}
func (m *Manager) monitor() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-m.done:
			return
		case <-ticker.C:
			m.checkHealth()
		}
	}
}
func (m *Manager) checkHealth() {
	m.mu.RLock()
	candidates := map[string]int{}
	for name, r := range m.runtime {
		if r.cmd != nil && (r.state == Starting || r.state == Running || r.state == Unhealthy) {
			candidates[name] = m.specs[name].Port
		}
	}
	m.mu.RUnlock()
	for name, port := range candidates {
		healthy := reachable(port)
		m.mu.Lock()
		r := m.runtime[name]
		previous := r.state
		if healthy {
			r.state = Running
		} else if r.state != Starting {
			r.state = Unhealthy
		}
		current := r.state
		m.mu.Unlock()
		if previous != current {
			m.emit(Event{Type: string(current), Service: name, At: time.Now()})
		}
	}
}
func (m *Manager) Close() {
	m.closeOnce.Do(func() {
		close(m.done)
		m.mu.RLock()
		names := make([]string, 0, len(m.runtime))
		for name := range m.runtime {
			names = append(names, name)
		}
		m.mu.RUnlock()
		for _, name := range names {
			_ = m.Stop(name)
		}
	})
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
	if err == nil {
		return ""
	}
	return err.Error()
}
