package main

import (
	"encoding/json"
	"flag"
	"io"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"dispatch-deck/internal/ports"
	"dispatch-deck/internal/services"
	"github.com/shravanasati/shadowfax/request"
	"github.com/shravanasati/shadowfax/response"
	"github.com/shravanasati/shadowfax/router"
	"github.com/shravanasati/shadowfax/server"
)

func main() {
	addr := flag.String("addr", ":8080", "control-plane address")
	config := flag.String("config", "configs/services.json", "service config")
	flag.Parse()
	specs, err := services.Load(*config)
	if err != nil {
		log.Fatal(err)
	}
	m := services.New(specs)
	metrics := newMetrics()
	defer m.Close()
	r := router.NewRouter(nil)
	r.Use(logRequest(metrics))
	r.Get("/health", func(*request.Request) response.Response { return jsonResponse(map[string]string{"status": "ok"}) })
	r.Get("/services", func(*request.Request) response.Response { return jsonResponse(m.Statuses()) })
	r.Get("/metrics", func(*request.Request) response.Response { return jsonResponse(metrics.snapshot(m.Statuses())) })
	r.Post("/services/start", func(*request.Request) response.Response { return result(servicesErrors(m.StartAll())) })
	r.Post("/services/:name/start", func(req *request.Request) response.Response { return result(m.Start(req.PathParams["name"])) })
	r.Post("/services/:name/stop", func(req *request.Request) response.Response { return result(m.Stop(req.PathParams["name"])) })
	r.Get("/ports/:port", func(req *request.Request) response.Response {
		port, err := strconv.Atoi(req.PathParams["port"])
		if err != nil {
			return bad(err)
		}
		owners, err := ports.Find(port)
		if err != nil {
			return bad(err)
		}
		return jsonResponse(owners)
	})
	r.Get("/events", func(*request.Request) response.Response {
		events := m.Subscribe()
		return response.NewStreamResponse(func(w io.Writer, _ response.TrailerSetter) error {
			encoder := json.NewEncoder(w)
			for {
				if err := encoder.Encode(<-events); err != nil {
					return err
				}
			}
		}, nil).WithHeader("content-type", "application/x-ndjson")
	})
	srv, err := server.Serve(server.ServerOpts{Address: *addr, ReadTimeout: 10 * time.Second, WriteTimeout: 0, KeepAliveTimeout: 30 * time.Second}, r.Handler())
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Dispatch Deck listening on %s", *addr)
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	_ = srv.Close()
}

func jsonResponse(v any) response.Response {
	r, err := response.NewJSONResponse(v)
	if err != nil {
		return response.NewTextResponse("serialization error").WithStatusCode(500)
	}
	return r
}
func bad(err error) response.Response {
	return response.NewTextResponse(err.Error()).WithStatusCode(400)
}
func result(err error) response.Response {
	if err != nil {
		return bad(err)
	}
	return jsonResponse(map[string]string{"status": "ok"})
}
func servicesErrors(errs []error) error {
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

type metricState struct {
	started  time.Time
	requests atomic.Uint64
}

func newMetrics() *metricState { return &metricState{started: time.Now()} }
func (m *metricState) snapshot(statuses []services.Status) map[string]any {
	healthy, running := 0, 0
	for _, s := range statuses {
		if s.Healthy {
			healthy++
		}
		if s.Running {
			running++
		}
	}
	return map[string]any{"uptimeSeconds": int(time.Since(m.started).Seconds()), "requests": m.requests.Load(), "services": len(statuses), "runningServices": running, "healthyServices": healthy}
}
func logRequest(metrics *metricState) func(server.Handler) server.Handler {
	return func(next server.Handler) server.Handler {
		return func(req *request.Request) response.Response {
			metrics.requests.Add(1)
			start := time.Now()
			resp := next(req)
			log.Printf("%s %s %d %s", req.Method, req.Target, resp.GetStatusCode(), time.Since(start).Round(time.Millisecond))
			return resp
		}
	}
}
