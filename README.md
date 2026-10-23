# Gopher Dispatch

Gopher Dispatch is a local control plane for a delivery-service development environment. It starts and stops services, reports their health, identifies the process bound to a TCP port, and exposes operational state through an HTTP API and command-line client.

## Features

- Concurrent management of configured local services
- TCP health checks with PID and startup-time reporting
- Native Windows port-to-process discovery using `GetExtendedTcpTable`
- HTTP/1.1 control API built with Shadowfax
- Live lifecycle event stream using newline-delimited JSON
- Minimal CLI for service and port operations

## Requirements

- Go 1.26 or later
- Windows for native process-path lookup. Linux supports port inspection through `ss`.

## Quick start

Start the control plane:

```powershell
go run ./cmd/control-plane
```

In another terminal, start the sample order, payments, and delivery services:

```powershell
go run ./cmd/dispatch up
go run ./cmd/dispatch status
```

Inspect a port or stop a service:

```powershell
go run ./cmd/dispatch port 8081
go run ./cmd/dispatch stop orders
```

## API

| Method | Path | Purpose |
| --- | --- | --- |
| GET | `/health` | Control-plane health check |
| GET | `/services` | Service health and process state |
| GET | `/metrics` | Uptime, request count, and service totals |
| POST | `/services/start` | Start all configured services |
| POST | `/services/:name/start` | Start one service |
| POST | `/services/:name/stop` | Stop one service |
| GET | `/ports/:port` | Find the process holding a TCP port |
| GET | `/events` | Stream lifecycle events as NDJSON |

## Configuration

Service definitions live in `configs/services.json`. Each entry specifies a service name, command, arguments, and health-check port. The included configuration starts three small mock delivery services.

## Architecture

`cmd/control-plane` hosts the HTTP API. `internal/services` owns process lifecycle and health checks. `internal/ports` resolves TCP port ownership. `cmd/dispatch` provides a small client for the control API.

## Attribution

The HTTP layer depends on [Shadowfax](https://github.com/shravanasati/shadowfax). The Windows port-table implementation is adapted from [rekt](https://github.com/shravanasati/rekt) by Shravan Asati under the MIT License; see `LICENSES/REKT-MIT.txt`.
