**HTND Proxy** – A lightweight, high-performance gRPC load-balancing proxy for **Hoosat Network** (HTND) nodes.

![Go](https://img.shields.io/badge/Go-1.26-blue) ![Docker](https://img.shields.io/badge/Docker-ready-blue) [![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)

### What is htnd-proxy?

`htnd-proxy` is a smart RPC proxy that sits in front of multiple HTND (Hoosat Network Daemon) nodes. It automatically discovers healthy, fully-synced nodes from the network, performs health checks, and intelligently routes gRPC RPC requests (MessageStream) to them in a round-robin fashion.

It is ideal for:
- Public or private RPC endpoints
- Load balancing across multiple nodes
- Providing reliable API access even when individual nodes go offline
- Running behind services like explorers, wallets, or custom applications

---

### Features

- **Automatic peer discovery** – pulls live peers from `https://hoosatworld.net/peers`
- **Health checking** – probes nodes for sync status + UTXO index before using them
- **Round-robin load balancing** with fallback
- **Local node priority** – always tries your local HTND first
- **Manual peer support** (CLI flag + env vars)
- **gRPC passthrough** – full compatibility with HTND's RPC protocol
- **Lightweight & efficient** – written in Go, minimal resource usage
- **Docker ready** – multi-stage image, runs as non-root

---

### Quick Start

#### Using Docker (recommended)

```bash
docker run -d \
  --name htnd-proxy \
  -p 42420:42420 \
  -e LOCAL_PEER_HOST=127.0.0.1 \
  -e LOCAL_PEER_PORT=42520 \
  ghcr.io/hoosatnetwork/htnd-proxy:latest
```

Or build locally:

```bash
docker build -t htnd-proxy .
docker run -d -p 42420:42420 htnd-proxy
```

#### From Source

```bash
git clone https://github.com/HoosatNetwork/htnd-proxy.git
cd htnd-proxy
go build -o htnd-proxy .
./htnd-proxy -listen=:42420
```

---

### Configuration

#### Command-line flags

| Flag              | Default          | Description |
|-------------------|------------------|-----------|
| `-listen`         | `:42420`         | Address to listen on |
| `-refresh`        | `30`             | Peer refresh interval in seconds |
| `-manual-peers`   | `""`             | Comma-separated list of additional RPC peers |

#### Environment Variables

| Variable             | Description |
|----------------------|-----------|
| `LISTEN` / `listen`  | Listen address |
| `LOCAL_PEER_HOST`    | Local HTND host (default: `127.0.0.1`) |
| `LOCAL_PEER_PORT`    | Local HTND RPC port (default: `42520`) |
| `MANUAL_RPC_PEERS`   | Comma-separated manual RPC peers |

---

### Example Usage

After running the proxy, point your applications/explorer to:

```
grpc://your-server:42420
```

Or use any HTND-compatible client library.

---

### Project Structure

```
.
├── main.go          # Main proxy logic + gRPC server
├── Dockerfile
├── go.mod
└── proxy/           # (internal packages if extended)
```

---

### Building for Production

```bash
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o htnd-proxy .
```

---

### Related Projects

- **HTND** – [HoosatNetwork/HTND](https://github.com/HoosatNetwork/HTND) – Reference full node
- Hoosat Network Explorer & other tools

---

### License

This project is open source under the MIT License.

