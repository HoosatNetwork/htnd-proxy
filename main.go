package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Hoosat-Oy/HTND/infrastructure/network/netadapter/server/grpcserver"
	"github.com/Hoosat-Oy/HTND/infrastructure/network/netadapter/server/grpcserver/protowire"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	listenAddr  = flag.String("listen", ":42420", "listen address")
	refreshSec  = flag.Int("refresh", 30, "refresh interval")
	manualPeers = flag.String("manual-peers", "", "additional manual RPC peers (comma-separated)")
)

var (
	healthyPeers []string
	peersMu      sync.RWMutex
	nextPeer     uint64
)

const (
	peerProbeTimeout        = 10 * time.Second
	upstreamDialTimeout     = 5 * time.Second
	defaultLocalRPCPeerHost = "127.0.0.1"
	defaultLocalRPCPeerPort = "42520"
	maxConcurrentStreams    = ^uint32(0)
	listenAddrEnv           = "listen"
	listenAddrEnvUpper      = "LISTEN"
	localRPCPeerHostEnv     = "LOCAL_PEER_HOST"
	localRPCPeerPortEnv     = "LOCAL_PEER_PORT"
	manualRPCPeersEnv       = "MANUAL_RPC_PEERS"
)

func firstEnv(keys ...string) string {
	for _, key := range keys {
		if value := os.Getenv(key); value != "" {
			return value
		}
	}
	return ""
}

type proxyServer struct {
	protowire.UnimplementedRPCServer
}

type loggingListener struct {
	net.Listener
}

func (l loggingListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}

	log.Printf("Accepted TCP connection from %s to %s", conn.RemoteAddr(), conn.LocalAddr())
	return conn, nil
}

func fetchPeers() {
	candidates := manualRPCPeers()
	seen := make(map[string]struct{}, len(candidates))
	for _, address := range candidates {
		seen[address] = struct{}{}
	}

	resp, err := http.Get("https://hoosatworld.net/peers")
	if err != nil {
		log.Printf("fetch failed: %v", err)
		updateHealthyPeers(filterReachableRPCPeers(candidates))
		return
	}
	defer resp.Body.Close()

	var list []struct {
		Address string `json:"address"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		log.Printf("decode failed: %v", err)
		updateHealthyPeers(filterReachableRPCPeers(candidates))
		return
	}

	for _, p := range list {
		rpcAddress, ok := deriveRPCAddress(p.Address)
		if !ok {
			continue
		}
		if _, exists := seen[rpcAddress]; exists {
			continue
		}
		seen[rpcAddress] = struct{}{}
		candidates = append(candidates, rpcAddress)
	}

	updateHealthyPeers(filterReachableRPCPeers(candidates))
}

func updateHealthyPeers(good []string) {
	peersMu.Lock()
	healthyPeers = good
	peersMu.Unlock()
	log.Printf("Loaded %d RPC peers", len(good))
}

func getNextPeer() string {
	peersMu.RLock()
	defer peersMu.RUnlock()
	if len(healthyPeers) == 0 {
		return ""
	}
	index := atomic.AddUint64(&nextPeer, 1) - 1
	return healthyPeers[index%uint64(len(healthyPeers))]
}

func deriveRPCAddress(peerAddress string) (string, bool) {
	host, portText, err := net.SplitHostPort(peerAddress)
	if err != nil {
		return "", false
	}

	port, err := strconv.Atoi(portText)
	if err != nil || port <= 0 {
		return "", false
	}

	return net.JoinHostPort(host, strconv.Itoa(port-1)), true
}

// manualRPCPeers returns the local peer + any additional manual peers
func manualRPCPeers() []string {
	var peers []string

	// 1. Local / default peer
	host := firstEnv(localRPCPeerHostEnv)
	if host == "" {
		host = defaultLocalRPCPeerHost
	}
	port := firstEnv(localRPCPeerPortEnv)
	if port == "" {
		port = defaultLocalRPCPeerPort
	}
	localPeer := net.JoinHostPort(host, port)
	peers = append(peers, localPeer)

	// 2. From command-line flag
	if *manualPeers != "" {
		for _, p := range strings.Split(*manualPeers, ",") {
			p = strings.TrimSpace(p)
			if p != "" {
				peers = append(peers, p)
			}
		}
	}

	// 3. From environment variable
	if envPeers := os.Getenv(manualRPCPeersEnv); envPeers != "" {
		for _, p := range strings.Split(envPeers, ",") {
			p = strings.TrimSpace(p)
			if p != "" {
				peers = append(peers, p)
			}
		}
	}

	log.Printf("Manual RPC peers configured: %v", peers)
	return peers
}

func filterReachableRPCPeers(candidates []string) []string {
	if len(candidates) == 0 {
		return nil
	}

	reachable := make([]bool, len(candidates))
	var wg sync.WaitGroup
	for i, address := range candidates {
		log.Printf("Checking candidate %s", address)
		wg.Add(1)
		go func(index int, address string) {
			defer wg.Done()
			if probeRPCAddress(address) {
				reachable[index] = true
			}
		}(i, address)
	}

	wg.Wait()

	good := make([]string, 0, len(candidates))
	for i, address := range candidates {
		if reachable[i] {
			good = append(good, address)
		}
	}
	return good
}

func probeRPCAddress(address string) bool {
	log.Printf("Probing manual/direct RPC peer: %s", address)

	ctx, cancel := context.WithTimeout(context.Background(), peerProbeTimeout)
	defer cancel()

	connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("Failed to create gRPC client to %s: %v", address, err)
		return false
	}
	defer func() { _ = connection.Close() }()

	if err := waitForConnectionReady(ctx, connection); err != nil {
		log.Printf("Connection not ready for %s: %v", address, err)
		return false
	}

	client := protowire.NewRPCClient(connection)
	stream, err := client.MessageStream(ctx,
		grpc.MaxCallRecvMsgSize(grpcserver.RPCMaxMessageSize),
		grpc.MaxCallSendMsgSize(grpcserver.RPCMaxMessageSize),
	)
	if err != nil {
		log.Printf("Failed to open MessageStream to %s: %v", address, err)
		return false
	}
	defer func() { _ = stream.CloseSend() }()

	if err := stream.Send(&protowire.HoosatdMessage{
		Payload: &protowire.HoosatdMessage_GetInfoRequest{
			GetInfoRequest: &protowire.GetInfoRequestMessage{},
		},
	}); err != nil {
		log.Printf("Failed to send GetInfo to %s: %v", address, err)
		return false
	}

	response, err := stream.Recv()
	if err != nil {
		log.Printf("Failed to receive response from %s: %v", address, err)
		return false
	}

	info := response.GetGetInfoResponse()
	if info == nil {
		log.Printf("No GetInfoResponse from %s", address)
		return false
	}

	if rpcErr := info.GetError(); rpcErr != nil && rpcErr.GetMessage() != "" {
		log.Printf("RPC error from %s: %s", address, rpcErr.GetMessage())
		return false
	}

	success := info.GetIsSynced() && info.GetIsUtxoIndexed()
	log.Printf("Probe %s -> synced=%v, utxoIndexed=%v → %v",
		address, info.GetIsSynced(), info.GetIsUtxoIndexed(), success)

	return success
}

func snapshotPeers() []string {
	peersMu.RLock()
	defer peersMu.RUnlock()

	if len(healthyPeers) == 0 {
		return nil
	}

	peers := make([]string, len(healthyPeers))
	copy(peers, healthyPeers)
	return peers
}

func dialUpstream(ctx context.Context, target string) (*grpc.ClientConn, protowire.RPC_MessageStreamClient, error) {
	connection, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}

	readyCtx, cancel := context.WithTimeout(ctx, upstreamDialTimeout)
	defer cancel()
	if err := waitForConnectionReady(readyCtx, connection); err != nil {
		_ = connection.Close()
		return nil, nil, err
	}

	client := protowire.NewRPCClient(connection)
	stream, err := client.MessageStream(ctx,
		grpc.MaxCallRecvMsgSize(grpcserver.RPCMaxMessageSize),
		grpc.MaxCallSendMsgSize(grpcserver.RPCMaxMessageSize),
	)
	if err != nil {
		_ = connection.Close()
		return nil, nil, err
	}

	return connection, stream, nil
}

func waitForConnectionReady(ctx context.Context, connection *grpc.ClientConn) error {
	for {
		state := connection.GetState()
		switch state {
		case connectivity.Ready:
			return nil
		case connectivity.Shutdown:
			return context.Canceled
		case connectivity.Idle:
			connection.Connect()
		}

		if !connection.WaitForStateChange(ctx, state) {
			return ctx.Err()
		}
	}
}

func proxiedRPCName(message *protowire.HoosatdMessage) string {
	if message == nil || message.Payload == nil {
		return "unknown"
	}

	payloadType := reflect.TypeOf(message.Payload)
	if payloadType.Kind() == reflect.Ptr {
		payloadType = payloadType.Elem()
	}

	name := strings.TrimPrefix(payloadType.Name(), "HoosatdMessage_")
	name = strings.TrimSuffix(name, "Request")
	if name == "" {
		return "unknown"
	}

	return name
}

func relayClientToUpstream(client protowire.RPC_MessageStreamServer, upstream protowire.RPC_MessageStreamClient, target string, errChan chan<- error) {
	for {
		message, err := client.Recv()
		if err != nil {
			errChan <- err
			return
		}
		if target != "" {
			log.Printf("Proxying RPC %s to %s", proxiedRPCName(message), target)
			target = ""
		}
		if err := upstream.Send(message); err != nil {
			errChan <- err
			return
		}
	}
}

func relayUpstreamToClient(upstream protowire.RPC_MessageStreamClient, client protowire.RPC_MessageStreamServer, errChan chan<- error) {
	for {
		message, err := upstream.Recv()
		if err != nil {
			errChan <- err
			return
		}
		if err := client.Send(message); err != nil {
			errChan <- err
			return
		}
	}
}

func (p *proxyServer) MessageStream(stream protowire.RPC_MessageStreamServer) error {
	peers := snapshotPeers()
	if len(peers) == 0 {
		return grpc.ErrServerStopped
	}

	start := atomic.AddUint64(&nextPeer, 1) - 1
	var (
		upstreamConn   *grpc.ClientConn
		upstreamStream protowire.RPC_MessageStreamClient
		target         string
		err            error
	)

	for i := range peers {
		target = peers[(int(start)+i)%len(peers)]
		upstreamConn, upstreamStream, err = dialUpstream(stream.Context(), target)
		if err == nil {
			break
		}
		log.Printf("Upstream %s unavailable: %v", target, err)
	}
	if err != nil {
		return err
	}
	defer upstreamConn.Close()
	defer upstreamStream.CloseSend()

	errChan := make(chan error, 2)
	go relayClientToUpstream(stream, upstreamStream, target, errChan)
	go relayUpstreamToClient(upstreamStream, stream, errChan)

	return <-errChan
}

func main() {
	if envListenAddr := firstEnv(listenAddrEnv, listenAddrEnvUpper); envListenAddr != "" {
		*listenAddr = envListenAddr
	}

	flag.Parse()

	fetchPeers()
	go func() {
		ticker := time.NewTicker(time.Duration(*refreshSec) * time.Second)
		for range ticker.C {
			fetchPeers()
		}
	}()

	listener, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		log.Fatal(err)
	}
	listener = loggingListener{Listener: listener}

	server := grpc.NewServer(
		grpc.MaxConcurrentStreams(maxConcurrentStreams),
		grpc.MaxRecvMsgSize(grpcserver.RPCMaxMessageSize),
		grpc.MaxSendMsgSize(grpcserver.RPCMaxMessageSize),
		grpc.NumStreamWorkers(uint32(runtime.GOMAXPROCS(0))),
	)
	protowire.RegisterRPCServer(server, &proxyServer{})

	log.Printf("HTND RPC proxy listening on %s", *listenAddr)

	if err := server.Serve(listener); err != nil {
		log.Fatal(err)
	}
}
