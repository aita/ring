package ring

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
)

const (
	DefaultHeartbeatThreshold = 3
	DefaultHeartbeatFrequency = 500 * time.Millisecond
	DefaultHeartbeatTimeout   = 100 * time.Millisecond

	versionCommand = "version\r\n"
	versionPrefix  = "VERSION"
)

type Hash interface {
	Get(key string) (node string)
	Set(nodes map[string]Server)
	Members() []string
}

func resolveAddr(addr string) (net.Addr, error) {
	if strings.Contains(addr, "/") {
		return net.ResolveUnixAddr("unix", addr)
	} else {
		return net.ResolveTCPAddr("tcp", addr)
	}
}

type shard struct {
	addr  string
	down  int32
	naddr atomic.Value
}

func newShards(servers map[string]Server) (map[string]*shard, error) {
	shards := map[string]*shard{}
	for _, server := range servers {
		naddr, err := resolveAddr(server.Addr)
		if err != nil {
			return nil, err
		}
		shard := &shard{
			addr: server.Addr,
			down: 0,
		}
		shard.naddr.Store(naddr)
	}
	return shards, nil
}

func (shard *shard) netAddr() net.Addr {
	return shard.naddr.Load().(net.Addr)
}

func (shard *shard) isDown(threshold int) bool {
	return atomic.LoadInt32(&shard.down) >= int32(threshold)
}

func (shard *shard) checkHealth(opts *Options) (stateChanged bool) {
	if shard.isDown(opts.HeartbeatThreshold) {
		addr, err := resolveAddr(shard.addr)
		if err != nil {
			return false
		}
		shard.naddr.Store(addr)
	}

	err := checkServerHealth(shard.netAddr(), opts)
	up := err == nil
	if up {
		stateChanged = shard.isDown(opts.HeartbeatThreshold)
		atomic.StoreInt32(&shard.down, 0)
		return
	}

	if shard.isDown(opts.HeartbeatThreshold) {
		stateChanged = false
		return
	}

	atomic.AddInt32(&shard.down, 1)
	stateChanged = shard.isDown(opts.HeartbeatThreshold)
	return
}

type Server struct {
	Addr   string
	Weight uint32
}

type Options struct {
	Servers            map[string]Server
	Dialer             func(ctx context.Context, network, addr string) (net.Conn, error)
	Hash               Hash
	HeartbeatThreshold int
	HeartbeatTimeout   time.Duration
	HeartbeatFrequency time.Duration
}

func (opts *Options) init() error {
	for _, server := range opts.Servers {
		if server.Weight == 0 {
			return errors.New("ring: the weight of server must be greater than 0")
		}
	}
	if opts.Dialer == nil {
		var d net.Dialer
		opts.Dialer = d.DialContext
	}
	if opts.Hash == nil {
		opts.Hash = &Ketama{}
	}
	if opts.HeartbeatThreshold == 0 {
		opts.HeartbeatThreshold = DefaultHeartbeatThreshold
	}
	if opts.HeartbeatTimeout == 0 {
		opts.HeartbeatTimeout = DefaultHeartbeatTimeout
	}
	if opts.HeartbeatFrequency == 0 {
		opts.HeartbeatFrequency = DefaultHeartbeatFrequency
	}
	return nil
}

type Ring struct {
	opts   *Options
	mu     sync.RWMutex
	shards map[string]*shard
}

var _ memcache.ServerSelector = (*Ring)(nil)

func New(opts *Options) (*Ring, error) {
	if err := opts.init(); err != nil {
		return nil, err
	}
	ring := &Ring{
		opts: opts,
	}
	shards, err := newShards(ring.opts.Servers)
	if err != nil {
		return nil, err
	}
	ring.shards = shards
	ring.rebalance()
	go ring.heartbeat()
	return ring, nil
}

func (ring *Ring) heartbeat() {
	ticker := time.NewTicker(ring.opts.HeartbeatFrequency)
	defer ticker.Stop()

	for range ticker.C {
		ring.mu.RLock()
		shards := ring.shards
		ring.mu.RUnlock()

		rebalance := false
		for _, shard := range shards {
			if shard.checkHealth(ring.opts) {
				rebalance = true
			}
		}

		if rebalance {
			ring.rebalance()
		}
	}
}

func (ring *Ring) rebalance() {
	ring.mu.Lock()
	defer ring.mu.Unlock()

	servers := map[string]Server{}
	for name, shard := range ring.shards {
		if !shard.isDown((ring.opts.HeartbeatThreshold)) {
			servers[name] = ring.opts.Servers[name]
		}
	}
	ring.opts.Hash.Set(servers)
}

func (ring *Ring) PickServer(key string) (net.Addr, error) {
	ring.mu.RLock()
	defer ring.mu.RUnlock()

	nservers := len(ring.opts.Hash.Members())
	if nservers == 0 {
		return nil, memcache.ErrNoServers
	}
	server := ring.opts.Hash.Get(key)
	shard, ok := ring.shards[server]
	if !ok {
		return nil, memcache.ErrNoServers
	}
	return shard.netAddr(), nil
}

func (ring *Ring) Each(f func(net.Addr) error) error {
	ring.mu.RLock()
	defer ring.mu.RUnlock()

	members := ring.opts.Hash.Members()
	for _, server := range members {
		shard := ring.shards[server]
		if err := f(shard.netAddr()); nil != err {
			return err
		}
	}
	return nil
}

func checkServerHealth(addr net.Addr, opts *Options) error {
	d := time.Now().Add(opts.HeartbeatTimeout)
	ctx, cancel := context.WithDeadline(context.Background(), d)
	defer cancel()

	nc, err := opts.Dialer(ctx, addr.Network(), addr.String())
	if err != nil {
		return err
	}
	defer nc.Close()

	errch := make(chan error)
	go func() {
		errch <- ping(nc)
	}()
	select {
	case err := <-errch:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

var readerPool = sync.Pool{
	New: func() interface{} {
		return &bufio.Reader{}
	},
}

func ping(nc net.Conn) error {
	n, err := io.WriteString(nc, versionCommand)
	if n < len(versionCommand) && err == nil {
		err = io.ErrShortWrite
	}
	if err != nil {
		return err
	}
	r := readerPool.Get().(*bufio.Reader)
	r.Reset(nc)
	defer readerPool.Put(r)
	line, err := r.ReadSlice('\n')
	if err != nil {
		return err
	}
	if !bytes.HasPrefix(line, []byte(versionPrefix)) {
		return fmt.Errorf("ring: unexpected response: %q", string(line))
	}
	return nil
}
