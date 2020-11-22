package radix

import (
	"context"
	"errors"
	"math/rand"
	"net"
	"sync/atomic"
	"time"

	"github.com/mediocregopher/radix/v4/internal/proc"
	"github.com/mediocregopher/radix/v4/resp"
	"github.com/mediocregopher/radix/v4/trace"
	"github.com/tilinna/clock"
)

var errPoolFull = errors.New("connection pool is full")

// poolConn is a Conn which tracks the last net.Error which was seen either
// during an Encode call or a Decode call
type poolConn struct {
	Conn

	// A channel to which critical network errors are written. A critical
	// network error is basically any non-application level error, e.g. a
	// timeout, disconnect, etc...
	lastIOErrCh chan error
}

func newPoolConn(c Conn) *poolConn {
	return &poolConn{
		Conn:        c,
		lastIOErrCh: make(chan error, 1),
	}
}

func (pc *poolConn) EncodeDecode(ctx context.Context, m, u interface{}) error {
	err := pc.Conn.EncodeDecode(ctx, m, u)
	if err != nil && !errors.As(err, new(resp.ErrConnUsable)) {
		select {
		case pc.lastIOErrCh <- err:
		default:
		}
	}
	return err
}

func (pc *poolConn) Do(ctx context.Context, a Action) error {
	return a.Perform(ctx, pc)
}

func (pc *poolConn) Close() error {
	return pc.Conn.Close()
}

////////////////////////////////////////////////////////////////////////////////

type bwFlushCallback struct {
	resp.BufferedWriter
	cb func()
}

func (bw bwFlushCallback) Flush() error {
	bw.cb()
	return bw.BufferedWriter.Flush()
}

////////////////////////////////////////////////////////////////////////////////

// PoolConfig is used to create Pool instances with particular settings. All
// fields are optional, all methods are thread-safe.
type PoolConfig struct {
	// CustomPool indicates that this callback should be used in place of
	// NewClient when NewClient is called. All behavior of NewClient is
	// superceded when this is set.
	CustomPool func(ctx context.Context, network, addr string) (Client, error)

	// Dialer is used by Pool to create new Conns to the Pool's redis instance.
	Dialer Dialer

	// Size indicates the minimum number of Conns the Pool will attempt to
	// maintain.
	//
	// If -1 then the Pool will not maintain any open Conns and all Actions will
	// result in the creation and closing of a fresh Conn (except for where the
	// overflow buffer is used, see OverflowBufferSize).
	//
	// Defaults to 4.
	Size int

	// PingInterval specifies the interval at which a ping event happens. On
	// each ping event the Pool calls the PING redis command one one of its
	// available connections.
	//
	// Since connections are used in LIFO order, the ping interval * pool size
	// is the duration of time it takes to ping every connection once when the
	// pool is idle.
	//
	// If not given then the default value is calculated to be roughly 5 seconds
	// to check every connection in the Pool.
	PingInterval time.Duration

	// RefillInterval specifies the interval at which a refill event happens. On
	// each refill event the Pool checks to see if it is full, and if it's not a
	// single connection is created and added to it.
	//
	// Defaults to 1 second.
	RefillInterval time.Duration

	// Trace contains callbacks that a Pool can use to trace itself.
	//
	// All callbacks are blocking.
	Trace trace.PoolTrace

	// ErrCh is a channel which asynchronous errors encountered by the Pool will
	// be written to. If the channel blocks the error will be dropped. The
	// channel will be closed when the Pool is closed.
	ErrCh chan<- error

	// Clock is used for time operations and is primarily useful for testing.
	//
	// Defaults to clock.Realtime().
	Clock clock.Clock
}

func (cfg PoolConfig) withDefaults() PoolConfig {
	if cfg.Size == -1 {
		cfg.Size = 0
	} else if cfg.Size == 0 {
		cfg.Size = 4
	}
	if cfg.PingInterval == 0 {
		cfg.PingInterval = 5 * time.Second / time.Duration(cfg.Size+1)
	}
	if cfg.RefillInterval == 0 {
		cfg.RefillInterval = 1 * time.Second
	}
	if cfg.Clock == nil {
		cfg.Clock = clock.Realtime()
	}

	cfg.Dialer = cfg.Dialer.withDefaults()
	return cfg
}

// TODO allow for setting a hard upper limit on number of connections while also
// allowing for overflow buffer. See
// https://github.com/mediocregopher/radix/issues/219.

// Pool is a dynamic connection pool which implements the Client interface. It
// takes in a number of options which can effect its specific behavior; see the
// NewPool method.
//
// Pool is dynamic in that it can create more connections on-the-fly to handle
// increased load. The maximum number of extra connections (if any) can be
// configured, along with how long they are kept after load has returned to
// normal.
//
type Pool struct {
	// Atomic fields must be at the beginning of the struct since they must be
	// correctly aligned or else access may cause panics on 32-bit architectures
	// See https://golang.org/pkg/sync/atomic/#pkg-note-BUG
	totalConns int64 // atomic, must only be access using functions from sync/atomic

	proc          *proc.Proc
	cfg           PoolConfig
	network, addr string
	pool          []*poolConn
	notifyCh      chan struct{}
}

var _ Client = new(Pool)

// NewClient calls New and returns the Pool as a Client. If CustomPool is set in
// the PoolConfig then that is called instead.
func (cfg PoolConfig) NewClient(ctx context.Context, network, addr string) (Client, error) {
	if cfg.CustomPool != nil {
		return cfg.CustomPool(ctx, network, addr)
	}
	p, err := cfg.New(ctx, network, addr)
	return p, err
}

// New initializes and returns a Pool instance using the PoolConfig. This will
// panic if CustomPool is set in the PoolConfig, use NewClient instead.
func (cfg PoolConfig) New(ctx context.Context, network, addr string) (*Pool, error) {
	if cfg.CustomPool != nil {
		panic("Cannot use PoolConfig.New with PoolConfig.CustomPool")
	}

	cfg = cfg.withDefaults()
	p := &Pool{
		proc:     proc.New(),
		cfg:      cfg,
		network:  network,
		addr:     addr,
		pool:     make([]*poolConn, 0, cfg.Size),
		notifyCh: make(chan struct{}, 1),
	}

	// make one Conn synchronously to ensure there's actually a redis instance
	// present. The rest will be created asynchronously.
	pc, err := p.newConn(ctx, trace.PoolConnCreatedReasonInitialization)
	if err != nil {
		return nil, err
	}
	p.put(pc)

	p.proc.Run(func(ctx context.Context) {
		startTime := p.cfg.Clock.Now()
		for i := 0; i < p.cfg.Size-1; i++ {
			pc, err := p.newConn(ctx, trace.PoolConnCreatedReasonInitialization)
			if err != nil {
				p.err(err)
				// if there was an error connecting to the instance than it
				// might need a little breathing room, redis can sometimes get
				// sad if too many connections are created simultaneously.
				p.cfg.Clock.Sleep(100 * time.Millisecond)
				continue
			} else if !p.put(pc) {
				// if the connection wasn't put in it could be for two reasons:
				// - the Pool has already started being used and is full.
				// - Close was called.
				// in any case, bail
				break
			}
		}
		p.traceInitCompleted(p.cfg.Clock.Since(startTime))

	})

	if p.cfg.PingInterval > 0 && p.cfg.Size > 0 {
		p.atIntervalDo(p.cfg.PingInterval, func(ctx context.Context) {
			ctx, cancel := p.cfg.Clock.TimeoutContext(ctx, 5*time.Second)
			defer cancel()
			// TODO this needs to manually retrieve a connection from the pool.
			p.Do(ctx, Cmd(nil, "PING"))
		})
	}
	if p.cfg.RefillInterval > 0 && p.cfg.Size > 0 {
		p.atIntervalDo(p.cfg.RefillInterval, p.doRefill)
	}
	return p, nil
}

func (p *Pool) traceInitCompleted(elapsedTime time.Duration) {
	if p.cfg.Trace.InitCompleted != nil {
		p.cfg.Trace.InitCompleted(trace.PoolInitCompleted{
			PoolCommon:  p.traceCommon(),
			ElapsedTime: elapsedTime,
		})
	}
}

func (p *Pool) err(err error) {
	select {
	case p.cfg.ErrCh <- err:
	default:
	}
}

func (p *Pool) traceCommon() trace.PoolCommon {
	return trace.PoolCommon{
		Network: p.network, Addr: p.addr,
		PoolSize:   p.cfg.Size,
		AvailCount: len(p.pool),
	}
}

func (p *Pool) traceConnCreated(
	ctx context.Context,
	connectTime time.Duration,
	reason trace.PoolConnCreatedReason,
	err error,
) {
	if p.cfg.Trace.ConnCreated != nil {
		p.cfg.Trace.ConnCreated(trace.PoolConnCreated{
			PoolCommon:  p.traceCommon(),
			Context:     ctx,
			Reason:      reason,
			ConnectTime: connectTime,
			Err:         err,
		})
	}
}

func (p *Pool) traceConnClosed(reason trace.PoolConnClosedReason) {
	if p.cfg.Trace.ConnClosed != nil {
		p.cfg.Trace.ConnClosed(trace.PoolConnClosed{
			PoolCommon: p.traceCommon(),
			Reason:     reason,
		})
	}
}

func (p *Pool) newConn(ctx context.Context, reason trace.PoolConnCreatedReason) (*poolConn, error) {
	start := p.cfg.Clock.Now()
	c, err := p.cfg.Dialer.Dial(ctx, p.network, p.addr)
	elapsed := p.cfg.Clock.Since(start)
	p.traceConnCreated(ctx, elapsed, reason, err)
	if err != nil {
		return nil, err
	}
	pc := newPoolConn(c)
	atomic.AddInt64(&p.totalConns, 1)
	return pc, nil
}

func (p *Pool) atIntervalDo(d time.Duration, do func(context.Context)) {
	p.proc.Run(func(ctx context.Context) {
		t := p.cfg.Clock.NewTicker(d)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				do(ctx)
			case <-ctx.Done():
				return
			}
		}

	})
}

func (p *Pool) doRefill(ctx context.Context) {
	if atomic.LoadInt64(&p.totalConns) >= int64(p.cfg.Size) {
		return
	}
	pc, err := p.newConn(ctx, trace.PoolConnCreatedReasonRefill)
	if err == nil {
		p.putConn(pc)
	} else if err != errPoolFull {
		p.err(err)
	}
}

func (p *Pool) getConn(ctx context.Context) (*poolConn, error) {
	for {
		var pc *poolConn
		err := p.proc.WithLock(func() error {
			if len(p.pool) == 0 {
				return nil
			}

			i := len(p.pool) - 1
			pc, p.pool = p.pool[i], p.pool[:i]
			return nil
		})
		if err != nil || pc != nil {
			return pc, err
		}

		select {
		case <-p.notifyCh:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// returns true if the connection was put back, false if it was closed and
// discarded.
func (p *Pool) putConn(pc *poolConn) bool {
	select {
	case <-pc.lastIOErrCh:
		pc.Close()
		p.traceConnClosed(trace.PoolConnClosedReasonError)
		atomic.AddInt64(&p.totalConns, -1)
		return false
	default:
	}

	err := p.proc.WithLock(func() error {
		p.pool = append(p.pool, pc)
		select {
		case p.notifyCh <- struct{}{}:
		default:
		}
		return nil
	})
	return err == nil
}

func (p *Pool) useConn(ctx context.Context, a Action) error {
	for {
		var ok bool
		err := p.proc.WithRLock(func() error {
			if len(p.pool) == 0 {
				return nil
			}
			ok = true
			pc := p.pool[rand.Intn(len(p.pool))]
			return pc.Do(ctx, a)
		})
		if ok || err != nil {
			return err
		}

		select {
		case <-p.notifyCh:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Do implements the Do method of the Client interface by retrieving a Conn out
// of the pool, calling Perform on the given Action with it, and returning the
// Conn to the pool.
func (p *Pool) Do(ctx context.Context, a Action) error {
	if a.Properties().CanShareConn {
		return p.useConn(ctx, a)
	}

	pc, err := p.getConn(ctx)
	if err != nil {
		return err
	}

	err = pc.Do(ctx, a)
	p.putConn(pc)
	return err
}

// NumAvailConns returns the number of connections currently available in the
// pool, as well as in the overflow buffer if that option is enabled.
func (p *Pool) NumAvailConns() int {
	return len(p.pool)
}

// Addr implements the method for the Client interface.
func (p *Pool) Addr() net.Addr {
	return rawAddr{network: p.network, addr: p.addr}
}

// Close implements the method for the Client interface.
func (p *Pool) Close() error {
	return p.proc.Close(func() error {
		for _, pc := range p.pool {
			pc.Close()
			atomic.AddInt64(&p.totalConns, -1)
			p.traceConnClosed(trace.PoolConnClosedReasonPoolClosed)
		}
		if p.cfg.ErrCh != nil {
			close(p.cfg.ErrCh)
		}
		return nil
	})
}
