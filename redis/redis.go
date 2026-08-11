// Package redis provides a Redis-backed Broadcaster for socketio, enabling
// multi-node scale-out: broadcasts are relayed between server instances over
// Redis pub/sub so a message emitted on one node reaches sockets connected to
// any node. It is the Go counterpart of the Node.js @socket.io/redis-adapter,
// and it speaks the Redis RESP protocol directly using only the standard
// library — no third-party client and no cgo:
//
//	bc, _ := redis.New(redis.Options{Addr: "localhost:6379", Channel: "socket.io"})
//	io.SetBroadcaster(bc) // io is a *socketio.Server
//
// A single socketio.Server keeps its room and socket membership in memory, so a
// broadcast only reaches clients connected to that one process. Once you run
// more than one instance behind a load balancer — for horizontal scaling or high
// availability — clients are spread across processes and an in-memory broadcast
// no longer reaches everyone. Install this Broadcaster on every node and each
// call to io.Emit, io.To(room).Emit, and the other broadcast forms is instead
// published once to Redis and delivered to the local sockets of every node that
// receives it. Reach for this package as soon as you scale past a single server.
//
// It works by holding two Redis connections: one for PUBLISH and one that issues
// SUBSCRIBE and runs a background receive loop. When the server broadcasts, it
// serializes the target namespace, rooms, exclusions, event name, and arguments
// and calls Publish, which PUBLISHes the bytes to the configured channel. Redis
// fans that message out to every subscriber, including the publisher, and the
// receive loop hands each incoming payload to the handler the server registered
// through OnMessage — which decodes it and re-emits to that node's local
// sockets. Because pub/sub echoes to the sender, the originating node delivers
// its own broadcast through exactly the same path, so no node is special-cased.
//
// New connects, authenticates (AUTH) and selects a database (SELECT) if
// configured, subscribes, and returns a *Broadcaster ready to pass to
// server.SetBroadcaster; Options carries Addr, Channel, Password, DB, and an
// optional Dial hook used by tests to substitute an in-memory connection. The
// three methods that satisfy socketio.Broadcaster — Publish, OnMessage, and
// Close — are safe for concurrent use: Publish serializes writes on the publish
// connection with a mutex, and OnMessage/Close guard shared state with their
// own lock. Close is idempotent and tears down both connections, ending the
// receive loop.
//
// Delivery inherits Redis pub/sub semantics, which callers should understand.
// Pub/sub is fire-and-forget and at-most-once: messages are not persisted or
// queued, so a node that is down or momentarily disconnected misses whatever was
// published while it was unavailable — matching the behavior of the Node redis
// adapter. Publish returns an error only if the local write to Redis fails, not
// if a remote subscriber never receives the message. The RESP codec here
// implements just the commands this adapter needs (SUBSCRIBE, PUBLISH, AUTH,
// SELECT and their replies); it is not a general-purpose Redis client. It does
// not (yet) implement the adapter's remote request/response features such as
// cross-node fetchSockets or server-side acknowledgements.
package redis

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
)

// Options configures the Redis broadcaster.
type Options struct {
	// Addr is the Redis server address (host:port).
	Addr string
	// Channel is the pub/sub channel used for broadcasts (default "socket.io").
	Channel string
	// Password, if set, authenticates via AUTH.
	Password string
	// DB selects a Redis database via SELECT (default 0).
	DB int
	// Dial overrides the network dialer (used in tests).
	Dial func(addr string) (net.Conn, error)
}

// Broadcaster relays socketio broadcasts over Redis pub/sub. It satisfies
// socketio.Broadcaster.
type Broadcaster struct {
	channel string

	pubMu   sync.Mutex
	pubConn net.Conn
	pubRW   *bufio.ReadWriter

	subConn net.Conn

	mu      sync.Mutex
	handler func([]byte)
	closed  bool
}

// New connects to Redis, subscribes to the broadcast channel, and returns a
// Broadcaster ready to install with server.SetBroadcaster.
func New(opts Options) (*Broadcaster, error) {
	if opts.Channel == "" {
		opts.Channel = "socket.io"
	}
	if opts.Addr == "" {
		opts.Addr = "127.0.0.1:6379"
	}
	dial := opts.Dial
	if dial == nil {
		dial = func(addr string) (net.Conn, error) { return net.Dial("tcp", addr) }
	}

	pub, err := connect(dial, opts)
	if err != nil {
		return nil, err
	}
	sub, err := connect(dial, opts)
	if err != nil {
		pub.Close()
		return nil, err
	}

	b := &Broadcaster{
		channel: opts.Channel,
		pubConn: pub,
		pubRW:   bufio.NewReadWriter(bufio.NewReader(pub), bufio.NewWriter(pub)),
		subConn: sub,
	}

	// Subscribe on the sub connection and start the receive loop.
	subRW := bufio.NewReadWriter(bufio.NewReader(sub), bufio.NewWriter(sub))
	if err := writeCommand(subRW.Writer, "SUBSCRIBE", opts.Channel); err != nil {
		_ = b.Close()
		return nil, err
	}
	go b.readLoop(subRW.Reader)
	return b, nil
}

// connect dials Redis and performs optional AUTH/SELECT.
func connect(dial func(string) (net.Conn, error), opts Options) (net.Conn, error) {
	conn, err := dial(opts.Addr)
	if err != nil {
		return nil, err
	}
	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	if opts.Password != "" {
		if err := command(rw, "AUTH", opts.Password); err != nil {
			conn.Close()
			return nil, err
		}
	}
	if opts.DB != 0 {
		if err := command(rw, "SELECT", strconv.Itoa(opts.DB)); err != nil {
			conn.Close()
			return nil, err
		}
	}
	return conn, nil
}

// ErrClosed is returned by Publish once the Broadcaster has been closed. The
// caller (socketio's broadcast operator) treats a Publish failure as "the
// cluster link is unavailable" and falls back to local-only delivery, so a
// closed or broken Redis link degrades instead of silently dropping messages.
var ErrClosed = errors.New("redis: broadcaster closed")

// Publish implements socketio.Broadcaster: it PUBLISHes data to the channel.
// It returns ErrClosed after Close, and the underlying I/O error if the Redis
// connection has failed.
func (b *Broadcaster) Publish(data []byte) error {
	b.mu.Lock()
	closed := b.closed
	b.mu.Unlock()
	if closed {
		return ErrClosed
	}
	b.pubMu.Lock()
	defer b.pubMu.Unlock()
	if err := writeCommandBytes(b.pubRW.Writer, [][]byte{[]byte("PUBLISH"), []byte(b.channel), data}); err != nil {
		return err
	}
	_, err := readReply(b.pubRW.Reader) // integer: number of receivers
	return err
}

// OnMessage implements socketio.Broadcaster.
func (b *Broadcaster) OnMessage(fn func([]byte)) {
	b.mu.Lock()
	b.handler = fn
	b.mu.Unlock()
}

// Close implements socketio.Broadcaster.
func (b *Broadcaster) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	b.mu.Unlock()
	var err error
	if b.subConn != nil {
		err = b.subConn.Close()
	}
	if b.pubConn != nil {
		if e := b.pubConn.Close(); err == nil {
			err = e
		}
	}
	return err
}

// readLoop reads pub/sub frames and dispatches "message" payloads to the
// handler.
func (b *Broadcaster) readLoop(r *bufio.Reader) {
	for {
		v, err := readReply(r)
		if err != nil {
			return
		}
		arr, ok := v.([]any)
		if !ok || len(arr) < 3 {
			continue
		}
		kind, _ := arr[0].([]byte)
		if string(kind) != "message" {
			continue // subscribe confirmation, etc.
		}
		payload, _ := arr[2].([]byte)
		b.mu.Lock()
		h := b.handler
		b.mu.Unlock()
		if h != nil {
			cp := make([]byte, len(payload))
			copy(cp, payload)
			h(cp)
		}
	}
}

// ---- RESP protocol ----------------------------------------------------------

func command(rw *bufio.ReadWriter, args ...string) error {
	if err := writeCommand(rw.Writer, args...); err != nil {
		return err
	}
	_, err := readReply(rw.Reader)
	return err
}

func writeCommand(w *bufio.Writer, args ...string) error {
	parts := make([][]byte, len(args))
	for i, a := range args {
		parts[i] = []byte(a)
	}
	return writeCommandBytes(w, parts)
}

func writeCommandBytes(w *bufio.Writer, args [][]byte) error {
	if _, err := fmt.Fprintf(w, "*%d\r\n", len(args)); err != nil {
		return err
	}
	for _, a := range args {
		if _, err := fmt.Fprintf(w, "$%d\r\n", len(a)); err != nil {
			return err
		}
		if _, err := w.Write(a); err != nil {
			return err
		}
		if _, err := w.WriteString("\r\n"); err != nil {
			return err
		}
	}
	return w.Flush()
}

// MaxBulkLength bounds the declared length of a RESP bulk string ("$<n>") that
// readReply will accept. The length arrives on the wire before the bytes do, so
// without a bound a corrupt or hostile peer could make the client allocate an
// arbitrary amount of memory (or panic in make) from a handful of bytes. 64 MiB
// is far above any broadcast payload this adapter produces.
const MaxBulkLength = 64 << 20

// MaxArrayLength bounds the declared element count of a RESP array ("*<n>"),
// for the same reason as MaxBulkLength: the count is used to size a slice
// before any element has been read. Redis pub/sub replies have three elements.
const MaxArrayLength = 1 << 20

// maxReplyDepth bounds RESP array nesting. Real Redis replies nest at most a
// couple of levels; the bound keeps a hostile stream of "*1\r\n" prefixes from
// recursing readReply until the goroutine stack is exhausted.
const maxReplyDepth = 32

// ErrReplyTooLarge is returned when a RESP reply declares a bulk string or
// array larger than MaxBulkLength / MaxArrayLength, or nests deeper than the
// internal depth limit. It signals a corrupt or hostile peer, not a transient
// failure: the connection's framing can no longer be trusted.
var ErrReplyTooLarge = errors.New("redis: reply exceeds size limit")

// readReply reads one RESP value: simple string/int → string/int64, bulk →
// []byte, array → []any, error → error. Malformed input (a non-numeric length,
// an oversized declaration, excessive nesting) yields an error; it never panics.
func readReply(r *bufio.Reader) (any, error) {
	return readReplyDepth(r, 0)
}

func readReplyDepth(r *bufio.Reader, depth int) (any, error) {
	if depth > maxReplyDepth {
		return nil, ErrReplyTooLarge
	}
	prefix, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	line, err := readLine(r)
	if err != nil {
		return nil, err
	}
	switch prefix {
	case '+':
		return string(line), nil
	case '-':
		return nil, errors.New("redis: " + string(line))
	case ':':
		n, err := strconv.ParseInt(string(line), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("redis: bad integer reply %q", line)
		}
		return n, nil
	case '$':
		n, err := strconv.Atoi(string(line))
		if err != nil {
			return nil, fmt.Errorf("redis: bad bulk length %q", line)
		}
		if n < 0 {
			return nil, nil
		}
		if n > MaxBulkLength {
			return nil, ErrReplyTooLarge
		}
		buf := make([]byte, n)
		if _, err := readFull(r, buf); err != nil {
			return nil, err
		}
		if _, err := readLine(r); err != nil { // trailing CRLF
			return nil, err
		}
		return buf, nil
	case '*':
		n, err := strconv.Atoi(string(line))
		if err != nil {
			return nil, fmt.Errorf("redis: bad array length %q", line)
		}
		if n < 0 {
			return nil, nil
		}
		if n > MaxArrayLength {
			return nil, ErrReplyTooLarge
		}
		arr := make([]any, n)
		for i := 0; i < n; i++ {
			v, err := readReplyDepth(r, depth+1)
			if err != nil {
				return nil, err
			}
			arr[i] = v
		}
		return arr, nil
	default:
		return nil, fmt.Errorf("redis: unexpected reply type %q", prefix)
	}
}

func readLine(r *bufio.Reader) ([]byte, error) {
	line, err := r.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	// strip trailing \r\n
	n := len(line)
	if n >= 2 && line[n-2] == '\r' {
		return line[:n-2], nil
	}
	return line[:n-1], nil
}

func readFull(r *bufio.Reader, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := r.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}
