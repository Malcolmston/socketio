// Package client is a Go Socket.IO client. It connects to a Socket.IO server
// (this module's server or the Node reference server) over the WebSocket
// transport and provides the familiar On/Emit/EmitWithAck API that mirrors the
// browser socket.io-client, letting a Go process act as a first-class Socket.IO
// peer rather than merely a server:
//
//	c, err := client.Dial("http://localhost:3000")
//	c.On("news", func(args []any) []any { fmt.Println(args); return nil })
//	c.Emit("hello", "world")
//
// Use it whenever a Go program needs to talk to a Socket.IO endpoint as a
// client: integration and end-to-end tests against a running server, service-to-
// service messaging where the other side already speaks Socket.IO, bots and
// load generators, or bridging a Socket.IO event stream into another system.
// Dial blocks until the Socket.IO CONNECT handshake for the chosen namespace
// completes (or DialTimeout elapses), so a successful return means the client is
// fully connected and ready to Emit. Optional authentication data is sent with
// the CONNECT packet via Options.Auth, matching the server-side socket.Auth().
//
// Addressing follows socket.io-client: the PATH of the dialled URL names the
// namespace, so Dial("http://host/admin") connects to namespace "/admin", while
// the HTTP path the server is mounted on is Options.Path (default
// DefaultPath, "/socket.io/"). See ResolveEndpoint for the full set of rules.
//
// Under the hood the client speaks Engine.IO v4 directly over a single
// WebSocket (it does not use HTTP long-polling): it dials ws(s)://.../socket.io/
// with EIO=4&transport=websocket, reads the Engine.IO OPEN handshake, sends the
// Socket.IO CONNECT packet, and then runs a background read loop that answers
// heartbeat pings, dispatches inbound EVENT packets to handlers registered with
// On, and resolves acknowledgements. Outgoing events are encoded with the shared
// socketio packet codec, so binary ([]byte) arguments are automatically split
// into BINARY_EVENT attachment frames and reassembled on receipt.
//
// Concurrency and lifecycle: On, Emit, EmitWithAck, and Close are safe to call
// from multiple goroutines. Handlers run on the read-loop goroutine, so a
// handler that itself blocks on EmitWithAck should hand off to another goroutine
// to avoid deadlocking the loop; returning a non-nil slice from a handler
// acknowledges an event the server sent with an ack id. EmitWithAck waits up to
// the supplied timeout and returns an error if no acknowledgement arrives. When
// Options.Reconnection is enabled, an unexpected transport drop triggers a
// reconnect loop on the same schedule as the JavaScript client's Manager — a
// Backoff seeded with ReconnectionDelay, ReconnectionDelayMax and
// RandomizationFactor, bounded by ReconnectionAttempts — and fires the local
// "reconnect" event; a user-initiated Close suppresses reconnection. Note that
// socket.io-client reconnects by default whereas Options.Reconnection must be
// set explicitly, because a Go zero value cannot express a true default.
//
// Compared with the JavaScript socket.io-client the surface is intentionally
// small: one namespace per Client, no long-polling fallback, and lifecycle
// events surfaced as ordinary events ("disconnect", "reconnect") registered
// through On. The wire protocol is identical, so this client interoperates with
// the Node reference server and with this module's server package. For
// multi-namespace usage, Dial a separate Client per namespace.
package client

import (
	"errors"
	"math/rand"
	"net/url"
	"strings"
	"sync"
	"time"

	socketio "github.com/malcolmston/socketio"
	"github.com/malcolmston/socketio/engineio"
	"github.com/malcolmston/socketio/internal/ws"
)

// Handler handles an inbound event. Returning a non-nil slice acknowledges an
// event the server sent with an ack id.
type Handler func(args []any) []any

// Client is a connected Socket.IO client.
type Client struct {
	namespace string
	wsURL     string
	opts      Options

	mu          sync.Mutex
	ws          *ws.Conn
	handlers    map[string][]Handler
	ackIDs      AckIDs
	pendingAcks map[uint64]chan []any
	sid         string
	closed      bool
	nsClosed    bool
	discFired   bool
	connErr     error
	pendingBin  *pendingBinary

	connectedCh chan struct{}
	connectOnce *sync.Once
}

// DefaultPath is the HTTP path a Socket.IO server is mounted on, and the path
// the client requests unless Options.Path says otherwise. It is the counterpart
// of socket.io-client's `path` option (whose default is "/socket.io").
const DefaultPath = "/socket.io/"

// Options configures Dial. The zero value is usable: every field below states
// the default applied by WithDefaults.
type Options struct {
	// Namespace to connect to. When empty the namespace is taken from the path
	// of the dialled URL (socket.io-client's rule), which is "/" for a bare
	// host.
	Namespace string
	// Path is the HTTP path the server is mounted on (default DefaultPath). It
	// is never taken from the dialled URL, whose path names the namespace. A
	// missing trailing slash is added.
	Path string
	// Auth is an optional payload sent with the CONNECT packet.
	Auth any
	// DialTimeout bounds the connection handshake (default 20s, matching
	// socket.io-client's `timeout`).
	DialTimeout time.Duration
	// Reconnection enables automatic reconnection after an unexpected
	// disconnect. Note that socket.io-client reconnects by DEFAULT; a Go zero
	// value cannot express a true default, so this must be set explicitly (see
	// API-DEVIATIONS.md).
	Reconnection bool
	// ReconnectionAttempts bounds reconnection tries (0 = unlimited, which is
	// socket.io-client's Infinity default).
	ReconnectionAttempts int
	// ReconnectionDelay is the initial delay between attempts (default 1s).
	ReconnectionDelay time.Duration
	// ReconnectionDelayMax is the ceiling the delay grows to (default 5s).
	ReconnectionDelayMax time.Duration
	// RandomizationFactor spreads each reconnection delay by up to this
	// fraction, in [0,1]. socket.io-client defaults it to 0.5; a Go zero value
	// cannot express that default, so jitter is off unless set (see
	// API-DEVIATIONS.md).
	RandomizationFactor float64
}

// WithDefaults returns a copy of o with every unset field filled in with the
// default documented on it. Dial and ResolveEndpoint both apply it, so the
// options a Client runs with are always the normalised ones.
func (o Options) WithDefaults() Options {
	if o.Path == "" {
		o.Path = DefaultPath
	}
	if !strings.HasSuffix(o.Path, "/") {
		o.Path += "/"
	}
	if !strings.HasPrefix(o.Path, "/") {
		o.Path = "/" + o.Path
	}
	if o.DialTimeout <= 0 {
		o.DialTimeout = 20 * time.Second
	}
	if o.ReconnectionDelay <= 0 {
		o.ReconnectionDelay = time.Second
	}
	if o.ReconnectionDelayMax <= 0 {
		o.ReconnectionDelayMax = 5 * time.Second
	}
	if o.RandomizationFactor < 0 {
		o.RandomizationFactor = 0
	}
	if o.RandomizationFactor > 1 {
		o.RandomizationFactor = 0
	}
	return o
}

// ConnectPacket builds the Socket.IO CONNECT packet that opens a namespace,
// carrying auth as its payload when non-nil. It is what Dial sends after the
// Engine.IO handshake, and the counterpart of socket.io-client's
// Socket._sendConnectPacket.
func ConnectPacket(namespace string, auth any) socketio.Packet {
	p := socketio.Packet{Type: socketio.Connect, Namespace: namespace}
	if auth != nil {
		p.Data = auth
	}
	return p
}

// EventPacket builds the EVENT packet an Emit sends: the event name followed by
// its arguments, addressed to namespace. A non-nil ackID asks the server for an
// acknowledgement under that id (what EmitWithAck does). Reserved Socket.IO
// event names are refused with socketio.ErrReservedEvent, matching
// socket.io-client, which throws for them.
func EventPacket(namespace, event string, ackID *uint64, args []any) (socketio.Packet, error) {
	if socketio.IsReservedEvent(event) {
		return socketio.Packet{}, socketio.ErrReservedEvent
	}
	data := append([]any{event}, args...)
	return socketio.Packet{Type: socketio.Event, Namespace: namespace, ID: ackID, Data: data}, nil
}

// AckIDs allocates the acknowledgement ids an emit claims: 0 for the first
// acknowledged emit on a connection, then 1, 2, … . It is the counterpart of
// socket.io-client's Socket.ids counter, and every Client owns one. The zero
// value is ready to use and is safe for concurrent use.
type AckIDs struct {
	mu   sync.Mutex
	next uint64
}

// Next returns the id for the next acknowledged emit and advances the counter.
func (a *AckIDs) Next() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	id := a.next
	a.next++
	return id
}

// Dial connects to a Socket.IO server at rawURL (http:// or ws://) and returns
// once the Socket.IO CONNECT handshake completes. The path of rawURL names the
// namespace, as in socket.io-client — see ResolveEndpoint.
func Dial(rawURL string, opts ...Options) (*Client, error) {
	var o Options
	if len(opts) > 0 {
		o = opts[0]
	}
	endpoint, err := ResolveEndpoint(rawURL, o)
	if err != nil {
		return nil, err
	}
	o = o.WithDefaults()
	o.Namespace = endpoint.Namespace

	c := &Client{
		namespace:   endpoint.Namespace,
		wsURL:       endpoint.URL,
		opts:        o,
		handlers:    make(map[string][]Handler),
		pendingAcks: make(map[uint64]chan []any),
	}
	if err := c.establish(); err != nil {
		return nil, err
	}
	return c, nil
}

// establish opens the transport, performs the Engine.IO + Socket.IO handshakes,
// and starts the read loop. It is used for both the initial connection and
// reconnections.
func (c *Client) establish() error {
	conn, err := ws.Dial(c.wsURL, nil)
	if err != nil {
		return err
	}

	// First frame must be the Engine.IO OPEN packet.
	mt, data, err := conn.ReadMessage()
	if err != nil || mt != ws.TextMessage {
		_ = conn.Close()
		return errors.New("client: expected OPEN packet")
	}
	openPkt, err := engineio.Decode(string(data))
	if err != nil || openPkt.Type != engineio.Open {
		_ = conn.Close()
		return errors.New("client: malformed OPEN packet")
	}

	c.mu.Lock()
	c.ws = conn
	c.connectedCh = make(chan struct{})
	c.connectOnce = &sync.Once{}
	c.connErr = nil
	// A fresh transport starts a fresh namespace session: clear the
	// disconnected/disconnect-fired flags so a reconnect delivers events again.
	c.nsClosed = false
	c.discFired = false
	c.mu.Unlock()

	go c.readLoop(conn)

	if err := c.sendPacket(ConnectPacket(c.namespace, c.opts.Auth)); err != nil {
		_ = conn.Close()
		return err
	}

	select {
	case <-c.connectedCh:
		c.mu.Lock()
		e := c.connErr
		c.mu.Unlock()
		if e != nil {
			_ = conn.Close()
			return e
		}
		return nil
	case <-time.After(c.opts.DialTimeout):
		_ = conn.Close()
		return errors.New("client: connect timeout")
	}
}

// reconnectLoop attempts to re-establish the connection with exponential
// backoff after an unexpected disconnect.
func (c *Client) reconnectLoop() {
	// Same schedule as socket.io-client's Manager: a backo2-shaped exponential
	// backoff seeded from the reconnection options, jittered by
	// RandomizationFactor.
	backoff := NewBackoff(BackoffOptions{
		Min:    c.opts.ReconnectionDelay,
		Max:    c.opts.ReconnectionDelayMax,
		Jitter: c.opts.RandomizationFactor,
		Rand:   rand.Float64,
	})
	for attempt := 1; c.opts.ReconnectionAttempts == 0 || attempt <= c.opts.ReconnectionAttempts; attempt++ {
		time.Sleep(backoff.Duration())
		c.mu.Lock()
		closed := c.closed
		c.mu.Unlock()
		if closed {
			return
		}
		if err := c.establish(); err == nil {
			backoff.Reset()
			c.fireLocal("reconnect")
			return
		}
	}
}

// fireLocal dispatches a client-lifecycle event ("reconnect", "disconnect") to
// any handlers registered for it.
func (c *Client) fireLocal(event string, args ...any) {
	c.mu.Lock()
	handlers := append([]Handler(nil), c.handlers[event]...)
	c.mu.Unlock()
	for _, h := range handlers {
		h(args)
	}
}

// ID returns the socket id assigned by the server.
func (c *Client) ID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sid
}

// On registers a handler for an event.
func (c *Client) On(event string, h Handler) {
	c.mu.Lock()
	c.handlers[event] = append(c.handlers[event], h)
	c.mu.Unlock()
}

// Emit sends an event to the server. Emitting a reserved Socket.IO event name
// (connect, connect_error, disconnect, disconnecting, ...) returns
// socketio.ErrReservedEvent without sending anything, matching socket.io-client,
// which throws for those names.
func (c *Client) Emit(event string, args ...any) error {
	p, err := EventPacket(c.namespace, event, nil, args)
	if err != nil {
		return err
	}
	return c.sendPacket(p)
}

// EmitWithAck sends an event and waits for the server's acknowledgement, up to
// timeout.
func (c *Client) EmitWithAck(event string, timeout time.Duration, args ...any) ([]any, error) {
	id := c.ackIDs.Next()
	c.mu.Lock()
	ch := make(chan []any, 1)
	c.pendingAcks[id] = ch
	c.mu.Unlock()

	p, err := EventPacket(c.namespace, event, &id, args)
	if err != nil {
		c.mu.Lock()
		delete(c.pendingAcks, id)
		c.mu.Unlock()
		return nil, err
	}
	if err := c.sendPacket(p); err != nil {
		return nil, err
	}
	select {
	case reply := <-ch:
		return reply, nil
	case <-time.After(timeout):
		c.mu.Lock()
		delete(c.pendingAcks, id)
		c.mu.Unlock()
		return nil, errors.New("client: ack timeout")
	}
}

// Close disconnects from the server.
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	conn := c.ws
	c.mu.Unlock()
	_ = c.sendPacket(socketio.Packet{Type: socketio.Disconnect, Namespace: c.namespace})
	if conn != nil {
		return conn.Close()
	}
	return nil
}

// pendingBinary accumulates binary attachments for an inbound binary packet.
type pendingBinary struct {
	packet  socketio.Packet
	buffers [][]byte
	need    int
}

func (c *Client) sendPacket(p socketio.Packet) error {
	c.mu.Lock()
	conn := c.ws
	c.mu.Unlock()
	if conn == nil {
		return errors.New("client: not connected")
	}
	text, buffers, err := p.EncodeBinary()
	if err != nil {
		return err
	}
	if err := conn.WriteText(engineio.NewMessage(text).Encode()); err != nil {
		return err
	}
	for _, buf := range buffers {
		if err := conn.WriteMessage(ws.BinaryMessage, buf); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) readLoop(conn *ws.Conn) {
	for {
		mt, data, err := conn.ReadMessage()
		if err != nil {
			c.finishConnect(errors.New("client: connection closed before connect"))
			c.handleDisconnect()
			return
		}
		if mt == ws.BinaryMessage {
			c.handleBinaryAttachment(data)
			continue
		}
		if mt != ws.TextMessage {
			continue
		}
		ep, err := engineio.Decode(string(data))
		if err != nil {
			continue
		}
		switch ep.Type {
		case engineio.Ping:
			_ = conn.WriteText(engineio.Packet{Type: engineio.Pong, Data: ep.Data}.Encode())
		case engineio.Message:
			c.handleSioPacket(ep.Data)
		case engineio.Close:
			c.handleDisconnect()
			return
		}
	}
}

// handleDisconnect fails any pending acks and, when reconnection is enabled and
// the client was not closed by the user, starts the reconnect loop. The reason
// mirrors socket.io-client's disconnect reasons: a transport that drops without
// a namespace DISCONNECT packet reports "transport close".
func (c *Client) handleDisconnect() {
	c.mu.Lock()
	closed := c.closed
	reconnect := c.opts.Reconnection && !closed
	c.mu.Unlock()
	// In-flight EmitWithAck calls unblock via their own timeout.

	c.fireDisconnectOnce("transport close")
	if reconnect {
		go c.reconnectLoop()
	}
}

// fireDisconnectOnce emits the local "disconnect" event with a reason argument,
// at most once per connection. socket.io-client's "disconnect" always carries a
// reason; a server-initiated namespace disconnect reports "io server
// disconnect", a dropped transport "transport close".
func (c *Client) fireDisconnectOnce(reason string) {
	c.mu.Lock()
	if c.discFired {
		c.mu.Unlock()
		return
	}
	c.discFired = true
	c.mu.Unlock()
	c.fireLocal("disconnect", reason)
}

func (c *Client) handleSioPacket(raw string) {
	pkt, err := socketio.DecodePacket(raw)
	if err != nil || pkt.Namespace != c.namespace {
		return
	}
	// Once the server has disconnected this namespace, ignore any further
	// packets for it — upstream drops events that arrive after a DISCONNECT
	// rather than delivering them to handlers.
	c.mu.Lock()
	nsClosed := c.nsClosed
	c.mu.Unlock()
	if nsClosed {
		return
	}
	switch pkt.Type {
	case socketio.Disconnect:
		// Server-initiated namespace disconnect. Mark the namespace closed, fire
		// the local "disconnect" with the reason socket.io-client uses, and stop
		// delivering events. Previously this packet had no case and was dropped,
		// so the client never learned about a namespace-level disconnect.
		c.mu.Lock()
		c.nsClosed = true
		c.mu.Unlock()
		c.fireDisconnectOnce("io server disconnect")
	case socketio.Connect:
		if obj, ok := pkt.Data.(map[string]any); ok {
			if id, ok := obj["sid"].(string); ok {
				c.mu.Lock()
				c.sid = id
				c.mu.Unlock()
			}
		}
		c.finishConnect(nil)
	case socketio.ConnectError:
		msg := "connect error"
		if obj, ok := pkt.Data.(map[string]any); ok {
			if m, ok := obj["message"].(string); ok {
				msg = m
			}
		}
		c.finishConnect(errors.New("client: " + msg))
	case socketio.Event:
		c.dispatch(pkt)
	case socketio.Ack:
		c.resolveAck(pkt)
	case socketio.BinaryEvent, socketio.BinaryAck:
		if pkt.Attachments() > 0 {
			c.mu.Lock()
			c.pendingBin = &pendingBinary{packet: pkt, need: pkt.Attachments()}
			c.mu.Unlock()
			return
		}
		c.dispatchBinary(pkt)
	}
}

// handleBinaryAttachment collects an inbound binary buffer and dispatches once
// all attachments for the pending packet have arrived.
func (c *Client) handleBinaryAttachment(buf []byte) {
	c.mu.Lock()
	pb := c.pendingBin
	if pb == nil {
		c.mu.Unlock()
		return
	}
	pb.buffers = append(pb.buffers, buf)
	if len(pb.buffers) < pb.need {
		c.mu.Unlock()
		return
	}
	c.pendingBin = nil
	c.mu.Unlock()

	pkt := pb.packet
	data, err := socketio.Reconstruct(pkt.Data, pb.buffers)
	if err != nil {
		// Drop a frame with an out-of-range binary placeholder rather than
		// dispatching a raw placeholder object to a handler as if it were data.
		return
	}
	pkt.Data = data
	c.dispatchBinary(pkt)
}

// dispatchBinary routes a reassembled binary packet as an event or ack.
func (c *Client) dispatchBinary(pkt socketio.Packet) {
	if pkt.Type == socketio.BinaryEvent {
		pkt.Type = socketio.Event
		c.dispatch(pkt)
		return
	}
	pkt.Type = socketio.Ack
	c.resolveAck(pkt)
}

func (c *Client) resolveAck(pkt socketio.Packet) {
	if pkt.ID == nil {
		return
	}
	c.mu.Lock()
	ch := c.pendingAcks[*pkt.ID]
	delete(c.pendingAcks, *pkt.ID)
	c.mu.Unlock()
	if ch != nil {
		ch <- pkt.Args()
	}
}

func (c *Client) dispatch(pkt socketio.Packet) {
	c.mu.Lock()
	handlers := append([]Handler(nil), c.handlers[pkt.EventName()]...)
	c.mu.Unlock()

	var ack []any
	for _, h := range handlers {
		if reply := h(pkt.Args()); reply != nil && ack == nil {
			ack = reply
		}
	}
	// Acknowledge only when a handler actually produced an ack (returned a
	// non-nil slice). Auto-acking an event no handler handled made the server's
	// EmitAck timeout unreachable when the peer was a Go client, and erased the
	// difference between "handled, nothing to report" and "no handler at all".
	if pkt.ID != nil && ack != nil {
		_ = c.sendPacket(socketio.Packet{Type: socketio.Ack, Namespace: c.namespace, ID: pkt.ID, Data: ack})
	}
}

func (c *Client) finishConnect(err error) {
	// Snapshot the once/channel under the lock so a concurrent reconnect (which
	// swaps them in establish) can't race with this signal.
	c.mu.Lock()
	once := c.connectOnce
	ch := c.connectedCh
	c.mu.Unlock()
	if once == nil {
		return
	}
	once.Do(func() {
		c.mu.Lock()
		c.connErr = err
		c.mu.Unlock()
		close(ch)
	})
}

// Endpoint is where a Dial will actually connect: the Engine.IO WebSocket URL
// and the Socket.IO namespace to hand shake for. It is the result of applying
// socket.io-client's addressing rules to a user-supplied URL.
type Endpoint struct {
	// URL is the ws:// or wss:// URL of the Engine.IO endpoint, including the
	// mandatory EIO=4&transport=websocket query.
	URL string
	// Namespace is the Socket.IO namespace the CONNECT packet will name.
	Namespace string
}

// ResolveEndpoint derives the Engine.IO WebSocket URL and the Socket.IO
// namespace from a user-supplied URL, following the same rules as
// socket.io-client's url() plus Transport.uri():
//
//   - http/ws become ws, https/wss become wss;
//   - the URL's PATH is the namespace, not the HTTP path — io("http://h/admin")
//     connects to namespace "/admin". The HTTP path the server is mounted on is
//     Options.Path (default DefaultPath), never taken from the URL;
//   - a port that is the default for the scheme (80 for ws, 443 for wss) is
//     omitted, so ws://h:80/ and ws://h/ produce one endpoint, not two;
//   - userinfo is dropped: Engine.IO addresses the host, and credentials belong
//     in Options.Auth or a header, not in the transport URL;
//   - any query in the URL is preserved, with EIO=4 and transport=websocket
//     added.
//
// An explicit Options.Namespace wins over the namespace in the URL. Dial calls
// this; it is exported so that addressing can be inspected (and tested) without
// opening a connection.
func ResolveEndpoint(rawURL string, opts Options) (Endpoint, error) {
	o := opts.WithDefaults()
	u, err := url.Parse(rawURL)
	if err != nil {
		return Endpoint{}, err
	}
	switch u.Scheme {
	case "http", "ws":
		u.Scheme = "ws"
	case "https", "wss":
		u.Scheme = "wss"
	default:
		u.Scheme = "ws"
	}

	// The URL path names the namespace (socket.io-client: io.socket(parsed.path)).
	namespace := u.Path
	if namespace == "" {
		namespace = "/"
	}
	if !strings.HasPrefix(namespace, "/") {
		namespace = "/" + namespace
	}
	if opts.Namespace != "" {
		namespace = opts.Namespace
	}

	// Credentials never travel in the transport URL.
	u.User = nil

	// Drop a port that is the scheme's default, as Transport._port does.
	if port := u.Port(); (u.Scheme == "ws" && port == "80") || (u.Scheme == "wss" && port == "443") {
		host := u.Hostname()
		if strings.Contains(host, ":") { // IPv6 literal
			host = "[" + host + "]"
		}
		u.Host = host
	}

	u.Path = o.Path
	q := u.Query()
	q.Set("EIO", "4")
	q.Set("transport", "websocket")
	u.RawQuery = q.Encode()
	return Endpoint{URL: u.String(), Namespace: namespace}, nil
}
