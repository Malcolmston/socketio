// Package socketio is a pure-Go port of the Node.js Socket.IO server. It
// implements the Engine.IO v4 transport layer (HTTP long-polling and WebSocket,
// with the polling→websocket upgrade) and the Socket.IO v5 protocol (namespaces,
// rooms, events, and acknowledgements), exposing an API that mirrors the
// original JavaScript library so that idioms such as io.on("connection"),
// socket.join(room), and io.to(room).emit(...) translate almost line for line:
//
//	io := socketio.New()
//	io.OnConnection(func(s *socketio.Socket) {
//		s.On("chat", func(args []any) []any {
//			io.To("room1").Emit("chat", args...)
//			return nil
//		})
//		s.Join("room1")
//	})
//	http.Handle("/socket.io/", io)
//	http.ListenAndServe(":3000", nil)
//
// Reach for this package when you want real-time, bidirectional, event-based
// communication between a Go backend and Socket.IO clients (browsers, mobile
// apps, or the companion client package) without pulling in cgo or third-party
// dependencies — the entire stack, down to the RFC 6455 WebSocket framing, is
// built on the standard library. A *Server is an http.Handler, so it mounts on
// any net/http server or router: use ServeHTTP directly, Attach it to a
// *http.ServeMux, or wrap an existing handler with Handler to coexist with a
// REST/Express-style application on the same port.
//
// Internally each connected client owns one Engine.IO session (a conn),
// identified by a session id (sid). A session begins over HTTP long-polling and
// is transparently upgraded to WebSocket via the Engine.IO probe handshake; the
// engineio subpackage encodes the transport frames and the polling payload,
// while this package encodes the Socket.IO layer on top — CONNECT, EVENT, ACK,
// DISCONNECT, and their binary variants. Events carry JSON arguments; any
// []byte in a payload is transmitted out-of-band as a BINARY_EVENT with
// placeholder markers (see binary.go). A single session may be attached to
// several Namespaces at once, each of which multiplexes its own sockets, rooms,
// and connection middleware over the shared transport.
//
// The concurrency and delivery semantics follow from that design. The server
// sends periodic heartbeat pings and disconnects a session whose pong is
// overdue by more than PingInterval+PingTimeout. Inbound events are dispatched
// on their own goroutine so a handler may block on an acknowledgement without
// stalling the read loop, which means handlers for the same socket can run
// concurrently and must guard any shared state; ordering of delivery to the
// wire is preserved, but ordering between independently dispatched handlers is
// not. Socket.Emit targets one client and returns an error, whereas the
// broadcast forms (Server.Emit, Namespace.Emit, and BroadcastOperator.Emit) fan
// out to many recipients and deliberately do not surface a per-socket error.
// Rooms are managed by a pluggable Adapter; the default keeps all membership in
// process.
//
// Parity with the Node reference implementation is close but not total. The
// wire protocols are compatible, so this server interoperates with the official
// JavaScript client and this module's client package. Room broadcasting,
// acknowledgements, connection middleware (Use), per-socket data (Set/Get), and
// multi-node scale-out (via SetBroadcaster and the redis subpackage) are all
// supported. Differences reflect Go idioms and scope: handlers use typed
// func([]any) []any signatures rather than variadic JS callbacks, there is no
// built-in adapter persistence beyond what a Broadcaster provides, and features
// tied to the Node runtime (such as the admin UI or the msgpack parser) are out
// of scope. See COMPATIBILITY.md in the repository for the authoritative matrix.
package socketio

import (
	"crypto/rand"
	"encoding/base64"
	"net/http"
	"strings"
	"sync"
	"time"
)

// DefaultPath is the HTTP path Socket.IO serves from.
const DefaultPath = "/socket.io/"

// Options configures a Server.
type Options struct {
	// Path is the HTTP path the server handles (default "/socket.io/").
	Path string
	// PingInterval is how often the server sends heartbeat pings.
	PingInterval time.Duration
	// PingTimeout is how long the server waits for a pong before disconnecting.
	PingTimeout time.Duration
	// MaxPayload advertises the maximum HTTP payload size to clients.
	MaxPayload int
	// CheckOrigin, if set, authorizes cross-origin requests. When nil, all
	// origins are allowed — including cross-site WebSocket handshakes, which
	// are not subject to the same-origin policy. Set it on any deployment where
	// a connection carries ambient authority (cookies, session headers).
	CheckOrigin func(r *http.Request) bool
	// TrustProxy makes Socket.Handshake().Address honor the first entry of the
	// X-Forwarded-For header. Leave it false unless a reverse proxy you control
	// overwrites that header: it is otherwise fully client-controlled.
	TrustProxy bool
	// MaxSessions caps the number of concurrent Engine.IO sessions the server
	// will hold. Every unauthenticated handshake request allocates a session and
	// a heartbeat goroutine that survive until the ping timeout elapses, so
	// without a cap a request flood grows the session map without bound. When
	// the cap is reached, further handshakes are refused with 503 while existing
	// sessions keep working. Zero selects DefaultMaxSessions; a negative value
	// disables the cap entirely.
	MaxSessions int
}

// DefaultMaxSessions is the concurrent-session cap applied when
// Options.MaxSessions is zero. It is deliberately far above what a single
// process is expected to serve: it exists to bound a handshake flood, not to
// throttle legitimate traffic.
const DefaultMaxSessions = 65536

func (o *Options) withDefaults() {
	if o.Path == "" {
		o.Path = DefaultPath
	}
	if o.PingInterval == 0 {
		o.PingInterval = 25 * time.Second
	}
	if o.PingTimeout == 0 {
		o.PingTimeout = 20 * time.Second
	}
	if o.MaxPayload == 0 {
		o.MaxPayload = 1000000
	}
	if o.MaxSessions == 0 {
		o.MaxSessions = DefaultMaxSessions
	}
}

// Server is a Socket.IO server. It implements http.Handler and should be
// mounted at its configured Path.
type Server struct {
	opts Options

	mu             sync.RWMutex
	namespaces     map[string]*Namespace
	conns          map[string]*conn // engine.io sessions by sid
	serverHandlers map[string][]func([]any)
	broadcaster    Broadcaster
}

// New creates a Server. An optional Options may be supplied.
func New(opts ...Options) *Server {
	var o Options
	if len(opts) > 0 {
		o = opts[0]
	}
	o.withDefaults()
	s := &Server{
		opts:       o,
		namespaces: make(map[string]*Namespace),
		conns:      make(map[string]*conn),
	}
	// The default namespace always exists.
	s.namespaces["/"] = newNamespace(s, "/")
	return s
}

// Of returns the namespace with the given name, creating it if necessary. A
// name without a leading slash has one added.
func (s *Server) Of(name string) *Namespace {
	if name == "" {
		name = "/"
	}
	if !strings.HasPrefix(name, "/") {
		name = "/" + name
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	ns, ok := s.namespaces[name]
	if !ok {
		ns = newNamespace(s, name)
		s.namespaces[name] = ns
	}
	return ns
}

// namespace returns an existing namespace or nil.
func (s *Server) namespace(name string) *Namespace {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.namespaces[name]
}

// OnConnection registers a handler invoked when a socket connects to the
// default namespace. It is the equivalent of io.on("connection", ...).
func (s *Server) OnConnection(fn func(*Socket)) {
	s.Of("/").OnConnection(fn)
}

// Emit broadcasts an event to every socket in the default namespace.
func (s *Server) Emit(event string, args ...any) {
	s.Of("/").Emit(event, args...)
}

// To returns a broadcast operator scoped to a room in the default namespace.
func (s *Server) To(room string) *BroadcastOperator {
	return s.Of("/").To(room)
}

// Sockets returns all connected sockets in the default namespace.
func (s *Server) Sockets() []*Socket {
	return s.Of("/").Sockets()
}

// ServeHTTP implements http.Handler and dispatches Engine.IO transport
// requests.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	origin := r.Header.Get("Origin")
	if s.opts.CheckOrigin != nil && origin != "" && !s.opts.CheckOrigin(r) {
		http.Error(w, "origin not allowed", http.StatusForbidden)
		return
	}
	// CORS: reflect the origin so browser clients can connect.
	//
	// Credentials (cookies, HTTP auth) are only allowed for an origin the
	// application has vetted. Reflecting an arbitrary origin *and* setting
	// Access-Control-Allow-Credentials would let any web page read this
	// server's polling responses with the visitor's ambient credentials, which
	// is the same class of bug as a wildcard CORS policy. Node's socket.io
	// likewise sends no CORS headers until `cors` is configured.
	if origin != "" {
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Add("Vary", "Origin") // Add, not Set: don't clobber a Vary a wrapper set
		if s.opts.CheckOrigin != nil {
			w.Header().Set("Access-Control-Allow-Credentials", "true")
		}
	}
	if r.Method == http.MethodOptions {
		w.Header().Set("Access-Control-Allow-Methods", "GET,POST")
		w.Header().Set("Access-Control-Allow-Headers", "content-type")
		w.WriteHeader(http.StatusNoContent)
		return
	}

	transport := r.URL.Query().Get("transport")
	sid := r.URL.Query().Get("sid")

	switch transport {
	case "websocket":
		s.handleWebSocket(w, r, sid)
	case "polling", "":
		s.handlePolling(w, r, sid)
	default:
		http.Error(w, "unknown transport", http.StatusBadRequest)
	}
}

// Use registers connection middleware on the default namespace.
func (s *Server) Use(fn func(socket *Socket, next func(err error))) *Server {
	s.Of("/").Use(fn)
	return s
}

// FetchSockets returns all sockets in the default namespace.
func (s *Server) FetchSockets() []*Socket { return s.Of("/").FetchSockets() }

// SocketsJoin makes every socket in the default namespace join the given rooms.
func (s *Server) SocketsJoin(rooms ...string) { s.Of("/").SocketsJoin(rooms...) }

// SocketsLeave makes every socket in the default namespace leave the given rooms.
func (s *Server) SocketsLeave(rooms ...string) { s.Of("/").SocketsLeave(rooms...) }

// DisconnectSockets disconnects every socket in the default namespace.
func (s *Server) DisconnectSockets(closeTransport bool) {
	s.Of("/").DisconnectSockets(closeTransport)
}

// OnServerEvent registers a handler for server-side events delivered via
// ServerSideEmit. On this single-node implementation these are local; a
// multi-node deployment would relay them between servers through an adapter.
func (s *Server) OnServerEvent(event string, handler func(args []any)) *Server {
	s.mu.Lock()
	if s.serverHandlers == nil {
		s.serverHandlers = make(map[string][]func([]any))
	}
	s.serverHandlers[event] = append(s.serverHandlers[event], handler)
	s.mu.Unlock()
	return s
}

// ServerSideEmit emits an event to other servers (and this one), the equivalent
// of io.serverSideEmit. Single-node: it invokes locally registered handlers.
func (s *Server) ServerSideEmit(event string, args ...any) {
	s.mu.RLock()
	handlers := append([]func([]any){}, s.serverHandlers[event]...)
	s.mu.RUnlock()
	for _, h := range handlers {
		h(args)
	}
}

// Handler wraps the server so it intercepts Socket.IO requests (those under its
// Path) and delegates everything else to next — the Go equivalent of attaching
// Socket.IO to an existing HTTP server that is otherwise served by Express:
//
//	app := express.New()          // your routes
//	io := socketio.New()          // your socket handlers
//	http.ListenAndServe(":3000", io.Handler(app))
//
// next may be any http.Handler (an *express.Application, an http.ServeMux, ...);
// pass nil to 404 non-Socket.IO requests.
func (s *Server) Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, s.opts.Path) {
			s.ServeHTTP(w, r)
			return
		}
		if next != nil {
			next.ServeHTTP(w, r)
			return
		}
		http.NotFound(w, r)
	})
}

// Attach registers the server on an http.ServeMux at its Path.
func (s *Server) Attach(mux *http.ServeMux) { mux.Handle(s.opts.Path, s) }

// Close disconnects every connected session and shuts the server down. It does
// not stop the underlying http.Server (the caller owns that).
func (s *Server) Close() {
	s.mu.Lock()
	conns := make([]*conn, 0, len(s.conns))
	for _, c := range s.conns {
		conns = append(conns, c)
	}
	s.mu.Unlock()
	for _, c := range conns {
		s.removeConn(c)
	}
}

// getConn returns the session for a sid, or nil.
func (s *Server) getConn(sid string) *conn {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.conns[sid]
}

// newConnFrom creates a session and captures the handshake details of the HTTP
// request that established it, so Socket.Handshake can report them later. It
// returns nil when Options.MaxSessions is already reached.
func (s *Server) newConnFrom(r *http.Request) *conn {
	c := s.newConn()
	if c == nil {
		return nil
	}
	c.handshake = newHandshake(r, nil, s.opts.TrustProxy)
	return c
}

// newConn creates and registers a new engine.io session, or returns nil when
// the server is already holding Options.MaxSessions sessions.
func (s *Server) newConn() *conn {
	c := &conn{
		sid:      newID(),
		server:   s,
		sockets:  make(map[string]*Socket),
		lastPong: time.Now(),
	}
	s.mu.Lock()
	if s.opts.MaxSessions > 0 && len(s.conns) >= s.opts.MaxSessions {
		s.mu.Unlock()
		return nil
	}
	s.conns[c.sid] = c
	s.mu.Unlock()
	return c
}

// removeConn deregisters a session and cleans up its sockets.
func (s *Server) removeConn(c *conn) {
	s.mu.Lock()
	delete(s.conns, c.sid)
	s.mu.Unlock()
	c.cleanup()
}

// newID generates a Socket.IO-style base64url identifier.
func newID() string {
	b := make([]byte, 15)
	_, _ = rand.Read(b)
	return base64.RawURLEncoding.EncodeToString(b)
}
