package socketio

import (
	"errors"
	"sync"
)

// Namespace is a communication channel that partitions a Socket.IO server. Each
// namespace has its own set of connected sockets, rooms, and connection
// handlers. The default namespace is "/".
type Namespace struct {
	name   string
	server *Server

	mu           sync.RWMutex
	adapter      Adapter
	connHandlers []func(*Socket)
	middleware   []func(*Socket, func(error))
}

// SetAdapter replaces the namespace's room adapter (e.g. with a Redis-backed
// one for multi-node scale-out). Call it before any sockets connect.
func (ns *Namespace) SetAdapter(a Adapter) *Namespace {
	ns.mu.Lock()
	ns.adapter = a
	ns.mu.Unlock()
	return ns
}

// Use registers connection middleware for the namespace. Each middleware runs
// for every incoming connection before the connection handler; calling next
// with a non-nil error rejects the connection with a CONNECT_ERROR carrying the
// error's message — the equivalent of io.use((socket, next) => ...).
func (ns *Namespace) Use(fn func(socket *Socket, next func(err error))) *Namespace {
	ns.mu.Lock()
	ns.middleware = append(ns.middleware, fn)
	ns.mu.Unlock()
	return ns
}

// errMiddlewareSilent marks a middleware chain that terminated without the
// middleware calling next() at all — neither next(nil) nor next(err). Upstream
// leaves such a handshake unanswered (the client eventually times out); the
// caller treats this sentinel as "do not admit the socket and do not reply",
// distinct from an explicit rejection, which sends a CONNECT_ERROR.
var errMiddlewareSilent = errors.New("socketio: middleware did not call next")

// runMiddleware executes the namespace middleware chain for a socket. It returns
// nil when every middleware called next(nil); the middleware's error when one
// called next(err); or errMiddlewareSilent when a middleware returned without
// calling next at all. A silent return must NOT be treated as acceptance — that
// would admit an unauthenticated socket whenever an auth middleware chose to
// reject by returning without calling next(err), the safe-by-default deny shape.
func (ns *Namespace) runMiddleware(s *Socket) error {
	ns.mu.RLock()
	chain := append([]func(*Socket, func(error)){}, ns.middleware...)
	ns.mu.RUnlock()

	for _, mw := range chain {
		called := false
		var runErr error
		mw(s, func(err error) {
			called = true
			runErr = err
		})
		if !called {
			// The middleware never continued the chain: deny, and do not answer
			// the handshake. Returning nil here (as this used to) told the caller
			// the socket was accepted — an authentication bypass.
			return errMiddlewareSilent
		}
		if runErr != nil {
			return runErr
		}
	}
	return nil
}

func newNamespace(server *Server, name string) *Namespace {
	return &Namespace{
		name:    name,
		server:  server,
		adapter: newMemoryAdapter(),
	}
}

// Name returns the namespace name (e.g. "/" or "/admin").
func (ns *Namespace) Name() string { return ns.name }

// OnConnection registers a handler invoked for each new socket that connects to
// this namespace.
func (ns *Namespace) OnConnection(fn func(*Socket)) *Namespace {
	ns.mu.Lock()
	ns.connHandlers = append(ns.connHandlers, fn)
	ns.mu.Unlock()
	return ns
}

// add creates and registers a socket for a new namespace connection.
func (ns *Namespace) add(c *conn, _ string, auth any) *Socket {
	// Register with the adapter before creating the socket so the socket's
	// implicit self-room join resolves.
	s := &Socket{
		id:          newID(),
		namespace:   ns,
		conn:        c,
		auth:        auth,
		handshake:   c.handshake.withAuth(auth),
		handlers:    make(map[string][]EventHandler),
		rooms:       make(map[string]struct{}),
		pendingAcks: make(map[uint64]func([]any)),
	}
	ns.adapter.Add(s.id, s)
	ns.join(s, s.id) // implicit room named after the socket id
	s.startDispatcher()
	return s
}

// fireConnection runs the namespace connection handlers for a socket.
func (ns *Namespace) fireConnection(s *Socket) {
	ns.mu.RLock()
	handlers := append([]func(*Socket){}, ns.connHandlers...)
	ns.mu.RUnlock()
	for _, h := range handlers {
		h(s)
	}
}

// remove deletes a socket from the namespace and all rooms.
func (ns *Namespace) remove(s *Socket) {
	ns.adapter.Remove(s.id)
}

// join adds a socket to a room.
func (ns *Namespace) join(s *Socket, room string) {
	ns.adapter.Join(s.id, room)
	s.addRoom(room)
}

// leave removes a socket from a room.
func (ns *Namespace) leave(s *Socket, room string) {
	ns.adapter.Leave(s.id, room)
	s.removeRoom(room)
}

// Sockets returns all sockets currently connected to the namespace.
func (ns *Namespace) Sockets() []*Socket { return ns.adapter.AllSockets() }

// FetchSockets returns all sockets in the namespace (alias for Sockets, matching
// io.fetchSockets()).
func (ns *Namespace) FetchSockets() []*Socket { return ns.adapter.AllSockets() }

// SocketsInRoom returns the sockets that are members of a room.
func (ns *Namespace) SocketsInRoom(room string) []*Socket {
	return ns.adapter.SocketsInRoom(room)
}

// SocketsJoin makes every socket in the namespace join the given rooms.
func (ns *Namespace) SocketsJoin(rooms ...string) {
	for _, s := range ns.Sockets() {
		s.Join(rooms...)
	}
}

// SocketsLeave makes every socket in the namespace leave the given rooms.
func (ns *Namespace) SocketsLeave(rooms ...string) {
	for _, s := range ns.Sockets() {
		s.Leave(rooms...)
	}
}

// DisconnectSockets disconnects every socket in the namespace.
func (ns *Namespace) DisconnectSockets(closeTransport bool) {
	for _, s := range ns.Sockets() {
		s.Disconnect(closeTransport)
	}
}

// Emit broadcasts an event to every socket in the namespace.
func (ns *Namespace) Emit(event string, args ...any) {
	(&BroadcastOperator{ns: ns}).Emit(event, args...)
}

// To returns a broadcast operator scoped to a room within this namespace.
func (ns *Namespace) To(room string) *BroadcastOperator {
	return &BroadcastOperator{ns: ns, rooms: []string{room}}
}
