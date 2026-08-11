package socketio

import (
	"errors"
	"sync"
	"time"
)

// EventHandler handles an inbound event. It receives the event arguments and
// may return a non-nil slice to acknowledge the event (sent back to the client
// when the event requested an ack).
type EventHandler func(args []any) []any

// Socket is a single client connection to a namespace. It is the primary object
// applications interact with — registering event handlers, emitting events, and
// joining rooms.
type Socket struct {
	id        string
	namespace *Namespace
	conn      *conn
	auth      any
	handshake *Handshake

	mu                    sync.Mutex
	handlers              map[string][]EventHandler
	rooms                 map[string]struct{}
	disconnectHandlers    []func(reason string)
	disconnectingHandlers []func(reason string)
	ackCounter            uint64
	pendingAcks           map[uint64]func([]any)
	disconnected          bool
	disconnecting         bool
	data                  map[string]any
	anyHandlers           []func(event string, args []any)

	// inbox is the socket's ordered inbound-event queue. Every EVENT/BINARY_EVENT
	// for this socket is appended here and dispatched by a single goroutine, so
	// handlers run in the order the client emitted them — Socket.IO guarantees
	// ordered delivery within a socket. Dispatching each packet on its own
	// goroutine (as this used to) reordered back-to-back emits arbitrarily.
	inboxMu   sync.Mutex
	inboxCond *sync.Cond
	inbox     []Packet
	inboxStop bool
}

// ID returns the socket's unique identifier.
func (s *Socket) ID() string { return s.id }

// Auth returns the authentication payload the client sent with CONNECT.
func (s *Socket) Auth() any { return s.auth }

// Set stores an arbitrary value on the socket, persisting for the lifetime of
// the connection — the equivalent of Socket.IO's socket.data. Use it to attach
// session-like state (the authenticated user, a tenant id, ...).
func (s *Socket) Set(key string, value any) *Socket {
	s.mu.Lock()
	if s.data == nil {
		s.data = make(map[string]any)
	}
	s.data[key] = value
	s.mu.Unlock()
	return s
}

// Get returns a value previously stored with Set, and whether it was present.
func (s *Socket) Get(key string) (any, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data[key]
	return v, ok
}

// GetString returns a stored string value, or "" if missing / not a string.
func (s *Socket) GetString(key string) string {
	if v, ok := s.Get(key); ok {
		if str, ok := v.(string); ok {
			return str
		}
	}
	return ""
}

// Delete removes a stored value.
func (s *Socket) Delete(key string) *Socket {
	s.mu.Lock()
	delete(s.data, key)
	s.mu.Unlock()
	return s
}

// Data returns a copy of all values stored on the socket.
func (s *Socket) Data() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]any, len(s.data))
	for k, v := range s.data {
		out[k] = v
	}
	return out
}

// Handshake returns the details of the HTTP request that established this
// socket's session — headers, query string, address, TLS flag, and the auth
// payload sent with CONNECT — the equivalent of socket.handshake in the
// JavaScript API. It is nil only for sockets created outside an HTTP request
// (in tests). The returned value is a per-socket snapshot; mutating it does not
// affect the session or other namespaces on it.
func (s *Socket) Handshake() *Handshake { return s.handshake }

// Namespace returns the namespace this socket belongs to.
func (s *Socket) Namespace() *Namespace { return s.namespace }

// On registers a handler for an event.
func (s *Socket) On(event string, handler EventHandler) *Socket {
	s.mu.Lock()
	s.handlers[event] = append(s.handlers[event], handler)
	s.mu.Unlock()
	return s
}

// Off removes all handlers for an event.
func (s *Socket) Off(event string) *Socket {
	s.mu.Lock()
	delete(s.handlers, event)
	s.mu.Unlock()
	return s
}

// OnDisconnect registers a callback invoked when the socket disconnects.
func (s *Socket) OnDisconnect(fn func(reason string)) *Socket {
	s.mu.Lock()
	s.disconnectHandlers = append(s.disconnectHandlers, fn)
	s.mu.Unlock()
	return s
}

// OnDisconnecting registers a callback invoked as the socket begins to
// disconnect, before it has left its rooms — the equivalent of
// socket.on("disconnecting"). Unlike OnDisconnect, the socket's Rooms() are
// still populated when this fires, so a handler can record which rooms the
// socket was in on its way out. It runs with the same reason string that the
// subsequent disconnect carries.
func (s *Socket) OnDisconnecting(fn func(reason string)) *Socket {
	s.mu.Lock()
	s.disconnectingHandlers = append(s.disconnectingHandlers, fn)
	s.mu.Unlock()
	return s
}

// Emit sends an event to this socket. Emitting a reserved Socket.IO event name
// (connect, connect_error, disconnect, disconnecting, ...) returns
// ErrReservedEvent without sending anything, matching the JavaScript server,
// which throws for those names — they are the library's own lifecycle signals
// and must not be spoofed as application events.
func (s *Socket) Emit(event string, args ...any) error {
	if IsReservedEvent(event) {
		return ErrReservedEvent
	}
	return s.conn.sendPacket(newEvent(s.namespace.name, event, args, nil))
}

// startDispatcher launches the single goroutine that delivers this socket's
// inbound events in order. It is started once, when the socket is created.
func (s *Socket) startDispatcher() {
	s.inboxCond = sync.NewCond(&s.inboxMu)
	go s.dispatchLoop()
}

// enqueue appends an inbound event to the ordered queue. It never blocks the
// transport read loop: a handler that blocks (for example on EmitAck) stalls
// only this socket's own queue, while the ACK it awaits still arrives on the
// read loop and resolves independently.
func (s *Socket) enqueue(pkt Packet) {
	s.inboxMu.Lock()
	if s.inboxStop {
		s.inboxMu.Unlock()
		return
	}
	s.inbox = append(s.inbox, pkt)
	s.inboxCond.Signal()
	s.inboxMu.Unlock()
}

func (s *Socket) dispatchLoop() {
	for {
		s.inboxMu.Lock()
		for len(s.inbox) == 0 && !s.inboxStop {
			s.inboxCond.Wait()
		}
		if len(s.inbox) == 0 && s.inboxStop {
			s.inboxMu.Unlock()
			return
		}
		pkt := s.inbox[0]
		s.inbox = s.inbox[1:]
		s.inboxMu.Unlock()
		s.dispatch(pkt)
	}
}

func (s *Socket) stopDispatcher() {
	s.inboxMu.Lock()
	s.inboxStop = true
	if s.inboxCond != nil {
		s.inboxCond.Signal()
	}
	s.inboxMu.Unlock()
}

// EmitWithAck sends an event and invokes ackFn with the client's acknowledgement
// arguments. If the packet cannot be sent the pending callback is discarded and
// the send error is returned; ackFn is not invoked.
func (s *Socket) EmitWithAck(event string, ackFn func(args []any), args ...any) error {
	id := s.registerAck(ackFn)
	if err := s.conn.sendPacket(newEvent(s.namespace.name, event, args, &id)); err != nil {
		s.cancelAck(id)
		return err
	}
	return nil
}

// registerAck reserves an acknowledgement id for a callback.
func (s *Socket) registerAck(ackFn func(args []any)) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := s.ackCounter
	s.ackCounter++
	s.pendingAcks[id] = ackFn
	return id
}

// cancelAck drops a pending acknowledgement callback that will never fire.
func (s *Socket) cancelAck(id uint64) {
	s.mu.Lock()
	delete(s.pendingAcks, id)
	s.mu.Unlock()
}

// PendingAcks reports how many acknowledgements this socket is still waiting
// for. It is primarily useful in tests and for instrumentation.
func (s *Socket) PendingAcks() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pendingAcks)
}

// EmitAck sends an event and blocks until the client acknowledges it or timeout
// elapses, returning the acknowledgement arguments. On timeout it returns
// ErrAckTimeout and forgets the acknowledgement, so a client that never answers
// (or answers late) cannot make the socket accumulate pending callbacks.
func (s *Socket) EmitAck(event string, timeout time.Duration, args ...any) ([]any, error) {
	ch := make(chan []any, 1)
	id := s.registerAck(func(reply []any) { ch <- reply })
	if err := s.conn.sendPacket(newEvent(s.namespace.name, event, args, &id)); err != nil {
		s.cancelAck(id)
		return nil, err
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case reply := <-ch:
		return reply, nil
	case <-timer.C:
		s.cancelAck(id)
		return nil, ErrAckTimeout
	}
}

// ErrAckTimeout is returned by EmitAck when the client does not acknowledge the
// event within the supplied timeout.
var ErrAckTimeout = errors.New("socketio: ack timeout")

// errAckTimeout is the historical unexported alias for ErrAckTimeout.
var errAckTimeout = ErrAckTimeout

// dispatch delivers an inbound event to the registered handlers, replying with
// an ACK when the packet requested one.
func (s *Socket) dispatch(pkt Packet) {
	s.mu.Lock()
	handlers := append([]EventHandler(nil), s.handlers[pkt.EventName()]...)
	anyHandlers := make([]func(string, []any), len(s.anyHandlers))
	copy(anyHandlers, s.anyHandlers)
	s.mu.Unlock()

	for _, h := range anyHandlers {
		h(pkt.EventName(), pkt.Args())
	}

	var ackData []any
	for _, h := range handlers {
		if reply := h(pkt.Args()); reply != nil && ackData == nil {
			ackData = reply
		}
	}
	// Acknowledge only when a handler actually produced an ack (returned a
	// non-nil slice — the port's equivalent of a JavaScript handler invoking its
	// callback). Upstream never acks an event that no handler handled, nor one
	// whose handler ignored its callback; auto-acking made an ack timeout
	// unreachable and erased the difference between "handled, nothing to report"
	// and "no handler at all". A handler that wants to ack with no data returns
	// an empty (non-nil) slice.
	if pkt.ID != nil && ackData != nil {
		_ = s.conn.sendPacket(Packet{
			Type:      Ack,
			Namespace: s.namespace.name,
			ID:        pkt.ID,
			Data:      ackData,
		})
	}
}

// resolveAck invokes and clears a pending ack callback.
func (s *Socket) resolveAck(id uint64, args []any) {
	s.mu.Lock()
	fn := s.pendingAcks[id]
	delete(s.pendingAcks, id)
	s.mu.Unlock()
	if fn != nil {
		fn(args)
	}
}

// Join adds the socket to a room.
func (s *Socket) Join(rooms ...string) *Socket {
	for _, room := range rooms {
		s.namespace.join(s, room)
	}
	return s
}

// Leave removes the socket from a room.
func (s *Socket) Leave(rooms ...string) *Socket {
	for _, room := range rooms {
		s.namespace.leave(s, room)
	}
	return s
}

// Rooms returns the set of rooms the socket is currently in.
func (s *Socket) Rooms() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.rooms))
	for r := range s.rooms {
		out = append(out, r)
	}
	return out
}

// To returns a broadcast operator that targets a room, excluding this socket —
// the equivalent of socket.to(room).
func (s *Socket) To(room string) *BroadcastOperator {
	return &BroadcastOperator{
		ns:     s.namespace,
		rooms:  []string{room},
		except: map[string]struct{}{s.id: {}},
	}
}

// Broadcast returns an operator targeting every other socket in the namespace.
func (s *Socket) Broadcast() *BroadcastOperator {
	return &BroadcastOperator{
		ns:     s.namespace,
		except: map[string]struct{}{s.id: {}},
	}
}

// Disconnect closes the socket, optionally closing the underlying transport.
func (s *Socket) Disconnect(closeTransport bool) {
	s.fireDisconnecting("server namespace disconnect")
	_ = s.conn.sendPacket(Packet{Type: Disconnect, Namespace: s.namespace.name})
	s.conn.mu.Lock()
	delete(s.conn.sockets, s.namespace.name)
	s.conn.mu.Unlock()
	s.namespace.remove(s)
	s.fireDisconnect("server namespace disconnect")
	if closeTransport {
		s.conn.server.removeConn(s.conn)
	}
}

// fireDisconnecting notifies "disconnecting" handlers exactly once, while the
// socket is still a member of its rooms.
func (s *Socket) fireDisconnecting(reason string) {
	s.mu.Lock()
	if s.disconnecting {
		s.mu.Unlock()
		return
	}
	s.disconnecting = true
	handlers := append([]func(reason string){}, s.disconnectingHandlers...)
	s.mu.Unlock()
	for _, h := range handlers {
		h(reason)
	}
}

// fireDisconnect notifies disconnect handlers exactly once. It also stops the
// socket's ordered dispatcher goroutine, since no further events will arrive.
func (s *Socket) fireDisconnect(reason string) {
	s.mu.Lock()
	if s.disconnected {
		s.mu.Unlock()
		return
	}
	s.disconnected = true
	handlers := append([]func(reason string){}, s.disconnectHandlers...)
	s.mu.Unlock()
	s.stopDispatcher()
	for _, h := range handlers {
		h(reason)
	}
}

// addRoom / removeRoom maintain the socket's own room set (called by the
// namespace adapter under its lock).
func (s *Socket) addRoom(room string) {
	s.mu.Lock()
	s.rooms[room] = struct{}{}
	s.mu.Unlock()
}

func (s *Socket) removeRoom(room string) {
	s.mu.Lock()
	delete(s.rooms, room)
	s.mu.Unlock()
}
