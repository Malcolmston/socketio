package socketio

import (
	"errors"
	"sync"
	"time"

	"github.com/malcolmston/socketio/engineio"
	"github.com/malcolmston/socketio/internal/ws"
)

// conn is a single Engine.IO session (one client). It owns the active transport
// (polling or websocket), buffers outgoing packets for polling, and dispatches
// incoming Socket.IO packets to the sockets bound on this session.
type conn struct {
	sid    string
	server *Server
	// handshake describes the HTTP request that opened this session. It is set
	// once at creation and never mutated, so it needs no lock.
	handshake *Handshake

	mu     sync.Mutex
	wsConn *ws.Conn
	// wsOut is the bounded outbound queue for the websocket transport, drained
	// by a single writeLoop goroutine. Writes to a socket are not instantaneous:
	// a client that stops reading eventually fills its TCP window, and a
	// synchronous write from a room broadcast would then block the broadcasting
	// goroutine (and every socket after it in the fan-out) for as long as the
	// slow peer cared to stall. Queuing decouples the broadcast from the peer;
	// when the queue fills, the session is dropped rather than the broadcast.
	//
	// It is only ever sent on, and only ever closed, while holding mu with
	// closed still false, so a concurrent cleanup cannot close it out from under
	// a sender.
	wsOut      chan engineio.Packet
	outbuf     []engineio.Packet
	pollWaiter chan []engineio.Packet
	sockets    map[string]*Socket // socket.io sockets by namespace name
	closed     bool

	lastPong time.Time
	stopPing chan struct{}
	pingOnce sync.Once

	// pendingBin buffers a binary packet awaiting its attachment frames.
	pendingBin *pendingBinary
}

// pendingBinary accumulates the binary attachments for a BINARY_EVENT/ACK until
// all declared buffers have arrived.
type pendingBinary struct {
	packet  Packet
	buffers [][]byte
	need    int
	// bytes is the running total of buffered attachment bytes, used to bound
	// how much an unfinished binary packet can make the server hold.
	bytes int
}

// maxOutboundBuffer bounds how many Engine.IO packets a session may accumulate
// while no polling request is outstanding. A client that opens a session and
// then never polls (or stops polling while the server keeps broadcasting to it)
// would otherwise grow this buffer without limit. Once the cap is reached the
// session is treated as dead and torn down, matching the "client is too slow,
// drop it" behavior of engine.io's write buffer.
const maxOutboundBuffer = 2048

// send queues or writes an Engine.IO packet, depending on the active transport.
func (c *conn) send(p engineio.Packet) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	if c.wsOut != nil {
		// Non-blocking enqueue under mu: the channel is buffered, so this never
		// blocks, which is what keeps a stalled peer from holding the lock (or
		// the caller's goroutine) hostage.
		select {
		case c.wsOut <- p:
			c.mu.Unlock()
		default:
			// The peer has not drained maxOutboundBuffer packets. Drop the
			// session instead of stalling every other recipient of a broadcast.
			c.mu.Unlock()
			go c.server.removeConn(c)
		}
		return
	}
	if len(c.outbuf) >= maxOutboundBuffer {
		// The peer is not draining its polling transport. Drop the session
		// instead of buffering for it forever.
		c.mu.Unlock()
		go c.server.removeConn(c)
		return
	}
	c.outbuf = append(c.outbuf, p)
	waiter := c.pollWaiter
	if waiter != nil {
		c.pollWaiter = nil
		buf := c.outbuf
		c.outbuf = nil
		c.mu.Unlock()
		select {
		case waiter <- buf:
		default:
		}
		return
	}
	c.mu.Unlock()
}

// sendPacket encodes and sends a Socket.IO packet inside an Engine.IO MESSAGE.
// When the payload contains binary ([]byte) data it is sent as a BINARY_EVENT/
// BINARY_ACK followed by the raw attachment frames.
func (c *conn) sendPacket(p Packet) error {
	text, buffers, err := p.EncodeBinary()
	if err != nil {
		return err
	}
	c.send(engineio.NewMessage(text))
	for _, buf := range buffers {
		c.send(engineio.Packet{Type: engineio.Message, Binary: buf})
	}
	return nil
}

// handleEnginePacket processes one decoded Engine.IO packet.
func (c *conn) handleEnginePacket(p engineio.Packet) {
	switch p.Type {
	case engineio.Message:
		if p.Binary != nil {
			c.handleBinaryAttachment(p.Binary)
			return
		}
		c.handleMessage(p.Data)
	case engineio.Ping:
		// A client-initiated ping (rare in EIO4) is answered with a pong.
		c.send(engineio.Packet{Type: engineio.Pong, Data: p.Data})
	case engineio.Pong:
		c.mu.Lock()
		c.lastPong = time.Now()
		c.mu.Unlock()
	case engineio.Close:
		c.server.removeConn(c)
	}
}

// handleMessage decodes and dispatches a Socket.IO packet.
func (c *conn) handleMessage(data string) {
	pkt, err := DecodePacket(data)
	if err != nil {
		return
	}
	switch pkt.Type {
	case Connect:
		c.handleConnect(pkt)
	case Event:
		c.handleEvent(pkt)
	case Ack:
		c.handleAck(pkt)
	case BinaryEvent, BinaryAck:
		if pkt.attachments > 0 {
			// Wait for the attachment frames before dispatching.
			c.mu.Lock()
			c.pendingBin = &pendingBinary{packet: pkt, need: pkt.attachments}
			c.mu.Unlock()
			return
		}
		if pkt.Type == BinaryEvent {
			c.handleEvent(pkt)
		} else {
			c.handleAck(pkt)
		}
	case Disconnect:
		c.handleDisconnect(pkt)
	}
}

// handleBinaryAttachment collects a binary buffer for a pending binary packet
// and dispatches the packet once all attachments have arrived.
func (c *conn) handleBinaryAttachment(buf []byte) {
	c.mu.Lock()
	pb := c.pendingBin
	if pb == nil {
		c.mu.Unlock()
		return
	}
	if pb.bytes+len(buf) > c.server.opts.MaxPayload {
		// A BINARY_EVENT may declare up to MaxAttachments buffers; cap the
		// total bytes held for it so a client cannot make the server buffer
		// unbounded data by declaring many attachments and dribbling them in.
		c.pendingBin = nil
		c.mu.Unlock()
		return
	}
	pb.bytes += len(buf)
	pb.buffers = append(pb.buffers, buf)
	if len(pb.buffers) < pb.need {
		c.mu.Unlock()
		return
	}
	c.pendingBin = nil
	c.mu.Unlock()

	pkt := pb.packet
	data, err := reconstruct(pkt.Data, pb.buffers)
	if err != nil {
		// A malformed binary frame (an out-of-range placeholder) is dropped
		// rather than dispatched with a raw placeholder object in its payload.
		return
	}
	pkt.Data = data
	pkt.attachments = 0
	if pkt.Type == BinaryEvent {
		pkt.Type = Event
		c.handleEvent(pkt)
	} else {
		pkt.Type = Ack
		c.handleAck(pkt)
	}
}

func (c *conn) handleConnect(pkt Packet) {
	// A repeated CONNECT for a namespace this session is already joined to is a
	// no-op: previously each one minted a fresh Socket and overwrote the map
	// entry, orphaning the old socket in the namespace adapter (and letting a
	// client grow that adapter without bound by spamming CONNECT).
	c.mu.Lock()
	existing := c.sockets[pkt.Namespace]
	c.mu.Unlock()
	if existing != nil {
		_ = c.sendPacket(Packet{
			Type:      Connect,
			Namespace: pkt.Namespace,
			Data:      map[string]any{"sid": existing.id},
		})
		return
	}

	ns := c.server.namespace(pkt.Namespace)
	if ns == nil {
		_ = c.sendPacket(Packet{
			Type:      ConnectError,
			Namespace: pkt.Namespace,
			Data:      map[string]any{"message": "Invalid namespace"},
		})
		return
	}
	socket := ns.add(c, pkt.Namespace, pkt.Data)

	// Run connection middleware. An explicit rejection (next(err)) answers with a
	// CONNECT_ERROR; a middleware that returns without calling next at all leaves
	// the handshake unanswered, exactly as upstream does — the client times out
	// and no socket is admitted. Never fall through to acceptance in either case.
	if err := ns.runMiddleware(socket); err != nil {
		socket.stopDispatcher()
		ns.remove(socket)
		if errors.Is(err, errMiddlewareSilent) {
			// Deny silently: send nothing, so the client's connect attempt times
			// out rather than being admitted.
			return
		}
		_ = c.sendPacket(Packet{
			Type:      ConnectError,
			Namespace: pkt.Namespace,
			Data:      map[string]any{"message": err.Error()},
		})
		return
	}

	// Re-check under the lock: two CONNECT packets for the same namespace can be
	// in flight concurrently (polling POSTs are independent HTTP requests), and
	// the loser must not orphan its socket in the adapter.
	c.mu.Lock()
	if won := c.sockets[pkt.Namespace]; won != nil {
		c.mu.Unlock()
		ns.remove(socket)
		return
	}
	c.sockets[pkt.Namespace] = socket
	c.mu.Unlock()

	// The client considers itself connected on receiving this CONNECT packet.
	_ = c.sendPacket(Packet{
		Type:      Connect,
		Namespace: pkt.Namespace,
		Data:      map[string]any{"sid": socket.id},
	})
	ns.fireConnection(socket)
}

func (c *conn) handleEvent(pkt Packet) {
	c.mu.Lock()
	socket := c.sockets[pkt.Namespace]
	c.mu.Unlock()
	if socket != nil {
		// Hand the event to the socket's ordered dispatcher rather than spawning
		// a goroutine per packet: Socket.IO guarantees in-order delivery within a
		// socket, and enqueue never blocks this read loop, so a handler that
		// blocks on EmitAck still lets the loop read the ACK it awaits.
		socket.enqueue(pkt)
	}
}

func (c *conn) handleAck(pkt Packet) {
	c.mu.Lock()
	socket := c.sockets[pkt.Namespace]
	c.mu.Unlock()
	if socket != nil && pkt.ID != nil {
		socket.resolveAck(*pkt.ID, pkt.Args())
	}
}

func (c *conn) handleDisconnect(pkt Packet) {
	c.mu.Lock()
	socket := c.sockets[pkt.Namespace]
	delete(c.sockets, pkt.Namespace)
	c.mu.Unlock()
	if socket != nil {
		socket.fireDisconnecting("client namespace disconnect")
		socket.namespace.remove(socket)
		socket.fireDisconnect("client namespace disconnect")
	}
}

// startPing launches the server heartbeat for this session.
func (c *conn) startPing() {
	c.pingOnce.Do(func() {
		c.stopPing = make(chan struct{})
		go c.pingLoop()
	})
}

func (c *conn) pingLoop() {
	interval := c.server.opts.PingInterval
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-c.stopPing:
			return
		case <-ticker.C:
			c.mu.Lock()
			last := c.lastPong
			c.mu.Unlock()
			if time.Since(last) > c.server.opts.PingInterval+c.server.opts.PingTimeout {
				c.server.removeConn(c)
				return
			}
			c.send(engineio.Packet{Type: engineio.Ping})
		}
	}
}

// cleanup disconnects all sockets and closes the transport.
func (c *conn) cleanup() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	sockets := make([]*Socket, 0, len(c.sockets))
	for _, s := range c.sockets {
		sockets = append(sockets, s)
	}
	c.sockets = map[string]*Socket{}
	wsc := c.wsConn
	out := c.wsOut
	c.wsOut = nil
	waiter := c.pollWaiter
	c.pollWaiter = nil
	stop := c.stopPing
	c.mu.Unlock()

	for _, s := range sockets {
		s.fireDisconnecting("transport close")
		s.namespace.remove(s)
		s.fireDisconnect("transport close")
	}
	if out != nil {
		// Hand the close over to writeLoop: it flushes whatever is still queued
		// (notably the DISCONNECT packet a server-side disconnect just sent)
		// before tearing the socket down, then closes wsc and exits.
		close(out)
	} else if wsc != nil {
		_ = wsc.Close()
	}
	if waiter != nil {
		select {
		case waiter <- nil:
		default:
		}
	}
	if stop != nil {
		close(stop)
	}
}

// attachWebSocket switches this session to the websocket transport, releasing
// any pending polling request and flushing buffered packets. It reports whether
// the session accepted the transport: a session that was torn down while the
// upgrade probe was in flight returns false, and the caller must close wsc
// itself — otherwise the connection (and the readLoop goroutine pumping it)
// would be retained by a session nothing can reach.
func (c *conn) attachWebSocket(wsc *ws.Conn) bool {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return false
	}
	c.wsConn = wsc
	c.wsOut = make(chan engineio.Packet, maxOutboundBuffer)
	out := c.wsOut
	waiter := c.pollWaiter
	c.pollWaiter = nil
	buf := c.outbuf
	c.outbuf = nil
	// Packets buffered by the polling transport are re-queued ahead of anything
	// new, preserving order across the upgrade. Encoding happens in writeLoop so
	// binary attachments keep their native binary frames — flushing them here
	// with WriteText used to base64-mangle every buffered binary packet.
	for _, p := range buf {
		select {
		case out <- p:
		default:
		}
	}
	c.mu.Unlock()

	go c.writeLoop(wsc, out)

	if waiter != nil {
		// Release the outstanding poll with a noop so it returns promptly.
		select {
		case waiter <- []engineio.Packet{{Type: engineio.Noop}}:
		default:
		}
	}
	return true
}

// writeLoop serializes all websocket writes for a session. It owns wsc: it
// drains out (which cleanup closes) and then closes the connection, so queued
// packets are flushed before the transport goes away.
func (c *conn) writeLoop(wsc *ws.Conn, out chan engineio.Packet) {
	defer wsc.Close()
	for p := range out {
		var err error
		if p.Binary != nil {
			// Binary attachments travel as native websocket binary frames.
			err = wsc.WriteMessage(ws.BinaryMessage, p.Binary)
		} else {
			err = wsc.WriteText(p.Encode())
		}
		if err != nil {
			// The transport is gone; tear the session down and stop writing.
			// cleanup closes out, which this loop has already abandoned.
			go c.server.removeConn(c)
			return
		}
	}
}

// readLoop pumps websocket frames into the engine.io dispatcher until close.
func (c *conn) readLoop(wsc *ws.Conn) {
	for {
		mt, data, err := wsc.ReadMessage()
		if err != nil {
			break
		}
		if mt == ws.BinaryMessage {
			c.handleBinaryAttachment(data)
			continue
		}
		if mt != ws.TextMessage {
			continue
		}
		p, err := engineio.Decode(string(data))
		if err != nil {
			continue
		}
		c.handleEnginePacket(p)
	}
	c.server.removeConn(c)
}
