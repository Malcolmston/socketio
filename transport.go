package socketio

import (
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/malcolmston/socketio/engineio"
	"github.com/malcolmston/socketio/internal/ws"
)

// handshake is the Engine.IO OPEN packet payload.
type handshake struct {
	Sid          string   `json:"sid"`
	Upgrades     []string `json:"upgrades"`
	PingInterval int      `json:"pingInterval"`
	PingTimeout  int      `json:"pingTimeout"`
	MaxPayload   int      `json:"maxPayload"`
}

func (s *Server) openPacket(c *conn, upgrades []string) engineio.Packet {
	h := handshake{
		Sid:          c.sid,
		Upgrades:     upgrades,
		PingInterval: int(s.opts.PingInterval / time.Millisecond),
		PingTimeout:  int(s.opts.PingTimeout / time.Millisecond),
		MaxPayload:   s.opts.MaxPayload,
	}
	data, _ := json.Marshal(h)
	return engineio.NewOpen(string(data))
}

// ---- HTTP long-polling transport -------------------------------------------

func writePayload(w http.ResponseWriter, packets []engineio.Packet) {
	w.Header().Set("Content-Type", "text/plain; charset=UTF-8")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, engineio.EncodePayload(packets))
}

func (s *Server) handlePolling(w http.ResponseWriter, r *http.Request, sid string) {
	if sid == "" {
		// New session handshake.
		c := s.newConnFrom(r)
		if c == nil {
			// Session cap reached; refuse rather than grow the session map.
			http.Error(w, "too many sessions", http.StatusServiceUnavailable)
			return
		}
		c.startPing()
		writePayload(w, []engineio.Packet{s.openPacket(c, []string{"websocket"})})
		return
	}

	c := s.getConn(sid)
	if c == nil {
		// Unknown session id.
		w.Header().Set("Content-Type", "text/plain; charset=UTF-8")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, `{"code":1,"message":"Session ID unknown"}`)
		return
	}

	switch r.Method {
	case http.MethodGet:
		s.pollGet(w, r, c)
	case http.MethodPost:
		s.pollPost(w, r, c)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// pollGet holds the request open until packets are available (long-polling).
func (s *Server) pollGet(w http.ResponseWriter, r *http.Request, c *conn) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		writePayload(w, []engineio.Packet{{Type: engineio.Close}})
		return
	}
	if len(c.outbuf) > 0 {
		buf := c.outbuf
		c.outbuf = nil
		c.mu.Unlock()
		writePayload(w, buf)
		return
	}
	waiter := make(chan []engineio.Packet, 1)
	c.pollWaiter = waiter
	c.mu.Unlock()

	select {
	case buf := <-waiter:
		writePayload(w, buf)
	case <-time.After(s.opts.PingInterval + s.opts.PingTimeout):
		c.clearWaiter(waiter)
		writePayload(w, nil)
	case <-r.Context().Done():
		c.clearWaiter(waiter)
	}
}

func (c *conn) clearWaiter(waiter chan []engineio.Packet) {
	c.mu.Lock()
	if c.pollWaiter == waiter {
		c.pollWaiter = nil
	}
	c.mu.Unlock()
}

// pollPost ingests packets posted by the client.
func (s *Server) pollPost(w http.ResponseWriter, r *http.Request, c *conn) {
	body, err := io.ReadAll(io.LimitReader(r.Body, int64(s.opts.MaxPayload)))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	packets, err := engineio.DecodePayload(string(body))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	for _, p := range packets {
		c.handleEnginePacket(p)
	}
	w.Header().Set("Content-Type", "text/plain; charset=UTF-8")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, "ok")
}

// ---- WebSocket transport ---------------------------------------------------

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request, sid string) {
	if !ws.IsWebSocketUpgrade(r) {
		http.Error(w, "expected websocket upgrade", http.StatusBadRequest)
		return
	}
	wsc, err := ws.Upgrade(w, r)
	if err != nil {
		return
	}

	if sid == "" {
		// Fresh websocket connection: create the session and send OPEN.
		c := s.newConnFrom(r)
		if c == nil {
			_ = wsc.Close()
			return
		}
		if !c.attachWebSocket(wsc) {
			_ = wsc.Close()
			return
		}
		c.send(s.openPacket(c, []string{}))
		c.startPing()
		c.readLoop(wsc)
		return
	}

	// Upgrade an existing polling session.
	c := s.getConn(sid)
	if c == nil {
		_ = wsc.Close()
		return
	}
	if !s.probe(wsc, c) {
		_ = wsc.Close()
		return
	}
	// attachWebSocket reports false when the session was torn down while the
	// probe was in flight; without that check the websocket (and this readLoop)
	// would be owned by a session no longer in the server's map.
	if !c.attachWebSocket(wsc) {
		_ = wsc.Close()
		return
	}
	c.readLoop(wsc)
}

// probe performs the Engine.IO upgrade handshake: the client sends "2probe"
// (ping probe), the server replies "3probe" (pong probe), then the client sends
// "5" (upgrade). It returns true once the upgrade packet is received.
func (s *Server) probe(wsc *ws.Conn, c *conn) bool {
	for {
		mt, data, err := wsc.ReadMessage()
		if err != nil || mt != ws.TextMessage {
			return false
		}
		p, err := engineio.Decode(string(data))
		if err != nil {
			continue
		}
		switch p.Type {
		case engineio.Ping:
			if p.Data == "probe" {
				_ = wsc.WriteText(engineio.Packet{Type: engineio.Pong, Data: "probe"}.Encode())
			}
		case engineio.Upgrade:
			return true
		default:
			// Handle any real packets that arrive during the probe.
			c.handleEnginePacket(p)
		}
	}
}
