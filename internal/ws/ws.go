// Package ws is a minimal, dependency-free RFC 6455 WebSocket implementation,
// sufficient to carry Engine.IO/Socket.IO traffic. It provides both sides of a
// connection: a server that upgrades an incoming net/http request and a client
// that dials a remote endpoint. It supports the opening handshake, fragmented
// data messages, and the control frames (ping/pong/close). Only what Socket.IO
// needs is implemented; it is not a general-purpose WebSocket library, and it is
// an internal package with no compatibility guarantees for outside importers.
//
// It exists so that the surrounding module can offer WebSocket transport using
// nothing but the Go standard library — no gorilla/websocket, no nhooyr, no cgo.
// The socketio server calls Upgrade to turn a hijacked HTTP connection into a
// *Conn; the client package calls Dial to obtain a *Conn to a remote server.
// Both then exchange Engine.IO frames through the same small message API, so the
// rest of the stack is transport-agnostic.
//
// The server handshake works by validating the Upgrade/Connection headers and
// Sec-WebSocket-Version (13), hijacking the underlying net.Conn via
// http.Hijacker, and replying with the 101 Switching Protocols response whose
// Sec-WebSocket-Accept is the SHA-1/base64 digest of the client key plus the
// RFC 6455 GUID. IsWebSocketUpgrade reports whether a request is a WebSocket
// upgrade so callers can branch before attempting Upgrade. Dial performs the
// mirror image: it opens a TCP connection (wrapped in TLS for a wss:// or
// https:// URL — see DialTLS for a custom *tls.Config), sends the GET upgrade
// request with a random key, and verifies the server's accept value.
//
// Reads are bounded. A frame header declares its payload length before the
// payload arrives, so an unbounded reader would allocate whatever a peer asks
// for; ReadMessage instead rejects any frame — or any fragmented message whose
// reassembly would grow — beyond the connection's read limit
// (DefaultMaxMessageSize, adjustable with SetReadLimit) with ErrMessageTooLarge.
// Control frames are held to the RFC's 125-byte, never-fragmented rule, and a
// server-side Conn rejects the unmasked client frames RFC 6455 §5.1 forbids.
//
// Messages are exchanged with ReadMessage and WriteMessage (or the WriteText
// convenience), which speak whole logical messages rather than raw frames.
// ReadMessage reassembles fragmented messages, transparently answers ping
// control frames with a pong, and responds to a close frame by echoing it and
// returning ErrClosed. MessageType distinguishes TextMessage from
// BinaryMessage, which matches Engine.IO's split between string packets and
// binary attachments. Per RFC 6455, client-originated frames are masked with a
// fresh random key (Conn tracks whether it is a client), while server frames are
// sent unmasked.
//
// A Conn is safe for one concurrent reader and one concurrent writer: writes are
// serialized by an internal mutex and the closed flag is atomic, so a reader
// that observes a close while a writer is mid-flight will not race. It is not
// safe to call ReadMessage from two goroutines at once. Close sends a normal
// (1000) close frame and tears down the transport; subsequent writes return
// ErrClosed. Notably absent, because Socket.IO does not require them, are
// permessage-deflate compression, subprotocol negotiation, and read/write
// deadlines — callers that need timeouts should manage them on the underlying
// connection or via request context.
package ws

import (
	"bufio"
	"crypto/rand"
	"crypto/sha1"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	neturl "net/url"
	"strings"
	"sync"
	"sync/atomic"
)

// guid is the RFC 6455 magic value appended to the client key.
const guid = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

// Opcode values (RFC 6455 §5.2).
const (
	opContinuation = 0x0
	opText         = 0x1
	opBinary       = 0x2
	opClose        = 0x8
	opPing         = 0x9
	opPong         = 0xA
)

// MessageType distinguishes text and binary data messages.
type MessageType int

const (
	// TextMessage is a UTF-8 text message.
	TextMessage MessageType = iota
	// BinaryMessage is a binary message.
	BinaryMessage
)

// ErrClosed is returned once the connection has been closed.
var ErrClosed = errors.New("ws: connection closed")

// ErrMessageTooLarge is returned by ReadMessage when a peer announces a frame,
// or accumulates a fragmented message, larger than the connection's read limit.
var ErrMessageTooLarge = errors.New("ws: message exceeds read limit")

// DefaultMaxMessageSize is the read limit a Conn starts with: the largest
// single frame, and the largest reassembled fragmented message, that
// ReadMessage will accept. It matters because a frame header declares its
// payload length in up to 64 bits *before* any of that payload is sent, so an
// unbounded reader would allocate whatever an unauthenticated peer asks for
// from 14 bytes of input. It defaults to 1 MiB, matching engine.io's
// maxHttpBufferSize default; raise it with Conn.SetReadLimit.
const DefaultMaxMessageSize = 1 << 20

// maxControlFramePayload is the RFC 6455 §5.5 limit on control frame payloads.
const maxControlFramePayload = 125

// Conn is a WebSocket connection (server- or client-side).
type Conn struct {
	conn net.Conn
	br   *bufio.Reader
	bw   *bufio.Writer

	// client is true for connections created by Dial; client-to-server frames
	// must be masked per RFC 6455.
	client bool

	// readLimit bounds the payload of a single frame and the total size of a
	// reassembled fragmented message. Read only by the (single) reader
	// goroutine, so it needs no lock beyond the atomic store in SetReadLimit.
	readLimit atomic.Int64

	writeMu sync.Mutex
	closeMu sync.Mutex
	// closed is read by writeFrame (under writeMu) and set by closeConn (under
	// closeMu); it must be atomic so those two paths don't race when a reader
	// closes the connection while a writer is mid-flight.
	closed atomic.Bool
}

// IsWebSocketUpgrade reports whether r is a WebSocket upgrade request.
func IsWebSocketUpgrade(r *http.Request) bool {
	return tokenListContains(r.Header.Get("Connection"), "upgrade") &&
		strings.EqualFold(r.Header.Get("Upgrade"), "websocket")
}

// Upgrade completes the server handshake and hijacks the connection, returning
// a *Conn ready for ReadMessage/WriteMessage.
func Upgrade(w http.ResponseWriter, r *http.Request) (*Conn, error) {
	if !IsWebSocketUpgrade(r) {
		return nil, errors.New("ws: not a websocket upgrade request")
	}
	key := r.Header.Get("Sec-WebSocket-Key")
	if key == "" {
		return nil, errors.New("ws: missing Sec-WebSocket-Key")
	}
	if v := r.Header.Get("Sec-WebSocket-Version"); v != "13" {
		return nil, fmt.Errorf("ws: unsupported version %q", v)
	}
	hj, ok := w.(http.Hijacker)
	if !ok {
		return nil, errors.New("ws: response writer does not support hijacking")
	}
	conn, rw, err := hj.Hijack()
	if err != nil {
		return nil, err
	}

	accept := acceptKey(key)
	resp := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + accept + "\r\n\r\n"
	if _, err := rw.WriteString(resp); err != nil {
		conn.Close()
		return nil, err
	}
	if err := rw.Flush(); err != nil {
		conn.Close()
		return nil, err
	}
	return newConn(conn, rw.Reader, rw.Writer, false), nil
}

// newConn builds a Conn with the default read limit applied.
func newConn(nc net.Conn, br *bufio.Reader, bw *bufio.Writer, client bool) *Conn {
	c := &Conn{conn: nc, br: br, bw: bw, client: client}
	c.readLimit.Store(DefaultMaxMessageSize)
	return c
}

// SetReadLimit sets the maximum payload size, in bytes, that ReadMessage will
// accept for a single frame or for a fully reassembled fragmented message.
// A value <= 0 restores DefaultMaxMessageSize. Exceeding the limit fails the
// read with ErrMessageTooLarge instead of allocating what the peer asked for.
func (c *Conn) SetReadLimit(n int64) {
	if n <= 0 {
		n = DefaultMaxMessageSize
	}
	c.readLimit.Store(n)
}

// ReadLimit returns the current read limit in bytes.
func (c *Conn) ReadLimit() int64 { return c.readLimit.Load() }

func acceptKey(key string) string {
	h := sha1.New()
	h.Write([]byte(key + guid))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

// ReadMessage reads the next data message, transparently answering ping and
// close control frames. It reassembles fragmented messages.
func (c *Conn) ReadMessage() (MessageType, []byte, error) {
	var (
		payload []byte
		msgType MessageType
		started bool
	)
	limit := c.readLimit.Load()
	if limit <= 0 {
		// Zero-valued Conn (constructed outside Upgrade/Dial): fall back to the
		// default rather than rejecting every frame.
		limit = DefaultMaxMessageSize
	}
	for {
		fin, opcode, data, err := c.readFrame(limit)
		if err != nil {
			return 0, nil, err
		}
		// Guard the reassembly buffer too: a peer can otherwise stream an
		// unbounded number of individually-legal continuation frames.
		if opcode == opText || opcode == opBinary || opcode == opContinuation {
			if int64(len(payload))+int64(len(data)) > limit {
				return 0, nil, ErrMessageTooLarge
			}
		}
		switch opcode {
		case opPing:
			if err := c.writeFrame(opPong, data); err != nil {
				return 0, nil, err
			}
			continue
		case opPong:
			continue
		case opClose:
			_ = c.writeFrame(opClose, data)
			_ = c.closeConn()
			return 0, nil, ErrClosed
		case opText, opBinary:
			if started {
				return 0, nil, errors.New("ws: unexpected non-continuation frame")
			}
			started = true
			if opcode == opText {
				msgType = TextMessage
			} else {
				msgType = BinaryMessage
			}
			payload = append(payload, data...)
		case opContinuation:
			if !started {
				return 0, nil, errors.New("ws: unexpected continuation frame")
			}
			payload = append(payload, data...)
		default:
			return 0, nil, fmt.Errorf("ws: unknown opcode 0x%x", opcode)
		}
		if fin {
			return msgType, payload, nil
		}
	}
}

// readFrame reads a single frame, unmasking the client payload. limit bounds
// the declared payload length: the length arrives before the payload does, so
// it must be validated before it is used to size an allocation.
func (c *Conn) readFrame(limit int64) (fin bool, opcode byte, payload []byte, err error) {
	var header [2]byte
	if _, err = io.ReadFull(c.br, header[:]); err != nil {
		return
	}
	fin = header[0]&0x80 != 0
	opcode = header[0] & 0x0f
	masked := header[1]&0x80 != 0
	length := uint64(header[1] & 0x7f)

	switch length {
	case 126:
		var ext [2]byte
		if _, err = io.ReadFull(c.br, ext[:]); err != nil {
			return
		}
		length = uint64(binary.BigEndian.Uint16(ext[:]))
	case 127:
		var ext [8]byte
		if _, err = io.ReadFull(c.br, ext[:]); err != nil {
			return
		}
		length = binary.BigEndian.Uint64(ext[:])
	}

	isControl := opcode&0x08 != 0
	if isControl {
		// RFC 6455 §5.5: control frames carry at most 125 bytes and must not be
		// fragmented. Enforcing this also keeps a ping echo from being used to
		// make the server buffer and reflect a large payload.
		if length > maxControlFramePayload {
			err = errors.New("ws: control frame payload too long")
			return
		}
		if !fin {
			err = errors.New("ws: fragmented control frame")
			return
		}
	} else if length > uint64(limit) {
		err = ErrMessageTooLarge
		return
	}

	// RFC 6455 §5.1: a server MUST close the connection on an unmasked frame
	// from a client. Accepting one lets an attacker smuggle a WebSocket frame
	// through a naive intermediary that only ever sees plain HTTP.
	if !c.client && !masked {
		err = errors.New("ws: unmasked client frame")
		return
	}

	var maskKey [4]byte
	if masked {
		if _, err = io.ReadFull(c.br, maskKey[:]); err != nil {
			return
		}
	}

	if length > 0 {
		payload = make([]byte, length)
		if _, err = io.ReadFull(c.br, payload); err != nil {
			return
		}
		if masked {
			for i := range payload {
				payload[i] ^= maskKey[i%4]
			}
		}
	}
	return fin, opcode, payload, nil
}

// WriteMessage writes a complete data message (single, unmasked frame).
func (c *Conn) WriteMessage(t MessageType, data []byte) error {
	op := byte(opText)
	if t == BinaryMessage {
		op = opBinary
	}
	return c.writeFrame(op, data)
}

// WriteText is a convenience for sending a text message.
func (c *Conn) WriteText(s string) error {
	return c.WriteMessage(TextMessage, []byte(s))
}

// writeFrame writes a single unmasked frame with FIN set.
func (c *Conn) writeFrame(opcode byte, data []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.closed.Load() {
		return ErrClosed
	}

	var header []byte
	b0 := byte(0x80) | opcode // FIN + opcode
	maskBit := byte(0)
	if c.client {
		maskBit = 0x80
	}
	n := len(data)
	switch {
	case n < 126:
		header = []byte{b0, maskBit | byte(n)}
	case n < 65536:
		header = []byte{b0, maskBit | 126, 0, 0}
		binary.BigEndian.PutUint16(header[2:], uint16(n))
	default:
		header = make([]byte, 10)
		header[0] = b0
		header[1] = maskBit | 127
		binary.BigEndian.PutUint64(header[2:], uint64(n))
	}
	if _, err := c.bw.Write(header); err != nil {
		return err
	}
	if c.client {
		// Mask the payload with a fresh random key (RFC 6455 §5.3).
		var key [4]byte
		if _, err := rand.Read(key[:]); err != nil {
			return err
		}
		if _, err := c.bw.Write(key[:]); err != nil {
			return err
		}
		masked := make([]byte, n)
		for i := range data {
			masked[i] = data[i] ^ key[i%4]
		}
		if n > 0 {
			if _, err := c.bw.Write(masked); err != nil {
				return err
			}
		}
		return c.bw.Flush()
	}
	if n > 0 {
		if _, err := c.bw.Write(data); err != nil {
			return err
		}
	}
	return c.bw.Flush()
}

// Dial opens a client WebSocket connection to a ws://, wss://, http:// or
// https:// URL. The returned Conn masks outgoing frames as required for
// clients. A wss:// or https:// URL is dialed over TLS, verified against the
// system roots using the URL host as the server name; use DialTLS to supply a
// custom *tls.Config.
func Dial(rawURL string, header http.Header) (*Conn, error) {
	return DialTLS(rawURL, header, nil)
}

// DialTLS is Dial with an explicit *tls.Config, used for secure (wss://)
// endpoints that need a custom root pool, client certificate, or ServerName.
// cfg is cloned before use and its ServerName is filled in from the URL host
// when empty. It is ignored for plaintext (ws://) URLs.
func DialTLS(rawURL string, header http.Header, cfg *tls.Config) (*Conn, error) {
	u, err := neturl.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	secure := u.Scheme == "wss" || u.Scheme == "https"
	host := u.Host
	if u.Port() == "" {
		if secure {
			host += ":443"
		} else {
			host += ":80"
		}
	}
	conn, err := net.Dial("tcp", host)
	if err != nil {
		return nil, err
	}
	if secure {
		// Without this a wss:// URL silently spoke cleartext to port 443 —
		// the handshake either hung or failed, and any traffic that did flow
		// would have been unencrypted.
		tcfg := &tls.Config{MinVersion: tls.VersionTLS12}
		if cfg != nil {
			tcfg = cfg.Clone()
		}
		if tcfg.ServerName == "" {
			tcfg.ServerName = u.Hostname()
		}
		tlsConn := tls.Client(conn, tcfg)
		if err := tlsConn.Handshake(); err != nil {
			conn.Close()
			return nil, err
		}
		conn = tlsConn
	}

	var keyRaw [16]byte
	if _, err := rand.Read(keyRaw[:]); err != nil {
		conn.Close()
		return nil, err
	}
	key := base64.StdEncoding.EncodeToString(keyRaw[:])

	reqPath := u.RequestURI()
	var b strings.Builder
	b.WriteString("GET " + reqPath + " HTTP/1.1\r\n")
	b.WriteString("Host: " + u.Host + "\r\n")
	b.WriteString("Upgrade: websocket\r\n")
	b.WriteString("Connection: Upgrade\r\n")
	b.WriteString("Sec-WebSocket-Key: " + key + "\r\n")
	b.WriteString("Sec-WebSocket-Version: 13\r\n")
	for k, vs := range header {
		for _, v := range vs {
			b.WriteString(k + ": " + v + "\r\n")
		}
	}
	b.WriteString("\r\n")
	if _, err := conn.Write([]byte(b.String())); err != nil {
		conn.Close()
		return nil, err
	}

	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, &http.Request{Method: "GET"})
	if err != nil {
		conn.Close()
		return nil, err
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		conn.Close()
		return nil, fmt.Errorf("ws: unexpected handshake status %d", resp.StatusCode)
	}
	if got := resp.Header.Get("Sec-WebSocket-Accept"); got != acceptKey(key) {
		conn.Close()
		return nil, errors.New("ws: bad Sec-WebSocket-Accept")
	}
	return newConn(conn, br, bufio.NewWriter(conn), true), nil
}

// Close sends a close frame and tears down the connection.
func (c *Conn) Close() error {
	_ = c.writeFrame(opClose, []byte{0x03, 0xe8}) // 1000 normal closure
	return c.closeConn()
}

func (c *Conn) closeConn() error {
	c.closeMu.Lock()
	defer c.closeMu.Unlock()
	if c.closed.Load() {
		return nil
	}
	c.closed.Store(true)
	return c.conn.Close()
}

// RemoteAddr returns the underlying connection's remote address.
func (c *Conn) RemoteAddr() net.Addr { return c.conn.RemoteAddr() }

// tokenListContains reports whether a comma-separated header value contains
// token (case-insensitive).
func tokenListContains(header, token string) bool {
	for _, part := range strings.Split(header, ",") {
		if strings.EqualFold(strings.TrimSpace(part), token) {
			return true
		}
	}
	return false
}
