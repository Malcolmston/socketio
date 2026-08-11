package ws

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"io"
	"net"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// buildFrame constructs a raw RFC 6455 frame. When mask is true the payload is
// masked with a random key (as a client must do).
func buildFrame(fin bool, opcode byte, payload []byte, mask bool) []byte {
	var b []byte
	b0 := opcode
	if fin {
		b0 |= 0x80
	}
	b = append(b, b0)

	maskBit := byte(0)
	if mask {
		maskBit = 0x80
	}
	n := len(payload)
	switch {
	case n < 126:
		b = append(b, maskBit|byte(n))
	case n < 65536:
		ext := []byte{0, 0}
		binary.BigEndian.PutUint16(ext, uint16(n))
		b = append(b, maskBit|126)
		b = append(b, ext...)
	default:
		ext := make([]byte, 8)
		binary.BigEndian.PutUint64(ext, uint64(n))
		b = append(b, maskBit|127)
		b = append(b, ext...)
	}

	if mask {
		var key [4]byte
		rand.Read(key[:])
		b = append(b, key[:]...)
		for i := range payload {
			b = append(b, payload[i]^key[i%4])
		}
	} else {
		b = append(b, payload...)
	}
	return b
}

// fakeNetConn is a minimal net.Conn used to satisfy Conn.conn in frame-level
// tests; only Close is meaningfully exercised.
type fakeNetConn struct{ closed bool }

func (f *fakeNetConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (f *fakeNetConn) Write(b []byte) (int, error)      { return len(b), nil }
func (f *fakeNetConn) Close() error                     { f.closed = true; return nil }
func (f *fakeNetConn) LocalAddr() net.Addr              { return dummyAddr{} }
func (f *fakeNetConn) RemoteAddr() net.Addr             { return dummyAddr{} }
func (f *fakeNetConn) SetDeadline(time.Time) error      { return nil }
func (f *fakeNetConn) SetReadDeadline(time.Time) error  { return nil }
func (f *fakeNetConn) SetWriteDeadline(time.Time) error { return nil }

type dummyAddr struct{}

func (dummyAddr) Network() string { return "fake" }
func (dummyAddr) String() string  { return "fake-addr" }

// newReadConn builds a Conn that reads the given raw bytes and buffers all
// writes into the returned buffer.
func newReadConn(raw []byte) (*Conn, *bytes.Buffer) {
	var out bytes.Buffer
	c := &Conn{
		conn: &fakeNetConn{},
		br:   bufio.NewReader(bytes.NewReader(raw)),
		bw:   bufio.NewWriter(&out),
	}
	return c, &out
}

// newServerReadConn builds a client-side Conn that reads the given raw bytes.
// Server-to-client frames are unmasked, so frames produced by a server (or by
// the test helper with mask=false) must be read through a client-side Conn —
// readFrame enforces RFC 6455's rule that a *server* reject unmasked frames.
func newServerReadConn(raw []byte) (*Conn, *bytes.Buffer) {
	c, out := newReadConn(raw)
	c.client = true
	return c, out
}

// TestReadFrameUnmasked checks that a client accepts the unmasked frames a
// server sends. (A server rejecting an unmasked *client* frame is covered by
// TestServerRejectsUnmaskedClientFrame in ws_limits_test.go.)
func TestReadFrameUnmasked(t *testing.T) {
	raw := buildFrame(true, opText, []byte("hello"), false)
	c, _ := newServerReadConn(raw)
	fin, opcode, payload, err := c.readFrame(DefaultMaxMessageSize)
	if err != nil {
		t.Fatal(err)
	}
	if !fin || opcode != opText || string(payload) != "hello" {
		t.Fatalf("readFrame = fin=%v op=%x payload=%q", fin, opcode, payload)
	}
}

func TestReadFrameMaskedUnmasks(t *testing.T) {
	raw := buildFrame(true, opBinary, []byte{0xde, 0xad, 0xbe, 0xef}, true)
	c, _ := newReadConn(raw)
	fin, opcode, payload, err := c.readFrame(DefaultMaxMessageSize)
	if err != nil {
		t.Fatal(err)
	}
	if !fin || opcode != opBinary || !bytes.Equal(payload, []byte{0xde, 0xad, 0xbe, 0xef}) {
		t.Fatalf("readFrame = fin=%v op=%x payload=%x", fin, opcode, payload)
	}
}

func TestReadFrameExtendedLength16(t *testing.T) {
	msg := bytes.Repeat([]byte("A"), 300) // 126 path (16-bit length)
	c, _ := newServerReadConn(buildFrame(true, opText, msg, false))
	_, _, payload, err := c.readFrame(DefaultMaxMessageSize)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(payload, msg) {
		t.Fatalf("payload len = %d, want %d", len(payload), len(msg))
	}
}

func TestReadFrameExtendedLength64(t *testing.T) {
	msg := bytes.Repeat([]byte("B"), 70000) // 127 path (64-bit length)
	c, _ := newServerReadConn(buildFrame(true, opText, msg, false))
	_, _, payload, err := c.readFrame(DefaultMaxMessageSize)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(payload, msg) {
		t.Fatalf("payload len = %d, want %d", len(payload), len(msg))
	}
}

func TestReadFrameTruncated(t *testing.T) {
	// Header claims 5 bytes but only 2 are present.
	raw := buildFrame(true, opText, []byte("hello"), false)[:4]
	c, _ := newServerReadConn(raw)
	if _, _, _, err := c.readFrame(DefaultMaxMessageSize); err == nil {
		t.Fatal("expected error on truncated frame")
	}
}

func TestWriteFrameServerUnmasked(t *testing.T) {
	c, out := newReadConn(nil)
	if err := c.WriteMessage(TextMessage, []byte("hi")); err != nil {
		t.Fatal(err)
	}
	got := out.Bytes()
	// FIN + text opcode, no mask bit, length 2, then payload.
	want := []byte{0x81, 0x02, 'h', 'i'}
	if !bytes.Equal(got, want) {
		t.Fatalf("frame = %x, want %x", got, want)
	}
}

func TestWriteFrameBinaryOpcode(t *testing.T) {
	c, out := newReadConn(nil)
	if err := c.WriteMessage(BinaryMessage, []byte{0x01}); err != nil {
		t.Fatal(err)
	}
	got := out.Bytes()
	if got[0] != 0x82 { // FIN + binary opcode
		t.Fatalf("first byte = %x, want 0x82", got[0])
	}
}

func TestWriteText(t *testing.T) {
	c, out := newReadConn(nil)
	if err := c.WriteText("yo"); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.Bytes(), []byte{0x81, 0x02, 'y', 'o'}) {
		t.Fatalf("frame = %x", out.Bytes())
	}
}

func TestWriteFrameClientMasks(t *testing.T) {
	var out bytes.Buffer
	c := &Conn{conn: &fakeNetConn{}, bw: bufio.NewWriter(&out), client: true}
	if err := c.WriteMessage(TextMessage, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	raw := out.Bytes()
	// Mask bit must be set.
	if raw[1]&0x80 == 0 {
		t.Fatal("client frame is not masked")
	}
	// Decode it back through readFrame to confirm the mask round-trips.
	rc, _ := newReadConn(raw)
	_, opcode, payload, err := rc.readFrame(DefaultMaxMessageSize)
	if err != nil {
		t.Fatal(err)
	}
	if opcode != opText || string(payload) != "hello" {
		t.Fatalf("decoded op=%x payload=%q", opcode, payload)
	}
}

func TestWriteFrameExtendedLengths(t *testing.T) {
	for _, n := range []int{300, 70000} {
		c, out := newReadConn(nil)
		msg := bytes.Repeat([]byte("z"), n)
		if err := c.WriteMessage(BinaryMessage, msg); err != nil {
			t.Fatal(err)
		}
		rc, _ := newServerReadConn(out.Bytes())
		_, _, payload, err := rc.readFrame(DefaultMaxMessageSize)
		if err != nil {
			t.Fatal(err)
		}
		if len(payload) != n {
			t.Fatalf("round-trip len = %d, want %d", len(payload), n)
		}
	}
}

func TestWriteAfterClose(t *testing.T) {
	c := &Conn{conn: &fakeNetConn{}, bw: bufio.NewWriter(&bytes.Buffer{})}
	c.closed.Store(true)
	if err := c.writeFrame(opText, []byte("x")); err != ErrClosed {
		t.Fatalf("writeFrame after close = %v, want ErrClosed", err)
	}
}

func TestReadMessageFragmented(t *testing.T) {
	var raw []byte
	raw = append(raw, buildFrame(false, opText, []byte("Hel"), true)...)
	raw = append(raw, buildFrame(false, opContinuation, []byte("lo, "), true)...)
	raw = append(raw, buildFrame(true, opContinuation, []byte("world"), true)...)
	c, _ := newReadConn(raw)
	mt, data, err := c.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if mt != TextMessage || string(data) != "Hello, world" {
		t.Fatalf("ReadMessage = %v %q", mt, data)
	}
}

func TestReadMessageAutoPong(t *testing.T) {
	var raw []byte
	raw = append(raw, buildFrame(true, opPing, []byte("ping-data"), true)...)
	raw = append(raw, buildFrame(true, opText, []byte("after"), true)...)
	c, out := newReadConn(raw)
	mt, data, err := c.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if mt != TextMessage || string(data) != "after" {
		t.Fatalf("ReadMessage = %v %q", mt, data)
	}
	// A pong echoing the ping payload must have been written.
	rc, _ := newServerReadConn(out.Bytes())
	_, opcode, payload, err := rc.readFrame(DefaultMaxMessageSize)
	if err != nil {
		t.Fatal(err)
	}
	if opcode != opPong || string(payload) != "ping-data" {
		t.Fatalf("auto response op=%x payload=%q, want pong ping-data", opcode, payload)
	}
}

func TestReadMessagePongIgnored(t *testing.T) {
	var raw []byte
	raw = append(raw, buildFrame(true, opPong, []byte("hb"), true)...)
	raw = append(raw, buildFrame(true, opText, []byte("data"), true)...)
	c, _ := newReadConn(raw)
	_, data, err := c.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "data" {
		t.Fatalf("data = %q", data)
	}
}

func TestReadMessageClose(t *testing.T) {
	closePayload := []byte{0x03, 0xe8} // 1000
	raw := buildFrame(true, opClose, closePayload, true)
	c, out := newReadConn(raw)
	fake := c.conn.(*fakeNetConn)
	_, _, err := c.ReadMessage()
	if err != ErrClosed {
		t.Fatalf("ReadMessage = %v, want ErrClosed", err)
	}
	if !fake.closed {
		t.Fatal("connection should be closed after close frame")
	}
	// A close frame should have been echoed back.
	rc, _ := newServerReadConn(out.Bytes())
	_, opcode, _, err := rc.readFrame(DefaultMaxMessageSize)
	if err != nil {
		t.Fatal(err)
	}
	if opcode != opClose {
		t.Fatalf("echo opcode = %x, want close", opcode)
	}
}

func TestReadMessageUnexpectedContinuation(t *testing.T) {
	raw := buildFrame(true, opContinuation, []byte("x"), true)
	c, _ := newReadConn(raw)
	if _, _, err := c.ReadMessage(); err == nil {
		t.Fatal("expected error for leading continuation frame")
	}
}

func TestReadMessageUnexpectedNonContinuation(t *testing.T) {
	var raw []byte
	raw = append(raw, buildFrame(false, opText, []byte("a"), true)...)
	raw = append(raw, buildFrame(true, opText, []byte("b"), true)...) // should be continuation
	c, _ := newReadConn(raw)
	if _, _, err := c.ReadMessage(); err == nil {
		t.Fatal("expected error for non-continuation after unfinished message")
	}
}

func TestReadMessageUnknownOpcode(t *testing.T) {
	raw := buildFrame(true, 0x3, []byte("x"), true) // reserved non-control opcode
	c, _ := newReadConn(raw)
	if _, _, err := c.ReadMessage(); err == nil {
		t.Fatal("expected error for unknown opcode")
	}
}

func TestReadMessageError(t *testing.T) {
	c, _ := newReadConn(nil) // EOF immediately
	if _, _, err := c.ReadMessage(); err == nil {
		t.Fatal("expected error on EOF")
	}
}

func TestAcceptKeyKnownVector(t *testing.T) {
	// RFC 6455 §1.3 worked example.
	if got := acceptKey("dGhlIHNhbXBsZSBub25jZQ=="); got != "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=" {
		t.Fatalf("acceptKey = %q, want s3pPLMBiTxaQ9kYGzzhZRbK+xOo=", got)
	}
}

func TestUpgradeRejectsNonUpgrade(t *testing.T) {
	req := httptest.NewRequest("GET", "/", nil)
	if _, err := Upgrade(httptest.NewRecorder(), req); err == nil {
		t.Fatal("expected error for non-upgrade request")
	}
}

func TestUpgradeMissingKey(t *testing.T) {
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "websocket")
	if _, err := Upgrade(httptest.NewRecorder(), req); err == nil {
		t.Fatal("expected error for missing key")
	}
}

func TestUpgradeBadVersion(t *testing.T) {
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Sec-WebSocket-Key", "abc")
	req.Header.Set("Sec-WebSocket-Version", "8")
	if _, err := Upgrade(httptest.NewRecorder(), req); err == nil {
		t.Fatal("expected error for unsupported version")
	}
}

func TestUpgradeNonHijacker(t *testing.T) {
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Sec-WebSocket-Key", "abc")
	req.Header.Set("Sec-WebSocket-Version", "13")
	// httptest.ResponseRecorder does not implement http.Hijacker.
	if _, err := Upgrade(httptest.NewRecorder(), req); err == nil {
		t.Fatal("expected error when hijacking is unsupported")
	}
}

func TestIsWebSocketUpgradeVariants(t *testing.T) {
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Connection", "keep-alive, Upgrade")
	req.Header.Set("Upgrade", "WebSocket")
	if !IsWebSocketUpgrade(req) {
		t.Fatal("multi-token Connection header with Upgrade should be recognised")
	}
}

func TestDialRoundTripAndClose(t *testing.T) {
	srv := echoServer(t)
	defer srv.Close()

	c, err := Dial(strings.Replace(srv.URL, "http://", "ws://", 1), nil)
	if err != nil {
		t.Fatal(err)
	}
	if c.RemoteAddr() == nil {
		t.Error("RemoteAddr returned nil")
	}
	for _, msg := range []string{"one", "two", strings.Repeat("q", 500)} {
		if err := c.WriteText(msg); err != nil {
			t.Fatal(err)
		}
		mt, data, err := c.ReadMessage()
		if err != nil {
			t.Fatal(err)
		}
		if mt != TextMessage || string(data) != msg {
			t.Fatalf("echo = %q, want %q", data, msg)
		}
	}
	if err := c.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestDialBadURL(t *testing.T) {
	if _, err := Dial("://bad", nil); err == nil {
		t.Fatal("expected parse error for malformed URL")
	}
}

func TestDialConnRefused(t *testing.T) {
	// Port 1 is not listening; dial must fail.
	if _, err := Dial("ws://127.0.0.1:1", nil); err == nil {
		t.Fatal("expected dial error to unreachable host")
	}
}
