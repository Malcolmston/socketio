package socketio_test

import (
	"bufio"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
	"time"

	socketio "github.com/malcolmston/socketio"
	"github.com/malcolmston/socketio/engineio"
)

// newOptServer starts a Socket.IO server with explicit Options and returns it
// together with the base URL its transports are reachable at.
func newOptServer(t *testing.T, opts socketio.Options, configure func(*socketio.Server)) (*socketio.Server, string) {
	t.Helper()
	io := socketio.New(opts)
	if configure != nil {
		configure(io)
	}
	srv := httptest.NewServer(http.HandlerFunc(io.ServeHTTP))
	t.Cleanup(func() {
		io.Close()
		srv.Close()
	})
	return io, srv.URL + "/socket.io/"
}

// openHandshake performs the Engine.IO polling handshake and returns the
// decoded OPEN payload.
type openPayload struct {
	Sid          string   `json:"sid"`
	Upgrades     []string `json:"upgrades"`
	PingInterval int      `json:"pingInterval"`
	PingTimeout  int      `json:"pingTimeout"`
	MaxPayload   int      `json:"maxPayload"`
}

func openHandshake(t *testing.T, base string) openPayload {
	t.Helper()
	resp, err := http.Get(base + "?EIO=4&transport=polling")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("handshake status = %d, body %q", resp.StatusCode, body)
	}
	packets, err := engineio.DecodePayload(string(body))
	if err != nil || len(packets) != 1 || packets[0].Type != engineio.Open {
		t.Fatalf("bad handshake payload %q (err %v)", body, err)
	}
	var out openPayload
	if err := json.Unmarshal([]byte(packets[0].Data), &out); err != nil {
		t.Fatalf("OPEN payload is not JSON: %v (%q)", err, packets[0].Data)
	}
	return out
}

// TestHandshakeAdvertisesConfiguredTimings checks the OPEN packet against the
// Engine.IO v4 handshake contract: it must carry the session id, the transports
// the client may upgrade to, and the two heartbeat timings *in milliseconds*
// (the client derives its own ping timer from them, so a unit mistake here
// silently breaks every client's liveness detection).
//
// The expected numbers are the configured durations converted by hand:
// 250ms → 250, 1500ms → 1500. MaxPayload is echoed verbatim.
func TestHandshakeAdvertisesConfiguredTimings(t *testing.T) {
	_, base := newOptServer(t, socketio.Options{
		PingInterval: 250 * time.Millisecond,
		PingTimeout:  1500 * time.Millisecond,
		MaxPayload:   4096,
	}, nil)

	hs := openHandshake(t, base)
	if hs.Sid == "" {
		t.Error("handshake sid is empty")
	}
	if hs.PingInterval != 250 {
		t.Errorf("pingInterval = %d, want 250 (250ms expressed in ms)", hs.PingInterval)
	}
	if hs.PingTimeout != 1500 {
		t.Errorf("pingTimeout = %d, want 1500 (1500ms expressed in ms)", hs.PingTimeout)
	}
	if hs.MaxPayload != 4096 {
		t.Errorf("maxPayload = %d, want 4096", hs.MaxPayload)
	}
	if len(hs.Upgrades) != 1 || hs.Upgrades[0] != "websocket" {
		t.Errorf("upgrades = %v, want [websocket]", hs.Upgrades)
	}
}

// TestPollingHandshakeRejectsUnknownSid pins the Engine.IO error body for a sid
// the server does not know: code 1, "Session ID unknown".
func TestPollingHandshakeRejectsUnknownSid(t *testing.T) {
	_, base := newOptServer(t, socketio.Options{}, nil)
	resp, err := http.Get(base + "?EIO=4&transport=polling&sid=doesnotexist")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
	if !strings.Contains(string(body), `"code":1`) {
		t.Errorf("body = %q, want Engine.IO error code 1", body)
	}
}

// TestSessionCapRefusesNewHandshakes covers the session-map bound. Every
// handshake — authenticated or not — allocates a session plus a heartbeat
// goroutine that outlive the request, so an unbounded map is a memory
// amplification primitive: a few bytes of HTTP buy a session that lives for
// PingInterval+PingTimeout.
func TestSessionCapRefusesNewHandshakes(t *testing.T) {
	_, base := newOptServer(t, socketio.Options{MaxSessions: 2}, nil)

	openHandshake(t, base)
	openHandshake(t, base)

	resp, err := http.Get(base + "?EIO=4&transport=polling")
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("third handshake status = %d, want 503", resp.StatusCode)
	}
}

// TestSessionCapCanBeDisabled documents that a negative MaxSessions restores
// the unbounded behavior for callers that bound sessions elsewhere.
func TestSessionCapCanBeDisabled(t *testing.T) {
	_, base := newOptServer(t, socketio.Options{MaxSessions: -1}, nil)
	for i := 0; i < 5; i++ {
		openHandshake(t, base)
	}
}

// TestHeartbeatDisconnectsSilentClient exercises the server heartbeat: after
// PingInterval+PingTimeout with no PONG, the session must be reaped. The client
// here connects over polling and then goes silent.
func TestHeartbeatDisconnectsSilentClient(t *testing.T) {
	disconnected := make(chan string, 1)
	_, base := newOptServer(t, socketio.Options{
		PingInterval: 20 * time.Millisecond,
		PingTimeout:  20 * time.Millisecond,
	}, func(io *socketio.Server) {
		io.OnConnection(func(s *socketio.Socket) {
			s.OnDisconnect(func(reason string) {
				select {
				case disconnected <- reason:
				default:
				}
			})
		})
	})

	hs := openHandshake(t, base)
	url := base + "?EIO=4&transport=polling&sid=" + hs.Sid
	resp, err := http.Post(url, "text/plain", strings.NewReader("40"))
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	select {
	case reason := <-disconnected:
		if reason != "transport close" {
			t.Fatalf("disconnect reason = %q, want %q", reason, "transport close")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("silent client was never reaped by the heartbeat")
	}
}

// ---- websocket upgrade -----------------------------------------------------

// dialUpgrade opens a raw websocket to the transport endpoint, optionally
// carrying an existing sid (which selects the polling→websocket upgrade path).
func dialUpgrade(t *testing.T, base, sid string) *wsClient {
	t.Helper()
	trimmed := strings.TrimPrefix(base, "http://")
	host := trimmed[:strings.IndexByte(trimmed, '/')]
	path := trimmed[strings.IndexByte(trimmed, '/'):] + "?EIO=4&transport=websocket"
	if sid != "" {
		path += "&sid=" + sid
	}

	conn, err := net.Dial("tcp", host)
	if err != nil {
		t.Fatal(err)
	}
	var key [16]byte
	if _, err := rand.Read(key[:]); err != nil {
		t.Fatal(err)
	}
	req := "GET " + path + " HTTP/1.1\r\n" +
		"Host: " + host + "\r\n" +
		"Upgrade: websocket\r\nConnection: Upgrade\r\n" +
		"Sec-WebSocket-Key: " + base64.StdEncoding.EncodeToString(key[:]) + "\r\n" +
		"Sec-WebSocket-Version: 13\r\n\r\n"
	if _, err := conn.Write([]byte(req)); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("upgrade status = %d, want 101", resp.StatusCode)
	}
	t.Cleanup(func() { conn.Close() })
	return &wsClient{conn: conn, br: br}
}

// TestPollingToWebSocketUpgradeProbe walks the Engine.IO upgrade handshake end
// to end: the client opens a polling session, dials a websocket carrying that
// sid, sends "2probe", must get "3probe" back, sends "5" (UPGRADE), and only
// then may traffic flow over the new transport. Sending an event before the
// UPGRADE packet must still be honored — engine.io keeps servicing the old
// transport until the upgrade completes.
func TestPollingToWebSocketUpgradeProbe(t *testing.T) {
	_, base := newOptServer(t, socketio.Options{}, func(io *socketio.Server) {
		io.OnConnection(func(s *socketio.Socket) {
			s.On("echo", func(args []any) []any {
				s.Emit("echo", args...)
				return nil
			})
		})
	})

	hs := openHandshake(t, base)
	pollURL := base + "?EIO=4&transport=polling&sid=" + hs.Sid

	// CONNECT over polling and drain the CONNECT ack.
	post(t, pollURL, "40")
	if got := pollOnce(t, pollURL); !strings.HasPrefix(got, "40") {
		t.Fatalf("polling CONNECT ack = %q", got)
	}

	c := dialUpgrade(t, base, hs.Sid)

	c.writeText("2probe")
	if got := c.readText(t); got != "3probe" {
		t.Fatalf("probe reply = %q, want %q", got, "3probe")
	}
	c.writeText("5") // UPGRADE

	// Traffic now flows over the websocket.
	c.writeText(`42["echo","after upgrade"]`)
	deadline := time.Now().Add(5 * time.Second)
	for {
		if time.Now().After(deadline) {
			t.Fatal("no echo received after upgrade")
		}
		got := c.readText(t)
		if strings.Contains(got, `["echo","after upgrade"]`) {
			return
		}
	}
}

// TestUpgradeForUnknownSessionClosesSocket covers an upgrade naming a sid the
// server does not know (a stale reconnect, or a guessed id): the websocket must
// be closed, not left open.
func TestUpgradeForUnknownSessionClosesSocket(t *testing.T) {
	_, base := newOptServer(t, socketio.Options{}, nil)

	c := dialUpgrade(t, base, "no-such-session")
	assertClosedByServer(t, c)
}

// TestUpgradeRaceSessionClosedDuringProbe drives the upgrade race directly. The
// server's probe handshake blocks reading until the client sends the UPGRADE
// packet, which gives the test a window to tear the session down in between:
//
//	client                     server
//	  "2probe"  ───────────▶   probe() replies "3probe", then blocks on read
//	                           (test calls server.Close(): session removed)
//	  "5"       ───────────▶   probe() returns true → attach
//
// At the attach point the session is gone. Attaching anyway would hand the
// websocket — and the goroutines pumping it — to a conn that is no longer in
// the server's map, so nothing will ever close it. The server must instead
// close the socket.
func TestUpgradeRaceSessionClosedDuringProbe(t *testing.T) {
	server, base := newOptServer(t, socketio.Options{}, nil)

	hs := openHandshake(t, base)
	c := dialUpgrade(t, base, hs.Sid)

	c.writeText("2probe")
	if got := c.readText(t); got != "3probe" {
		t.Fatalf("probe reply = %q, want %q", got, "3probe")
	}

	server.Close() // the session disappears mid-upgrade

	c.writeText("5") // UPGRADE
	assertClosedByServer(t, c)
}

// assertClosedByServer drains the connection and fails unless the server closes
// it (EOF or reset) within a short window.
func assertClosedByServer(t *testing.T, c *wsClient) {
	t.Helper()
	_ = c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	// A close frame may arrive first; what matters is that the stream ends.
	_, err := io.Copy(io.Discard, c.br)
	var nerr net.Error
	if errors.As(err, &nerr) && nerr.Timeout() {
		t.Fatal("server left the websocket open instead of closing it")
	}
}

// TestNoGoroutineLeakOnWebSocketDisconnect connects and disconnects repeatedly
// over websocket and asserts the goroutine count returns to its baseline. Each
// session starts a heartbeat goroutine, a read loop and a write loop; if any of
// them outlives the transport, a server that has served traffic for a while
// accumulates them without bound.
func TestNoGoroutineLeakOnWebSocketDisconnect(t *testing.T) {
	_, base := newOptServer(t, socketio.Options{
		PingInterval: 50 * time.Millisecond,
		PingTimeout:  50 * time.Millisecond,
	}, func(io *socketio.Server) {
		io.OnConnection(func(s *socketio.Socket) { s.Join("room") })
	})

	// Warm up so one-off runtime/http goroutines are already counted.
	for i := 0; i < 3; i++ {
		c := dialUpgrade(t, base, "")
		c.readText(t) // OPEN
		c.writeText("40")
		c.readText(t) // CONNECT ack
		c.conn.Close()
	}
	waitForGoroutines(t, 0, 2*time.Second) // let the warm-up sessions settle
	baseline := runtime.NumGoroutine()

	const rounds = 25
	for i := 0; i < rounds; i++ {
		c := dialUpgrade(t, base, "")
		c.readText(t) // OPEN
		c.writeText("40")
		c.readText(t) // CONNECT ack
		c.conn.Close()
	}

	if !waitForGoroutines(t, baseline+5, 5*time.Second) {
		t.Fatalf("goroutines = %d after %d connect/disconnect cycles, baseline %d",
			runtime.NumGoroutine(), rounds, baseline)
	}
}

// waitForGoroutines waits until the goroutine count drops to at most want,
// reporting whether it did. want == 0 means "just wait out the settling time".
func waitForGoroutines(t *testing.T, want int, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		runtime.Gosched()
		if want > 0 && runtime.NumGoroutine() <= want {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return want == 0
}

// TestSlowClientDoesNotStallRoomBroadcast is the availability regression test
// for a fan-out blocked by one peer. Websocket writes go to a TCP socket: a
// client that stops reading fills its receive window and then the server's send
// buffer, at which point a *synchronous* write blocks indefinitely. When that
// write happens inside a room broadcast loop, one unresponsive client freezes
// delivery to every other member of the room — and, because Socket.IO handlers
// broadcast from the read loop, to the whole server's event dispatch.
//
// Here "slow" is a client that completes the handshake and then never reads a
// byte. The healthy client must still receive the full broadcast.
func TestSlowClientDoesNotStallRoomBroadcast(t *testing.T) {
	server, base := newOptServer(t, socketio.Options{}, func(io *socketio.Server) {
		io.OnConnection(func(s *socketio.Socket) { s.Join("room") })
	})

	connect := func() *wsClient {
		c := dialUpgrade(t, base, "")
		c.readText(t) // OPEN
		c.writeText("40")
		c.readText(t) // CONNECT ack
		return c
	}

	fast := connect()
	slow := connect() // handshakes, then never reads again
	_ = slow

	// Wait for both sockets to be registered in the room.
	deadline := time.Now().Add(5 * time.Second)
	for len(server.Of("/").SocketsInRoom("room")) < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	const (
		messages = 1500
		payload  = 2000 // bytes; enough that ~500 messages fill the kernel buffers
	)
	body := strings.Repeat("x", payload)

	received := make(chan int, 1)
	go func() {
		_ = fast.conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		n := 0
		for n < messages {
			frame := fast.readText(t)
			if strings.Contains(frame, `["blast"`) {
				n++
			}
		}
		received <- n
	}()

	done := make(chan struct{})
	go func() {
		for i := 0; i < messages; i++ {
			server.To("room").Emit("blast", body)
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("broadcast loop stalled on the client that stopped reading")
	}

	select {
	case n := <-received:
		if n != messages {
			t.Fatalf("healthy client received %d/%d broadcasts", n, messages)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("healthy client did not receive the broadcast")
	}
}

// readAnyFrame reads one websocket frame and returns its opcode and payload.
// Unlike wsClient.readText it does not discard the opcode, which is what
// distinguishes an Engine.IO binary attachment (a native binary frame, opcode
// 0x2) from its base64 text fallback (opcode 0x1, payload "b<base64>").
func readAnyFrame(t *testing.T, c *wsClient) (byte, []byte) {
	t.Helper()
	var header [2]byte
	if _, err := io.ReadFull(c.br, header[:]); err != nil {
		t.Fatal(err)
	}
	opcode := header[0] & 0x0f
	length := uint64(header[1] & 0x7f)
	switch length {
	case 126:
		var ext [2]byte
		if _, err := io.ReadFull(c.br, ext[:]); err != nil {
			t.Fatal(err)
		}
		length = uint64(ext[0])<<8 | uint64(ext[1])
	case 127:
		var ext [8]byte
		if _, err := io.ReadFull(c.br, ext[:]); err != nil {
			t.Fatal(err)
		}
		for _, b := range ext {
			length = length<<8 | uint64(b)
		}
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(c.br, payload); err != nil {
		t.Fatal(err)
	}
	return opcode, payload
}

// TestBufferedBinaryFlushedAsBinaryFrameAfterUpgrade covers packets that were
// queued while the session was still polling and are flushed when it upgrades.
// Engine.IO v4 carries binary attachments over WebSocket as native binary
// frames and only falls back to "b"+base64 in a text context (the polling
// payload). The flush path used to encode every buffered packet with
// WriteText, which silently downgraded a queued attachment to the base64 text
// form — the wrong wire representation for the transport it was being written
// to.
func TestBufferedBinaryFlushedAsBinaryFrameAfterUpgrade(t *testing.T) {
	sockets := make(chan *socketio.Socket, 1)
	_, base := newOptServer(t, socketio.Options{}, func(io *socketio.Server) {
		io.OnConnection(func(s *socketio.Socket) { sockets <- s })
	})

	hs := openHandshake(t, base)
	pollURL := base + "?EIO=4&transport=polling&sid=" + hs.Sid
	post(t, pollURL, "40")
	if got := pollOnce(t, pollURL); !strings.HasPrefix(got, "40") {
		t.Fatalf("CONNECT ack = %q", got)
	}

	var sock *socketio.Socket
	select {
	case sock = <-sockets:
	case <-time.After(5 * time.Second):
		t.Fatal("connection handler never ran")
	}

	// No poll is outstanding, so this is buffered until the transport changes.
	want := []byte{0x00, 0x01, 0xfe, 0xff}
	if err := sock.Emit("bin", want); err != nil {
		t.Fatalf("Emit: %v", err)
	}

	c := dialUpgrade(t, base, hs.Sid)
	c.writeText("2probe")
	if got := c.readText(t); got != "3probe" {
		t.Fatalf("probe reply = %q", got)
	}
	c.writeText("5")

	_ = c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	// The BINARY_EVENT header arrives as a text frame...
	op, payload := readAnyFrame(t, c)
	for op == 0x1 && strings.HasPrefix(string(payload), "6") { // skip a NOOP
		op, payload = readAnyFrame(t, c)
	}
	if op != 0x1 {
		t.Fatalf("header frame opcode = 0x%x, want 0x1 (text)", op)
	}
	if !strings.HasPrefix(string(payload), `451-["bin"`) {
		t.Fatalf("header frame = %q, want an Engine.IO MESSAGE carrying BINARY_EVENT", payload)
	}
	// ...and the attachment as a native binary frame, not base64 text.
	op, payload = readAnyFrame(t, c)
	if op != 0x2 {
		t.Fatalf("attachment frame opcode = 0x%x (payload %q), want 0x2 (binary)", op, payload)
	}
	if string(payload) != string(want) {
		t.Fatalf("attachment = %v, want %v", payload, want)
	}
}

// ---- small polling helpers -------------------------------------------------

func post(t *testing.T, url, body string) {
	t.Helper()
	resp, err := http.Post(url, "text/plain", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
}

func pollOnce(t *testing.T, url string) string {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return string(body)
}
