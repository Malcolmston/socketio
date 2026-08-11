package socketio_test

import (
	"bufio"
	"crypto/rand"
	"encoding/base64"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	socketio "github.com/malcolmston/socketio"
)

func TestAllowedOrigins(t *testing.T) {
	tests := []struct {
		name    string
		allowed []string
		origin  string
		want    bool
	}{
		{"exact match", []string{"https://app.example.com"}, "https://app.example.com", true},
		{"case-insensitive host", []string{"https://App.Example.com"}, "https://app.example.com", true},
		{"different scheme rejected", []string{"https://app.example.com"}, "http://app.example.com", false},
		{"different port rejected", []string{"https://app.example.com"}, "https://app.example.com:8443", false},
		{"unlisted origin rejected", []string{"https://app.example.com"}, "https://evil.example", false},
		{"second entry matches", []string{"https://a.example", "https://b.example"}, "https://b.example", true},
		{"no Origin header allowed", []string{"https://app.example.com"}, "", true},
		{"wildcard allows everything", []string{"*"}, "https://evil.example", true},
		{"empty allowlist rejects", nil, "https://a.example", false},
		{"empty allowlist still allows originless", nil, "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			check := socketio.AllowedOrigins(tt.allowed...)
			r := httptest.NewRequest(http.MethodGet, "/socket.io/", nil)
			if tt.origin != "" {
				r.Header.Set("Origin", tt.origin)
			}
			if got := check(r); got != tt.want {
				t.Fatalf("AllowedOrigins(%v)(Origin: %q) = %v, want %v",
					tt.allowed, tt.origin, got, tt.want)
			}
		})
	}
}

// TestCheckOriginRejectsCrossSiteWebSocket is the cross-site WebSocket
// hijacking guard. A WebSocket handshake is a plain GET that the same-origin
// policy does not restrict and that triggers no CORS preflight, so without an
// Origin check any page can open an authenticated socket to this server. The
// rejection must happen before the 101, since after the upgrade there is no
// HTTP status left to refuse with.
func TestCheckOriginRejectsCrossSiteWebSocket(t *testing.T) {
	srv := newOriginServer(t, socketio.Options{
		CheckOrigin: socketio.AllowedOrigins("https://good.example"),
	})
	defer srv.Close()

	if status := wsHandshakeStatus(t, srv.URL, "https://evil.example"); status != http.StatusForbidden {
		t.Fatalf("cross-site upgrade status = %d, want %d", status, http.StatusForbidden)
	}
	if status := wsHandshakeStatus(t, srv.URL, "https://good.example"); status != http.StatusSwitchingProtocols {
		t.Fatalf("allowed-origin upgrade status = %d, want %d", status, http.StatusSwitchingProtocols)
	}
	// A client with no Origin header is not a browser and is not the threat the
	// check addresses; it must still be able to connect.
	if status := wsHandshakeStatus(t, srv.URL, ""); status != http.StatusSwitchingProtocols {
		t.Fatalf("originless upgrade status = %d, want %d", status, http.StatusSwitchingProtocols)
	}
}

// TestCheckOriginRejectsPolling covers the same guard on the polling transport.
func TestCheckOriginRejectsPolling(t *testing.T) {
	srv := newOriginServer(t, socketio.Options{
		CheckOrigin: socketio.AllowedOrigins("https://good.example"),
	})
	defer srv.Close()

	resp := getWithOrigin(t, srv.URL+"?EIO=4&transport=polling", "https://evil.example")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("polling handshake from disallowed origin = %d, want 403", resp.StatusCode)
	}
}

// TestCORSCredentialsRequireCheckOrigin pins the CORS policy. Reflecting an
// arbitrary Origin *and* allowing credentials would let any web page read this
// server's polling responses using the visitor's cookies, so credentials are
// only granted once the application has vetted origins via CheckOrigin.
func TestCORSCredentialsRequireCheckOrigin(t *testing.T) {
	open := newOriginServer(t, socketio.Options{})
	defer open.Close()

	resp := getWithOrigin(t, open.URL+"?EIO=4&transport=polling", "https://evil.example")
	defer resp.Body.Close()
	if got := resp.Header.Get("Access-Control-Allow-Origin"); got != "https://evil.example" {
		t.Errorf("Access-Control-Allow-Origin = %q, want the reflected origin", got)
	}
	if got := resp.Header.Get("Access-Control-Allow-Credentials"); got != "" {
		t.Errorf("Access-Control-Allow-Credentials = %q with no CheckOrigin configured; want unset", got)
	}
	if got := resp.Header.Get("Vary"); got != "Origin" {
		t.Errorf("Vary = %q, want %q (the response body varies by Origin)", got, "Origin")
	}

	vetted := newOriginServer(t, socketio.Options{
		CheckOrigin: socketio.AllowedOrigins("https://good.example"),
	})
	defer vetted.Close()

	resp2 := getWithOrigin(t, vetted.URL+"?EIO=4&transport=polling", "https://good.example")
	defer resp2.Body.Close()
	if got := resp2.Header.Get("Access-Control-Allow-Credentials"); got != "true" {
		t.Errorf("Access-Control-Allow-Credentials = %q for a vetted origin, want %q", got, "true")
	}
}

// ---- helpers ---------------------------------------------------------------

func newOriginServer(t *testing.T, opts socketio.Options) *httptest.Server {
	t.Helper()
	io := socketio.New(opts)
	t.Cleanup(io.Close)
	return httptest.NewServer(http.HandlerFunc(io.ServeHTTP))
}

func getWithOrigin(t *testing.T, url, origin string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	if origin != "" {
		req.Header.Set("Origin", origin)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

// wsHandshakeStatus performs a raw WebSocket opening handshake with the given
// Origin and returns the HTTP status the server replied with.
func wsHandshakeStatus(t *testing.T, base, origin string) int {
	t.Helper()
	host := strings.TrimPrefix(base, "http://")
	conn, err := net.Dial("tcp", host)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	var key [16]byte
	if _, err := rand.Read(key[:]); err != nil {
		t.Fatal(err)
	}
	req := "GET /socket.io/?EIO=4&transport=websocket HTTP/1.1\r\n" +
		"Host: " + host + "\r\n" +
		"Upgrade: websocket\r\nConnection: Upgrade\r\n" +
		"Sec-WebSocket-Key: " + base64.StdEncoding.EncodeToString(key[:]) + "\r\n" +
		"Sec-WebSocket-Version: 13\r\n"
	if origin != "" {
		req += "Origin: " + origin + "\r\n"
	}
	req += "\r\n"
	if _, err := conn.Write([]byte(req)); err != nil {
		t.Fatal(err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp.StatusCode
}
