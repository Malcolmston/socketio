package socketio_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"

	socketio "github.com/malcolmston/socketio"
)

// ExampleAllowedOrigins locks a server down to the origins a browser app is
// actually served from. This matters most for the WebSocket transport: a
// WebSocket handshake is an ordinary GET that the same-origin policy does not
// restrict and that triggers no CORS preflight, so without an Origin check any
// page a user visits can open an authenticated socket to this server —
// cross-site WebSocket hijacking. Setting CheckOrigin also switches on
// Access-Control-Allow-Credentials for the polling transport, which is withheld
// while every origin is accepted.
func ExampleAllowedOrigins() {
	io := socketio.New(socketio.Options{
		CheckOrigin: socketio.AllowedOrigins(
			"https://app.example.com",
			"https://admin.example.com",
		),
	})
	defer io.Close()

	ts := httptest.NewServer(io)
	defer ts.Close()

	probe := func(origin string) int {
		req, _ := http.NewRequest(http.MethodGet, ts.URL+"/socket.io/?EIO=4&transport=polling", nil)
		req.Header.Set("Origin", origin)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return 0
		}
		resp.Body.Close()
		return resp.StatusCode
	}

	fmt.Println("app.example.com  ->", probe("https://app.example.com"))
	fmt.Println("evil.example     ->", probe("https://evil.example"))
	// Output:
	// app.example.com  -> 200
	// evil.example     -> 403
}

// ExampleOptions_trustProxy shows when the X-Forwarded-For header may be
// believed. Socket.Handshake().Address defaults to the address of the socket
// the request actually arrived on, because X-Forwarded-For is a plain request
// header any client can set — trusting it unconditionally would let a client
// pick the address that rate limiting, authorization and audit logs see. Turn
// TrustProxy on only when a reverse proxy you control *overwrites* the header.
func ExampleOptions_trustProxy() {
	behindProxy := socketio.New(socketio.Options{TrustProxy: true})
	defer behindProxy.Close()

	direct := socketio.New() // TrustProxy is false by default
	defer direct.Close()

	r := httptest.NewRequest(http.MethodGet, "/socket.io/", nil)
	r.RemoteAddr = "10.0.0.9:51234"
	r.Header.Set("X-Forwarded-For", "203.0.113.7, 10.0.0.1")

	fmt.Println("trusted:  ", socketio.NewProxiedHandshake(r, nil).Address)
	fmt.Println("untrusted:", socketio.NewHandshake(r, nil).Address)
	// Output:
	// trusted:   203.0.113.7
	// untrusted: 10.0.0.9
}
