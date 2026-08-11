package socketio

import (
	"net/http"
	"strings"
)

// AllowedOrigins builds an Options.CheckOrigin function that admits only the
// listed origins. It is the Go equivalent of configuring socket.io's
// `cors: { origin: [...] }`, and exists because the zero value of Options
// accepts every origin:
//
//	io := socketio.New(socketio.Options{
//		CheckOrigin: socketio.AllowedOrigins("https://app.example.com"),
//	})
//
// Comparison is on the exact Origin header value (scheme, host and port),
// case-insensitively for the scheme and host as browsers always send them
// lowercased. A request with no Origin header is allowed: only browsers send
// one, and the same-origin policy — the thing an allowlist protects — does not
// apply to a non-browser client, which could set any value it liked anyway.
//
// Setting CheckOrigin matters most for the WebSocket transport. A WebSocket
// handshake is *not* subject to the same-origin policy: any page on any site
// can open a socket to this server and, if the browser attaches cookies, act
// with the visitor's authority ("cross-site WebSocket hijacking"). Unlike the
// polling transport, no CORS preflight stands in the way, so the Origin check
// is the only defense. Pass "*" to allow everything explicitly (the same as
// leaving CheckOrigin nil, but self-documenting).
func AllowedOrigins(origins ...string) func(*http.Request) bool {
	allowed := make(map[string]struct{}, len(origins))
	wildcard := false
	for _, o := range origins {
		if o == "*" {
			wildcard = true
			continue
		}
		allowed[strings.ToLower(strings.TrimSpace(o))] = struct{}{}
	}
	return func(r *http.Request) bool {
		if wildcard {
			return true
		}
		origin := r.Header.Get("Origin")
		if origin == "" {
			return true
		}
		_, ok := allowed[strings.ToLower(origin)]
		return ok
	}
}
