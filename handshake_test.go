package socketio

import (
	"net/http/httptest"
	"testing"
)

func TestNewHandshakeBasic(t *testing.T) {
	r := httptest.NewRequest("GET", "/socket.io/?EIO=4&transport=polling&token=abc", nil)
	r.RemoteAddr = "192.0.2.10:54321"
	r.Header.Set("Origin", "https://example.com")

	auth := map[string]any{"token": "abc"}
	h := NewHandshake(r, auth)

	if h.Address != "192.0.2.10" {
		t.Errorf("Address = %q, want 192.0.2.10 (port stripped)", h.Address)
	}
	if !h.XDomain {
		t.Error("XDomain should be true when Origin present")
	}
	if h.Secure {
		t.Error("Secure should be false for plain HTTP")
	}
	if h.Param("transport") != "polling" {
		t.Errorf("Param(transport) = %q", h.Param("transport"))
	}
	if h.Param("token") != "abc" {
		t.Errorf("Param(token) = %q", h.Param("token"))
	}
	if h.Get("Origin") != "https://example.com" {
		t.Errorf("Get(Origin) = %q", h.Get("Origin"))
	}
	if h.Issued != h.Time.UnixMilli() {
		t.Errorf("Issued = %d, Time.UnixMilli = %d", h.Issued, h.Time.UnixMilli())
	}
	m, ok := h.Auth.(map[string]any)
	if !ok || m["token"] != "abc" {
		t.Errorf("Auth = %#v", h.Auth)
	}
}

// TestNewHandshakeForwardedFor pins the security default: X-Forwarded-For is
// client-supplied, so NewHandshake must ignore it and report the address the
// socket actually came from. Honoring it unconditionally would let any peer
// choose the address that authorization, rate limiting and audit logs see.
func TestNewHandshakeForwardedFor(t *testing.T) {
	r := httptest.NewRequest("GET", "/socket.io/", nil)
	r.RemoteAddr = "10.0.0.1:1234"
	r.Header.Set("X-Forwarded-For", " 203.0.113.7 , 10.0.0.1")

	h := NewHandshake(r, nil)
	if h.Address != "10.0.0.1" {
		t.Errorf("Address = %q, want 10.0.0.1 (socket peer; forged XFF must be ignored)", h.Address)
	}
}

// TestNewProxiedHandshakeForwardedFor covers the opt-in path used behind a
// reverse proxy that overwrites X-Forwarded-For: the client-most hop is taken
// and surrounding whitespace trimmed.
func TestNewProxiedHandshakeForwardedFor(t *testing.T) {
	r := httptest.NewRequest("GET", "/socket.io/", nil)
	r.RemoteAddr = "10.0.0.1:1234"
	r.Header.Set("X-Forwarded-For", " 203.0.113.7 , 10.0.0.1")

	h := NewProxiedHandshake(r, nil)
	if h.Address != "203.0.113.7" {
		t.Errorf("Address = %q, want 203.0.113.7 (first forwarded hop, trimmed)", h.Address)
	}
}

// TestNewProxiedHandshakeFallsBackToRemoteAddr covers a trusted-proxy server
// receiving a request with no X-Forwarded-For at all.
func TestNewProxiedHandshakeFallsBackToRemoteAddr(t *testing.T) {
	r := httptest.NewRequest("GET", "/socket.io/", nil)
	r.RemoteAddr = "10.0.0.1:1234"

	h := NewProxiedHandshake(r, nil)
	if h.Address != "10.0.0.1" {
		t.Errorf("Address = %q, want 10.0.0.1", h.Address)
	}
}

func TestNewHandshakeSecure(t *testing.T) {
	r := httptest.NewRequest("GET", "https://example.com/socket.io/", nil)
	// httptest sets TLS for https URLs.
	h := NewHandshake(r, nil)
	if !h.Secure {
		t.Error("Secure should be true for https request")
	}
	if h.XDomain {
		t.Error("XDomain should be false without Origin header")
	}
}
