package client

import (
	"net/url"
	"strings"
	"testing"
)

// TestResolveEndpoint pins socket.io-client's addressing rules: the URL's path
// is the NAMESPACE, the HTTP path comes from Options.Path, default ports are
// dropped and userinfo never reaches the transport URL.
func TestResolveEndpoint(t *testing.T) {
	tests := []struct {
		name       string
		in         string
		opts       Options
		wantScheme string
		wantHost   string
		wantPath   string
		wantNS     string
		wantErr    bool
	}{
		{name: "http becomes ws", in: "http://host:3000", wantScheme: "ws", wantHost: "host:3000", wantPath: "/socket.io/", wantNS: "/"},
		{name: "https becomes wss", in: "https://host", wantScheme: "wss", wantHost: "host", wantPath: "/socket.io/", wantNS: "/"},
		{name: "ws stays ws", in: "ws://host", wantScheme: "ws", wantHost: "host", wantPath: "/socket.io/", wantNS: "/"},
		{name: "wss stays wss", in: "wss://host", wantScheme: "wss", wantHost: "host", wantPath: "/socket.io/", wantNS: "/"},
		{name: "unknown scheme defaults to ws", in: "foo://host", wantScheme: "ws", wantHost: "host", wantPath: "/socket.io/", wantNS: "/"},
		{name: "url path is the namespace", in: "http://host/admin", wantScheme: "ws", wantHost: "host", wantPath: "/socket.io/", wantNS: "/admin"},
		{name: "nested namespace", in: "http://host/a/b", wantScheme: "ws", wantHost: "host", wantPath: "/socket.io/", wantNS: "/a/b"},
		{name: "explicit namespace wins", in: "http://host/admin", opts: Options{Namespace: "/other"}, wantScheme: "ws", wantHost: "host", wantPath: "/socket.io/", wantNS: "/other"},
		{name: "custom mount path", in: "http://host/admin", opts: Options{Path: "/custom"}, wantScheme: "ws", wantHost: "host", wantPath: "/custom/", wantNS: "/admin"},
		{name: "default port dropped", in: "http://host:80", wantScheme: "ws", wantHost: "host", wantPath: "/socket.io/", wantNS: "/"},
		{name: "default tls port dropped", in: "https://host:443", wantScheme: "wss", wantHost: "host", wantPath: "/socket.io/", wantNS: "/"},
		{name: "non default port kept", in: "https://host:8443", wantScheme: "wss", wantHost: "host:8443", wantPath: "/socket.io/", wantNS: "/"},
		{name: "userinfo dropped", in: "http://user:pw@host:3000/ns", wantScheme: "ws", wantHost: "host:3000", wantPath: "/socket.io/", wantNS: "/ns"},
		{name: "ipv6 literal", in: "http://[::1]:3000", wantScheme: "ws", wantHost: "[::1]:3000", wantPath: "/socket.io/", wantNS: "/"},
		{name: "ipv6 default port dropped", in: "http://[::1]:80", wantScheme: "ws", wantHost: "[::1]", wantPath: "/socket.io/", wantNS: "/"},
		{name: "malformed url errors", in: "http://[::1", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ResolveEndpoint(tt.in, tt.opts)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got %q", got.URL)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.Namespace != tt.wantNS {
				t.Fatalf("namespace = %q, want %q", got.Namespace, tt.wantNS)
			}
			u, err := url.Parse(got.URL)
			if err != nil {
				t.Fatalf("result is not a valid URL: %v", err)
			}
			if u.Scheme != tt.wantScheme {
				t.Fatalf("scheme = %q, want %q", u.Scheme, tt.wantScheme)
			}
			if u.Host != tt.wantHost {
				t.Fatalf("host = %q, want %q", u.Host, tt.wantHost)
			}
			if u.Path != tt.wantPath {
				t.Fatalf("path = %q, want %q", u.Path, tt.wantPath)
			}
			if u.User != nil {
				t.Fatalf("userinfo leaked into the transport URL: %q", got.URL)
			}
			q := u.Query()
			if q.Get("EIO") != "4" || q.Get("transport") != "websocket" {
				t.Fatalf("query missing EIO/transport: %q", u.RawQuery)
			}
			if !strings.HasPrefix(got.URL, tt.wantScheme+"://") {
				t.Fatalf("url %q does not start with %s://", got.URL, tt.wantScheme)
			}
		})
	}
}

// TestOptionsWithDefaults documents the normalisation Dial applies.
func TestOptionsWithDefaults(t *testing.T) {
	o := Options{}.WithDefaults()
	if o.Path != DefaultPath {
		t.Errorf("Path = %q, want %q", o.Path, DefaultPath)
	}
	if o.DialTimeout.Seconds() != 20 {
		t.Errorf("DialTimeout = %v, want 20s", o.DialTimeout)
	}
	if o.ReconnectionDelay.Seconds() != 1 {
		t.Errorf("ReconnectionDelay = %v, want 1s", o.ReconnectionDelay)
	}
	if o.ReconnectionDelayMax.Seconds() != 5 {
		t.Errorf("ReconnectionDelayMax = %v, want 5s", o.ReconnectionDelayMax)
	}
	if o.RandomizationFactor != 0 {
		t.Errorf("RandomizationFactor = %v, want 0", o.RandomizationFactor)
	}

	// A path missing its slashes is normalised; an out-of-range randomisation
	// factor disables jitter rather than being clamped, as backo2 does.
	o = Options{Path: "custom", RandomizationFactor: 1.5}.WithDefaults()
	if o.Path != "/custom/" {
		t.Errorf("Path = %q, want %q", o.Path, "/custom/")
	}
	if o.RandomizationFactor != 0 {
		t.Errorf("RandomizationFactor = %v, want 0 for an out-of-range value", o.RandomizationFactor)
	}
}
