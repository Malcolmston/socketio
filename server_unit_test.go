package socketio

import (
	"errors"
	"sort"
	"testing"
)

func TestNamespaceNameAndSetAdapter(t *testing.T) {
	s := New()
	ns := s.Of("admin") // leading slash is added by Of
	if ns.Name() != "/admin" {
		t.Fatalf("Name() = %q", ns.Name())
	}

	replacement := newMemoryAdapter()
	if ns.SetAdapter(replacement) != ns {
		t.Fatal("SetAdapter should return the namespace")
	}
	if ns.adapter != replacement {
		t.Fatal("adapter was not replaced")
	}
}

func TestNamespaceMiddleware(t *testing.T) {
	tests := []struct {
		name    string
		mws     []func(*Socket, func(error))
		wantErr string
	}{
		{
			name:    "all pass",
			mws:     []func(*Socket, func(error)){func(_ *Socket, next func(error)) { next(nil) }},
			wantErr: "",
		},
		{
			name: "second rejects",
			mws: []func(*Socket, func(error)){
				func(_ *Socket, next func(error)) { next(nil) },
				func(_ *Socket, next func(error)) { next(errors.New("denied")) },
			},
			wantErr: "denied",
		},
		{
			name: "halts without next",
			mws: []func(*Socket, func(error)){
				func(_ *Socket, _ func(error)) {}, // never calls next
				func(_ *Socket, next func(error)) { next(errors.New("unreached")) },
			},
			// A middleware that never calls next must NOT be treated as
			// acceptance: it denies the connection (the handshake is left
			// unanswered), reported here as the errMiddlewareSilent sentinel.
			wantErr: "socketio: middleware did not call next",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := New()
			ns := s.Of("/")
			for _, mw := range tt.mws {
				ns.Use(mw)
			}
			err := ns.runMiddleware(&Socket{})
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("err = %v, want nil", err)
				}
			} else if err == nil || err.Error() != tt.wantErr {
				t.Fatalf("err = %v, want %q", err, tt.wantErr)
			}
		})
	}
}

func TestNamespaceSocketsJoinLeaveDisconnect(t *testing.T) {
	s := New()
	ns := s.Of("/")
	_, _, a := newBoundSocket(t, s, "/")
	_, _, b := newBoundSocket(t, s, "/")

	if got := len(ns.Sockets()); got != 2 {
		t.Fatalf("Sockets() = %d, want 2", got)
	}
	if got := len(ns.FetchSockets()); got != 2 {
		t.Fatalf("FetchSockets() = %d, want 2", got)
	}

	ns.SocketsJoin("all")
	if got := len(ns.SocketsInRoom("all")); got != 2 {
		t.Fatalf("room 'all' = %d, want 2", got)
	}

	ns.SocketsLeave("all")
	if got := len(ns.SocketsInRoom("all")); got != 0 {
		t.Fatalf("room 'all' after leave = %d, want 0", got)
	}

	ns.DisconnectSockets(false)
	if got := len(ns.Sockets()); got != 0 {
		t.Fatalf("Sockets() after disconnect = %d, want 0", got)
	}
	_, _ = a, b
}

func TestServerBroadcastHelpers(t *testing.T) {
	s := New()
	_, ca, a := newBoundSocket(t, s, "/")
	_, cb, _ := newBoundSocket(t, s, "/")

	if got := len(s.Sockets()); got != 2 {
		t.Fatalf("Server.Sockets() = %d, want 2", got)
	}
	if got := len(s.FetchSockets()); got != 2 {
		t.Fatalf("Server.FetchSockets() = %d, want 2", got)
	}

	// Server.Emit broadcasts to everyone in the default namespace.
	s.Emit("global", "hi")
	if names := eventNames(bufferedEvents(t, ca)); len(names) != 1 || names[0] != "global" {
		t.Fatalf("a events = %v", names)
	}
	if names := eventNames(bufferedEvents(t, cb)); len(names) != 1 {
		t.Fatalf("b events = %v", names)
	}

	// Server.SocketsJoin + Server.To scope a broadcast to a room.
	s.SocketsJoin("room")
	if got := len(s.To("room").rooms); got != 1 {
		t.Fatalf("To('room') rooms = %d", got)
	}
	a.Leave("room")
	s.SocketsLeave("room")
	_ = a
}

func TestServerSideEmit(t *testing.T) {
	s := New()

	var gotA, gotB []any
	if s.OnServerEvent("sync", func(args []any) { gotA = args }) != s {
		t.Fatal("OnServerEvent should return the server")
	}
	s.OnServerEvent("sync", func(args []any) { gotB = args })

	s.ServerSideEmit("sync", "payload", 7)

	if len(gotA) != 2 || gotA[0] != "payload" {
		t.Fatalf("handler A got %v", gotA)
	}
	if len(gotB) != 2 {
		t.Fatalf("handler B got %v", gotB)
	}

	// Emitting an event with no handlers is a no-op.
	s.ServerSideEmit("unknown", "x")
}

func TestServerUse(t *testing.T) {
	s := New()
	called := false
	if s.Use(func(_ *Socket, next func(error)) { called = true; next(nil) }) != s {
		t.Fatal("Use should return the server")
	}
	// The middleware is registered on the default namespace.
	if err := s.Of("/").runMiddleware(&Socket{}); err != nil {
		t.Fatalf("runMiddleware: %v", err)
	}
	if !called {
		t.Fatal("registered middleware was not invoked")
	}
}

func TestServerDisconnectSockets(t *testing.T) {
	s := New()
	_, _, _ = newBoundSocket(t, s, "/")
	_, _, _ = newBoundSocket(t, s, "/")
	if len(s.Sockets()) != 2 {
		t.Fatalf("setup: want 2 sockets")
	}
	s.DisconnectSockets(false)
	if got := len(s.Sockets()); got != 0 {
		t.Fatalf("after DisconnectSockets: %d, want 0", got)
	}
}

func TestServerClose(t *testing.T) {
	s := New()
	_, c, _ := newBoundSocket(t, s, "/")
	if s.getConn(c.sid) == nil {
		t.Fatal("conn should be registered")
	}
	s.Close()
	if s.getConn(c.sid) != nil {
		t.Fatal("conn should be removed after Close")
	}
}

func TestServerOfNormalizesNames(t *testing.T) {
	s := New()
	cases := map[string]string{"": "/", "/": "/", "chat": "/chat", "/room": "/room"}
	var got []string
	for in, want := range cases {
		if name := s.Of(in).Name(); name != want {
			t.Fatalf("Of(%q).Name() = %q, want %q", in, name, want)
		}
		got = append(got, want)
	}
	sort.Strings(got)
	// Requesting the same namespace twice returns the identical instance.
	if s.Of("chat") != s.Of("/chat") {
		t.Fatal("Of should return the same namespace instance")
	}
}
