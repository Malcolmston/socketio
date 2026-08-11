package socketio

import (
	"testing"
)

func u64(v uint64) *uint64 { return &v }

func TestEncodePacket(t *testing.T) {
	cases := []struct {
		packet Packet
		wire   string
	}{
		{Packet{Type: Connect, Data: map[string]any{"sid": "abc"}}, `0{"sid":"abc"}`},
		{Packet{Type: Connect, Namespace: "/admin", Data: map[string]any{"sid": "x"}}, `0/admin,{"sid":"x"}`},
		{Packet{Type: Event, Data: []any{"hello", "world"}}, `2["hello","world"]`},
		{Packet{Type: Event, Namespace: "/admin", Data: []any{"ping"}}, `2/admin,["ping"]`},
		{Packet{Type: Event, ID: u64(12), Data: []any{"ev"}}, `212["ev"]`},
		{Packet{Type: Ack, ID: u64(12), Data: []any{"ok"}}, `312["ok"]`},
		{Packet{Type: Disconnect, Namespace: "/admin"}, `1/admin,`},
		{Packet{Type: Disconnect}, `1`},
	}
	for _, c := range cases {
		got, err := c.packet.Encode()
		if err != nil {
			t.Fatalf("Encode(%+v): %v", c.packet, err)
		}
		if got != c.wire {
			t.Errorf("Encode(%+v) = %q, want %q", c.packet, got, c.wire)
		}
	}
}

func TestDecodePacket(t *testing.T) {
	p, err := DecodePacket(`2/admin,12["hello","world",42]`)
	if err != nil {
		t.Fatal(err)
	}
	if p.Type != Event {
		t.Errorf("Type = %v, want EVENT", p.Type)
	}
	if p.Namespace != "/admin" {
		t.Errorf("Namespace = %q, want /admin", p.Namespace)
	}
	if p.ID == nil || *p.ID != 12 {
		t.Errorf("ID = %v, want 12", p.ID)
	}
	if p.EventName() != "hello" {
		t.Errorf("EventName = %q, want hello", p.EventName())
	}
	args := p.Args()
	if len(args) != 2 || args[0] != "world" || args[1] != float64(42) {
		t.Errorf("Args = %v", args)
	}
}

func TestDecodeDefaultNamespace(t *testing.T) {
	p, err := DecodePacket(`2["msg","hi"]`)
	if err != nil {
		t.Fatal(err)
	}
	if p.Namespace != "/" {
		t.Errorf("Namespace = %q, want /", p.Namespace)
	}
	if p.ID != nil {
		t.Errorf("ID = %v, want nil", p.ID)
	}
	if p.EventName() != "msg" {
		t.Errorf("EventName = %q", p.EventName())
	}
}

func TestConnectRoundTrip(t *testing.T) {
	p, err := DecodePacket(`0/chat,{"token":"xyz"}`)
	if err != nil {
		t.Fatal(err)
	}
	if p.Type != Connect || p.Namespace != "/chat" {
		t.Fatalf("got %+v", p)
	}
	obj, ok := p.Data.(map[string]any)
	if !ok || obj["token"] != "xyz" {
		t.Fatalf("payload = %v", p.Data)
	}
}

func TestDecodeInvalid(t *testing.T) {
	// "" is valid: it decodes to a CONNECT to "/" (Number("") === 0), matching
	// socket.io-parser.
	if p, err := DecodePacket(""); err != nil || p.Type != Connect || p.Namespace != "/" {
		t.Errorf("DecodePacket(\"\") = %+v, %v; want a CONNECT to /", p, err)
	}
	if _, err := DecodePacket("9[]"); err == nil {
		t.Error("expected error for invalid type")
	}
}

func TestAckIdBoundary(t *testing.T) {
	// "212[...]" -> type 2, ack id 12 (digits are greedy).
	p, err := DecodePacket(`212["ev"]`)
	if err != nil {
		t.Fatal(err)
	}
	if p.ID == nil || *p.ID != 12 {
		t.Fatalf("ID = %v, want 12", p.ID)
	}
	if p.EventName() != "ev" {
		t.Fatalf("EventName = %q", p.EventName())
	}
}
