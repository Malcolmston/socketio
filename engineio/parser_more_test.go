package engineio

import (
	"reflect"
	"testing"
)

func TestPacketTypeString(t *testing.T) {
	cases := []struct {
		t    PacketType
		want string
	}{
		{Open, "open"},
		{Close, "close"},
		{Ping, "ping"},
		{Pong, "pong"},
		{Message, "message"},
		{Upgrade, "upgrade"},
		{Noop, "noop"},
		{PacketType(99), "unknown"},
	}
	for _, c := range cases {
		if got := c.t.String(); got != c.want {
			t.Errorf("PacketType(%d).String() = %q, want %q", c.t, got, c.want)
		}
	}
}

func TestEncodeAllTypes(t *testing.T) {
	cases := []struct {
		p    Packet
		wire string
	}{
		{Packet{Type: Open, Data: "{}"}, "0{}"},
		{Packet{Type: Close}, "1"},
		{Packet{Type: Ping, Data: "probe"}, "2probe"},
		{Packet{Type: Pong, Data: "probe"}, "3probe"},
		{Packet{Type: Message, Data: "hello"}, "4hello"},
		{Packet{Type: Upgrade}, "5"},
		{Packet{Type: Noop}, "6"},
	}
	for _, c := range cases {
		if got := c.p.Encode(); got != c.wire {
			t.Errorf("Encode(%v) = %q, want %q", c.p, got, c.wire)
		}
		dec, err := Decode(c.wire)
		if err != nil {
			t.Fatalf("Decode(%q): %v", c.wire, err)
		}
		if dec.Type != c.p.Type || dec.Data != c.p.Data {
			t.Errorf("Decode(%q) = %+v, want %+v", c.wire, dec, c.p)
		}
	}
}

func TestConstructors(t *testing.T) {
	m := NewMessage("hi")
	if m.Type != Message || m.Data != "hi" {
		t.Errorf("NewMessage = %+v", m)
	}
	o := NewOpen(`{"sid":"x"}`)
	if o.Type != Open || o.Data != `{"sid":"x"}` {
		t.Errorf("NewOpen = %+v", o)
	}
}

func TestDecodeBadBase64(t *testing.T) {
	if _, err := Decode("b@@@not-base64@@@"); err == nil {
		t.Error("expected base64 decode error")
	}
}

func TestDecodePayloadEmpty(t *testing.T) {
	// An empty polling body is malformed: engine.io-parser yields a "parser
	// error" for "". The port returns an error rather than an empty batch.
	if _, err := DecodePayload(""); err == nil {
		t.Error("DecodePayload(\"\") should return an error")
	}
}

func TestDecodePayloadPropagatesError(t *testing.T) {
	// The second packet is an empty string (two adjacent separators), which
	// Decode rejects.
	if _, err := DecodePayload("4hi\x1e\x1e2"); err == nil {
		t.Error("expected error from malformed payload")
	}
}

func TestPayloadWithBinaryPacket(t *testing.T) {
	packets := []Packet{
		{Type: Message, Data: "4"},
		{Type: Message, Binary: []byte{0x01, 0x02, 0x03, 0xfe}},
		{Type: Ping},
	}
	payload := EncodePayload(packets)
	got, err := DecodePayload(payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("decoded %d packets, want 3", len(got))
	}
	if got[0].Type != Message || got[0].Data != "4" {
		t.Errorf("packet 0 = %+v", got[0])
	}
	if got[1].Type != Message || !reflect.DeepEqual(got[1].Binary, packets[1].Binary) {
		t.Errorf("packet 1 = %+v, want binary %v", got[1], packets[1].Binary)
	}
	if got[2].Type != Ping {
		t.Errorf("packet 2 = %+v", got[2])
	}
}
