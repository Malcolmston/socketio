package socketio

// Upstream-parity tests for the Socket.IO v5 wire codec.
//
// The vectors below are transcribed verbatim from socket.io-parser's own test
// suite (github.com/socketio/socket.io-parser, branch master):
//
//   - test/parser.js  — "exposes types", the encode round-trip cases, the
//     circular-payload encode error, the "bad binary packet", and the
//     "invalid payload" known-answer strings.
//   - test/buffer.js  — the two binary (BINARY_EVENT / BINARY_ACK) round-trips.
//
// Each encode case asserts the exact wire string socket.io-parser produces and
// then decodes it back, so these are true known-answer tests rather than mere
// self-round-trips.

import (
	"reflect"
	"testing"
)

// TestParityParserExposesTypes mirrors socket.io-parser's "exposes types": the
// seven packet-type constants are distinct, contiguous numbers 0..6.
func TestParityParserExposesTypes(t *testing.T) {
	want := map[PacketType]string{
		Connect:      "CONNECT",
		Disconnect:   "DISCONNECT",
		Event:        "EVENT",
		Ack:          "ACK",
		ConnectError: "CONNECT_ERROR",
		BinaryEvent:  "BINARY_EVENT",
		BinaryAck:    "BINARY_ACK",
	}
	seen := map[PacketType]bool{}
	for tp, name := range want {
		if tp > BinaryAck {
			t.Errorf("%s = %d, out of range", name, tp)
		}
		if seen[tp] {
			t.Errorf("duplicate packet type value %d", tp)
		}
		seen[tp] = true
		if tp.String() != name {
			t.Errorf("PacketType(%d).String() = %q, want %q", tp, tp.String(), name)
		}
	}
}

// TestParityParserEncode covers the plain-text encode cases from
// socket.io-parser/test/parser.js. Each row asserts the exact wire string and
// that decoding it reproduces the packet.
func TestParityParserEncode(t *testing.T) {
	one := u64(1)
	oneTwoThree := u64(123)
	cases := []struct {
		name string
		pkt  Packet
		wire string
		data any // expected decoded Data (JSON value types)
	}{
		{
			"encodes connection",
			Packet{Type: Connect, Namespace: "/woot", Data: map[string]any{"token": "123"}},
			`0/woot,{"token":"123"}`,
			map[string]any{"token": "123"},
		},
		{
			"encodes disconnection",
			Packet{Type: Disconnect, Namespace: "/woot"},
			`1/woot,`,
			nil,
		},
		{
			"encodes an event",
			Packet{Type: Event, Namespace: "/", Data: []any{"a", 1, map[string]any{}}},
			`2["a",1,{}]`,
			[]any{"a", float64(1), map[string]any{}},
		},
		{
			"encodes an event (with an integer as event name)",
			Packet{Type: Event, Namespace: "/", Data: []any{1, "a", map[string]any{}}},
			`2[1,"a",{}]`,
			[]any{float64(1), "a", map[string]any{}},
		},
		{
			"encodes an event (with ack)",
			Packet{Type: Event, Namespace: "/test", ID: one, Data: []any{"a", 1, map[string]any{}}},
			`2/test,1["a",1,{}]`,
			[]any{"a", float64(1), map[string]any{}},
		},
		{
			"encodes an ack",
			Packet{Type: Ack, Namespace: "/", ID: oneTwoThree, Data: []any{"a", 1, map[string]any{}}},
			`3123["a",1,{}]`,
			[]any{"a", float64(1), map[string]any{}},
		},
		{
			"encodes a connect error",
			Packet{Type: ConnectError, Namespace: "/", Data: "Unauthorized"},
			`4"Unauthorized"`,
			"Unauthorized",
		},
		{
			"encodes a connect error (with object)",
			Packet{Type: ConnectError, Namespace: "/", Data: map[string]any{"message": "Unauthorized"}},
			`4{"message":"Unauthorized"}`,
			map[string]any{"message": "Unauthorized"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := c.pkt.Encode()
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			if got != c.wire {
				t.Fatalf("Encode = %q, want %q", got, c.wire)
			}
			dec, err := DecodePacket(c.wire)
			if err != nil {
				t.Fatalf("DecodePacket(%q): %v", c.wire, err)
			}
			if dec.Type != c.pkt.Type {
				t.Errorf("Type = %v, want %v", dec.Type, c.pkt.Type)
			}
			wantNsp := c.pkt.Namespace
			if wantNsp == "" {
				wantNsp = "/"
			}
			if dec.Namespace != wantNsp {
				t.Errorf("Namespace = %q, want %q", dec.Namespace, wantNsp)
			}
			if !reflect.DeepEqual(dec.Data, c.data) {
				t.Errorf("Data = %#v, want %#v", dec.Data, c.data)
			}
			if (dec.ID == nil) != (c.pkt.ID == nil) {
				t.Errorf("ID presence = %v, want %v", dec.ID != nil, c.pkt.ID != nil)
			} else if dec.ID != nil && *dec.ID != *c.pkt.ID {
				t.Errorf("ID = %d, want %d", *dec.ID, *c.pkt.ID)
			}
		})
	}
}

// TestParityParserEncodeCircular mirrors "throws an error when encoding circular
// objects". Go's deconstruct/json path cannot represent a self-referential value
// the way JavaScript can, so this exercises the equivalent contract — the
// Encoder returns an error rather than a frame for an un-encodable payload —
// using a channel value that json.Marshal rejects.
func TestParityParserEncodeCircular(t *testing.T) {
	pkt := Packet{Type: Event, ID: u64(1), Namespace: "/", Data: []any{"ev", make(chan int)}}
	if _, err := NewEncoder().Encode(pkt); err == nil {
		t.Fatal("Encoder.Encode of an un-encodable payload = nil error, want error")
	}
}

// TestParityParserBadBinaryPacket mirrors "decodes a bad binary packet": a
// BINARY_EVENT header ("5") with no "<n>-" attachments prefix is illegal.
func TestParityParserBadBinaryPacket(t *testing.T) {
	if _, err := NewDecoder().Add("5"); err != ErrInvalidPacket {
		t.Fatalf("Decoder.Add(%q) = %v, want ErrInvalidPacket", "5", err)
	}
}

// TestParityParserInvalidPayload mirrors "throw an error upon parsing error":
// the exact malformed strings socket.io-parser rejects with "invalid payload"
// and the "unknown packet type" case.
func TestParityParserInvalidPayload(t *testing.T) {
	bad := []string{
		`442["some","data"`, // CONNECT_ERROR with truncated JSON
		`0/admin,"invalid"`, // CONNECT payload must be an object, not a string
		`1/admin,{}`,        // DISCONNECT must not carry a payload
		`2/admin,"invalid`,  // EVENT with truncated JSON
		`2/admin,{}`,        // EVENT payload must be an array
		`999`,               // unknown packet type 9
	}
	for _, s := range bad {
		if _, err := DecodePacket(s); err != ErrInvalidPacket {
			t.Errorf("DecodePacket(%q) = %v, want ErrInvalidPacket", s, err)
		}
		// The streaming Decoder must reject them too.
		if _, err := NewDecoder().Add(s); err == nil {
			t.Errorf("Decoder.Add(%q) = nil error, want rejection", s)
		}
	}
}

// TestParityParserBinary covers socket.io-parser/test/buffer.js: a BINARY_EVENT
// and a BINARY_ACK carrying a Buffer, asserting the exact multi-frame wire form
// and a full encode→decode reconstruction of the []byte payload.
func TestParityParserBinary(t *testing.T) {
	cases := []struct {
		name    string
		pkt     Packet
		header  string
		buffers [][]byte
	}{
		{
			"encodes a Buffer",
			Packet{Type: Event, Namespace: "/cool", ID: u64(23), Data: []any{"a", []byte("abc")}},
			`51-/cool,23["a",{"_placeholder":true,"num":0}]`,
			[][]byte{[]byte("abc")},
		},
		{
			"encodes a binary ack with Buffer",
			Packet{Type: Ack, Namespace: "/back", ID: u64(127), Data: []any{"a", []byte("xxx"), map[string]any{}}},
			`61-/back,127["a",{"_placeholder":true,"num":0},{}]`,
			[][]byte{[]byte("xxx")},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			frames, err := NewEncoder().Encode(c.pkt)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			if len(frames) != 1+len(c.buffers) {
				t.Fatalf("frames = %d, want %d", len(frames), 1+len(c.buffers))
			}
			header, ok := frames[0].(string)
			if !ok {
				t.Fatalf("frame[0] type = %T, want string", frames[0])
			}
			if header != c.header {
				t.Fatalf("header = %q, want %q", header, c.header)
			}
			for i, b := range c.buffers {
				got, ok := frames[i+1].([]byte)
				if !ok {
					t.Fatalf("frame[%d] type = %T, want []byte", i+1, frames[i+1])
				}
				if !reflect.DeepEqual(got, b) {
					t.Fatalf("frame[%d] = %v, want %v", i+1, got, b)
				}
			}

			// Reassemble via the streaming Decoder and confirm the []byte
			// payload is reconstructed in place.
			dec := NewDecoder()
			var out *Packet
			for _, f := range frames {
				p, err := dec.Add(f)
				if err != nil {
					t.Fatalf("Decoder.Add: %v", err)
				}
				if p != nil {
					out = p
				}
			}
			if out == nil {
				t.Fatal("Decoder produced no packet")
			}
			// socket.io-parser rewrites a reassembled BINARY_EVENT/BINARY_ACK back
			// to its plain EVENT/ACK type, so the decoded packet keeps the
			// original (non-binary) type.
			wantType := c.pkt.Type
			if out.Type != wantType {
				t.Errorf("decoded Type = %v, want %v", out.Type, wantType)
			}
			if !reflect.DeepEqual(out.Data, c.pkt.Data) {
				t.Errorf("decoded Data = %#v, want %#v", out.Data, c.pkt.Data)
			}
		})
	}
}
