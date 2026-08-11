package socketio

import (
	"reflect"
	"testing"
)

func TestPacketTypeString(t *testing.T) {
	cases := []struct {
		t    PacketType
		want string
	}{
		{Connect, "CONNECT"},
		{Disconnect, "DISCONNECT"},
		{Event, "EVENT"},
		{Ack, "ACK"},
		{ConnectError, "CONNECT_ERROR"},
		{BinaryEvent, "BINARY_EVENT"},
		{BinaryAck, "BINARY_ACK"},
		{PacketType(99), "UNKNOWN"},
	}
	for _, c := range cases {
		if got := c.t.String(); got != c.want {
			t.Errorf("PacketType(%d).String() = %q, want %q", c.t, got, c.want)
		}
	}
}

func TestEncodeInvalidType(t *testing.T) {
	if _, err := (Packet{Type: PacketType(9)}).Encode(); err != ErrInvalidPacket {
		t.Fatalf("Encode invalid type = %v, want ErrInvalidPacket", err)
	}
}

func TestEncodeConnectError(t *testing.T) {
	got, err := (Packet{Type: ConnectError, Data: map[string]any{"message": "nope"}}).Encode()
	if err != nil {
		t.Fatal(err)
	}
	if got != `4{"message":"nope"}` {
		t.Fatalf("Encode = %q", got)
	}
}

func TestDecodeConnectError(t *testing.T) {
	p, err := DecodePacket(`4/admin,{"message":"nope"}`)
	if err != nil {
		t.Fatal(err)
	}
	if p.Type != ConnectError || p.Namespace != "/admin" {
		t.Fatalf("got %+v", p)
	}
	obj, ok := p.Data.(map[string]any)
	if !ok || obj["message"] != "nope" {
		t.Fatalf("data = %v", p.Data)
	}
}

func TestBinaryEventEncodeDecode(t *testing.T) {
	id := uint64(7)
	p := Packet{
		Type:        BinaryEvent,
		Namespace:   "/admin",
		ID:          &id,
		attachments: 2,
		Data:        []any{"ev", map[string]any{"_placeholder": true, "num": 0}},
	}
	wire, err := p.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if wire != `52-/admin,7["ev",{"_placeholder":true,"num":0}]` {
		t.Fatalf("Encode = %q", wire)
	}
	dec, err := DecodePacket(wire)
	if err != nil {
		t.Fatal(err)
	}
	if dec.Type != BinaryEvent {
		t.Errorf("Type = %v", dec.Type)
	}
	if dec.Attachments() != 2 {
		t.Errorf("Attachments = %d, want 2", dec.Attachments())
	}
	if dec.Namespace != "/admin" {
		t.Errorf("Namespace = %q", dec.Namespace)
	}
	if dec.ID == nil || *dec.ID != 7 {
		t.Errorf("ID = %v", dec.ID)
	}
}

func TestDecodeBinaryAck(t *testing.T) {
	p, err := DecodePacket(`61-5[{"_placeholder":true,"num":0}]`)
	if err != nil {
		t.Fatal(err)
	}
	if p.Type != BinaryAck || p.Attachments() != 1 {
		t.Fatalf("got %+v attachments=%d", p, p.Attachments())
	}
	if p.ID == nil || *p.ID != 5 {
		t.Fatalf("ID = %v", p.ID)
	}
}

func TestDecodeNamespaceNoComma(t *testing.T) {
	// Namespace runs to the end of the string with no trailing comma or payload.
	p, err := DecodePacket(`0/admin`)
	if err != nil {
		t.Fatal(err)
	}
	if p.Type != Connect || p.Namespace != "/admin" {
		t.Fatalf("got %+v", p)
	}
	if p.Data != nil {
		t.Errorf("Data = %v, want nil", p.Data)
	}
}

func TestDecodeBadJSON(t *testing.T) {
	if _, err := DecodePacket(`2[not-json`); err == nil {
		t.Fatal("expected JSON error")
	}
}

func TestDecodeAckIDOverflow(t *testing.T) {
	// A digit run too large for uint64 must be rejected.
	if _, err := DecodePacket(`299999999999999999999999["ev"]`); err != ErrInvalidPacket {
		t.Fatalf("overflow ack id = %v, want ErrInvalidPacket", err)
	}
}

func TestArgsForAck(t *testing.T) {
	id := uint64(1)
	p := Packet{Type: Ack, ID: &id, Data: []any{"a", "b"}}
	args := p.Args()
	if len(args) != 2 || args[0] != "a" || args[1] != "b" {
		t.Fatalf("Args = %v, want full array", args)
	}
}

func TestArgsNonArray(t *testing.T) {
	p := Packet{Type: Event, Data: map[string]any{"x": 1}}
	if p.Args() != nil {
		t.Errorf("Args = %v, want nil", p.Args())
	}
	if p.EventName() != "" {
		t.Errorf("EventName = %q, want empty", p.EventName())
	}
}

func TestEventNameNonStringFirst(t *testing.T) {
	p := Packet{Type: Event, Data: []any{float64(1), "x"}}
	if p.EventName() != "" {
		t.Errorf("EventName = %q, want empty", p.EventName())
	}
}

func TestNewEvent(t *testing.T) {
	id := uint64(3)
	p := newEvent("/chat", "msg", []any{"hi", 42}, &id)
	if p.Type != Event || p.Namespace != "/chat" {
		t.Fatalf("got %+v", p)
	}
	if p.EventName() != "msg" {
		t.Errorf("EventName = %q", p.EventName())
	}
	args := p.Args()
	if len(args) != 2 || args[0] != "hi" || args[1] != 42 {
		t.Errorf("Args = %v", args)
	}
}

func TestDeconstructReconstruct(t *testing.T) {
	buf := []byte{0x01, 0x02, 0x03}
	data := []any{"ev", map[string]any{"file": buf}, buf}
	rewritten, buffers := deconstruct(data)
	if len(buffers) != 2 {
		t.Fatalf("buffers = %d, want 2", len(buffers))
	}
	// The rewritten form must contain placeholder markers, not raw bytes.
	if !hasPlaceholder(rewritten) {
		t.Fatal("rewritten payload missing placeholder markers")
	}
	if hasBinary(rewritten) {
		t.Fatal("rewritten payload should not contain raw []byte")
	}

	back, err := Reconstruct(rewritten, buffers)
	if err != nil {
		t.Fatalf("Reconstruct: %v", err)
	}
	arr, ok := back.([]any)
	if !ok || len(arr) != 3 {
		t.Fatalf("reconstructed = %v", back)
	}
	if !reflect.DeepEqual(arr[2], buf) {
		t.Fatalf("reconstructed buffer = %v, want %v", arr[2], buf)
	}
	obj := arr[1].(map[string]any)
	if !reflect.DeepEqual(obj["file"], buf) {
		t.Fatalf("reconstructed nested buffer = %v", obj["file"])
	}
}

func hasPlaceholder(v any) bool {
	switch x := v.(type) {
	case map[string]any:
		if ph, ok := x["_placeholder"].(bool); ok && ph {
			return true
		}
		for _, vv := range x {
			if hasPlaceholder(vv) {
				return true
			}
		}
	case []any:
		for _, vv := range x {
			if hasPlaceholder(vv) {
				return true
			}
		}
	}
	return false
}

func TestEncodeBinaryPromotesEvent(t *testing.T) {
	p := Packet{Type: Event, Data: []any{"ev", []byte{0x09, 0x08}}}
	text, buffers, err := p.EncodeBinary()
	if err != nil {
		t.Fatal(err)
	}
	if len(buffers) != 1 {
		t.Fatalf("buffers = %d, want 1", len(buffers))
	}
	// Promoted to BINARY_EVENT (type 5) with a "1-" attachment prefix.
	if text != `51-["ev",{"_placeholder":true,"num":0}]` {
		t.Fatalf("EncodeBinary text = %q", text)
	}
}

func TestEncodeBinaryPromotesAck(t *testing.T) {
	id := uint64(4)
	p := Packet{Type: Ack, ID: &id, Data: []any{[]byte{0x01}}}
	text, buffers, err := p.EncodeBinary()
	if err != nil {
		t.Fatal(err)
	}
	if len(buffers) != 1 {
		t.Fatalf("buffers = %d, want 1", len(buffers))
	}
	if text != `61-4[{"_placeholder":true,"num":0}]` {
		t.Fatalf("EncodeBinary text = %q", text)
	}
}

func TestEncodeBinaryNoBuffers(t *testing.T) {
	p := Packet{Type: Event, Data: []any{"ev", "plain"}}
	text, buffers, err := p.EncodeBinary()
	if err != nil {
		t.Fatal(err)
	}
	if buffers != nil {
		t.Fatalf("buffers = %v, want nil", buffers)
	}
	if text != `2["ev","plain"]` {
		t.Fatalf("text = %q", text)
	}
}

func TestReconstructAsIntTypes(t *testing.T) {
	buffers := [][]byte{{0xaa}, {0xbb}}
	for _, num := range []any{int(0), int64(1), float64(1)} {
		data := map[string]any{"_placeholder": true, "num": num}
		got, err := reconstruct(data, buffers)
		if err != nil {
			t.Fatalf("num=%v (%T): %v", num, num, err)
		}
		b, ok := got.([]byte)
		if !ok {
			t.Fatalf("num=%v (%T): got %v, want []byte", num, num, got)
		}
		_ = b
	}
	// Out-of-range placeholder is rejected (upstream throws "illegal
	// attachments" rather than handing the raw placeholder to the application).
	data := map[string]any{"_placeholder": true, "num": 99}
	if _, err := reconstruct(data, buffers); err == nil {
		t.Fatal("out-of-range placeholder should be rejected")
	}
}

func TestHasBinary(t *testing.T) {
	if !hasBinary([]any{"x", map[string]any{"k": []byte{1}}}) {
		t.Error("hasBinary should detect nested []byte")
	}
	if hasBinary([]any{"x", map[string]any{"k": "s"}}) {
		t.Error("hasBinary false positive")
	}
}
