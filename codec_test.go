package socketio

import (
	"reflect"
	"testing"
)

func TestEncoderPlainEvent(t *testing.T) {
	enc := NewEncoder()
	id := uint64(7)
	p := Packet{Type: Event, Namespace: "/", ID: &id, Data: []any{"hello", "world"}}
	frames, err := enc.Encode(p)
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != 1 {
		t.Fatalf("plain event should encode to 1 frame, got %d", len(frames))
	}
	if s, ok := frames[0].(string); !ok || s != `27["hello","world"]` {
		t.Fatalf("frame = %#v, want text header", frames[0])
	}
}

func TestEncoderBinaryEvent(t *testing.T) {
	enc := NewEncoder()
	p := Packet{Type: Event, Namespace: "/", Data: []any{"blob", []byte{0x01, 0x02, 0x03}}}
	frames, err := enc.Encode(p)
	if err != nil {
		t.Fatal(err)
	}
	// header frame + one binary attachment frame.
	if len(frames) != 2 {
		t.Fatalf("binary event should encode to 2 frames, got %d", len(frames))
	}
	header, ok := frames[0].(string)
	if !ok || header[0] != '5' { // 5 == BINARY_EVENT
		t.Fatalf("header = %#v, want BINARY_EVENT text", frames[0])
	}
	if buf, ok := frames[1].([]byte); !ok || !reflect.DeepEqual(buf, []byte{0x01, 0x02, 0x03}) {
		t.Fatalf("attachment = %#v, want raw buffer", frames[1])
	}
}

func TestDecoderRoundTripPlain(t *testing.T) {
	enc := NewEncoder()
	dec := NewDecoder()
	id := uint64(3)
	in := Packet{Type: Event, Namespace: "/admin", ID: &id, Data: []any{"tick", float64(42)}}

	frames, err := enc.Encode(in)
	if err != nil {
		t.Fatal(err)
	}
	pkt, err := dec.Add(frames[0])
	if err != nil {
		t.Fatal(err)
	}
	if pkt == nil {
		t.Fatal("plain event should complete on the first frame")
	}
	if pkt.EventName() != "tick" || pkt.Namespace != "/admin" || pkt.ID == nil || *pkt.ID != 3 {
		t.Fatalf("decoded = %+v", pkt)
	}
	if dec.Pending() {
		t.Fatal("decoder should not be pending after a plain packet")
	}
}

func TestDecoderRoundTripBinary(t *testing.T) {
	enc := NewEncoder()
	dec := NewDecoder()
	in := Packet{Type: Event, Namespace: "/", Data: []any{"upload", []byte{0xde, 0xad}, []byte{0xbe, 0xef}}}

	frames, err := enc.Encode(in)
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != 3 {
		t.Fatalf("two attachments expected, got %d frames", len(frames))
	}

	// The header frame yields no packet yet — attachments are pending.
	pkt, err := dec.Add(frames[0])
	if err != nil {
		t.Fatal(err)
	}
	if pkt != nil {
		t.Fatal("packet should not complete before its attachments arrive")
	}
	if !dec.Pending() {
		t.Fatal("decoder should be pending after a binary header")
	}
	// First attachment: still pending.
	if pkt, err = dec.Add(frames[1]); err != nil || pkt != nil {
		t.Fatalf("first attachment: pkt=%v err=%v", pkt, err)
	}
	// Second attachment completes the packet.
	pkt, err = dec.Add(frames[2])
	if err != nil {
		t.Fatal(err)
	}
	if pkt == nil {
		t.Fatal("packet should complete on the last attachment")
	}
	// A reassembled binary packet is rewritten to a plain EVENT (matching
	// socket.io-parser's Decoder, which promotes BINARY_EVENT→EVENT once every
	// attachment has arrived).
	if pkt.Type != Event {
		t.Fatalf("type = %v, want Event", pkt.Type)
	}
	args := pkt.Args()
	if len(args) != 2 {
		t.Fatalf("args = %v", args)
	}
	if !reflect.DeepEqual(args[0], []byte{0xde, 0xad}) || !reflect.DeepEqual(args[1], []byte{0xbe, 0xef}) {
		t.Fatalf("reconstructed buffers = %#v", args)
	}
}

func TestDecoderErrors(t *testing.T) {
	dec := NewDecoder()
	// A binary attachment with no pending header is an error.
	if _, err := dec.Add([]byte{0x00}); err != ErrInvalidPacket {
		t.Fatalf("stray attachment err = %v, want ErrInvalidPacket", err)
	}
	// An unsupported frame type is an error.
	if _, err := dec.Add(123); err != ErrInvalidPacket {
		t.Fatalf("bad frame err = %v, want ErrInvalidPacket", err)
	}
	// A second header while mid-packet is an error.
	if _, err := dec.Add(`51-["x",{"_placeholder":true,"num":0}]`); err != nil {
		t.Fatal(err)
	}
	if _, err := dec.Add(`2["y"]`); err != ErrInvalidPacket {
		t.Fatalf("header while pending err = %v, want ErrInvalidPacket", err)
	}
	dec.Reset()
	if dec.Pending() {
		t.Fatal("Reset should clear pending state")
	}
}

func TestHasBinaryData(t *testing.T) {
	if (Packet{Data: []any{"x", float64(1)}}).HasBinaryData() {
		t.Fatal("plain payload reported as binary")
	}
	if !(Packet{Data: []any{"x", []byte{1}}}).HasBinaryData() {
		t.Fatal("binary payload not detected")
	}
}

func BenchmarkEncoderBinary(b *testing.B) {
	enc := NewEncoder()
	p := Packet{Type: Event, Namespace: "/", Data: []any{"blob", []byte{1, 2, 3, 4, 5, 6, 7, 8}}}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := enc.Encode(p); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecoderBinary(b *testing.B) {
	enc := NewEncoder()
	frames, _ := enc.Encode(Packet{Type: Event, Namespace: "/", Data: []any{"blob", []byte{1, 2, 3, 4}}})
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		dec := NewDecoder()
		for _, f := range frames {
			if _, err := dec.Add(f); err != nil {
				b.Fatal(err)
			}
		}
	}
}
