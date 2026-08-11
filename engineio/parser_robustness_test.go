package engineio_test

import (
	"strings"
	"testing"

	"github.com/malcolmston/socketio/engineio"
)

// malformedEIO is a corpus of broken Engine.IO packet strings. Every entry must
// return an error rather than panic: Decode runs on every frame an
// unauthenticated peer sends, so a panic there is a remote process kill.
//
// Note the byte arithmetic that makes the type check total: Decode computes
// PacketType(s[0] - '0') on a byte, so '/' (0x2f) becomes 0xff and '\x00'
// becomes 0xd0 — both well above Noop (6) — and every non-digit lands outside
// the valid range instead of wrapping into it.
var malformedEIO = []string{
	"",  // empty packet
	"7", // type 7 is past Noop
	"9",
	"/",
	"\x00",
	"\xff",
	"a",
	"-1",
	"b!!!!", // "b" prefix with invalid base64
	"b====",
	"b" + strings.Repeat("A", 3), // base64 length not a multiple of 4
}

// TestDecodeRejectsMalformedPackets asserts the corpus errors cleanly.
func TestDecodeRejectsMalformedPackets(t *testing.T) {
	for _, s := range malformedEIO {
		if p, err := engineio.Decode(s); err == nil {
			t.Errorf("Decode(%q) = %+v, nil error; want an error", s, p)
		}
	}
}

// TestDecodeNeverPanics sweeps every byte value as a one-byte packet and as a
// packet type prefix, and pushes the malformed corpus through both Decode and
// DecodePayload.
func TestDecodeNeverPanics(t *testing.T) {
	var inputs []string
	for b := 0; b < 256; b++ {
		s := string(rune(b))
		inputs = append(inputs, s, s+"data", s+"\x1e"+s)
	}
	inputs = append(inputs, malformedEIO...)
	for _, s := range inputs {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("Decode(%q) panicked: %v", s, r)
				}
			}()
			_, _ = engineio.Decode(s)
		}()
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("DecodePayload(%q) panicked: %v", s, r)
				}
			}()
			_, _ = engineio.DecodePayload(s)
		}()
	}
}

// TestDecodePayloadRejectsEmptyRecord pins that a payload containing an empty
// record (a stray or doubled 0x1e separator) is rejected rather than silently
// yielding a zero-valued OPEN packet — Decode's ErrEmptyPacket propagates.
func TestDecodePayloadRejectsEmptyRecord(t *testing.T) {
	for _, s := range []string{"\x1e", "4a\x1e", "\x1e4a", "4a\x1e\x1e4b"} {
		if _, err := engineio.DecodePayload(s); err == nil {
			t.Errorf("DecodePayload(%q) = nil error; want an error", s)
		}
	}
}

// TestPayloadRoundTripAllTypes is a known-answer test for the Engine.IO v4
// polling payload: each packet is "<digit><data>", records are joined with the
// ASCII record separator 0x1e, and a binary packet is serialized as "b" plus
// standard base64. The expected string is written out literally rather than
// derived from the encoder, so an encoder change cannot silently redefine it.
//
// base64("\x01\x02\x03") = "AQID": 0x01,0x02,0x03 = 000000 010000 001000 000011
// → indices 0,16,8,3 → 'A','Q','I','D', with no padding (3 bytes is a whole
// 24-bit group).
func TestPayloadRoundTripAllTypes(t *testing.T) {
	packets := []engineio.Packet{
		{Type: engineio.Open, Data: `{"sid":"x"}`},
		{Type: engineio.Ping, Data: "probe"},
		{Type: engineio.Pong, Data: "probe"},
		engineio.NewMessage("hello"),
		{Type: engineio.Upgrade},
		{Type: engineio.Noop},
		{Type: engineio.Close},
		{Type: engineio.Message, Binary: []byte{1, 2, 3}},
	}
	const want = `0{"sid":"x"}` + "\x1e" + "2probe" + "\x1e" + "3probe" + "\x1e" +
		"4hello" + "\x1e" + "5" + "\x1e" + "6" + "\x1e" + "1" + "\x1e" + "bAQID"

	got := engineio.EncodePayload(packets)
	if got != want {
		t.Fatalf("EncodePayload =\n%q\nwant\n%q", got, want)
	}

	back, err := engineio.DecodePayload(got)
	if err != nil {
		t.Fatalf("DecodePayload: %v", err)
	}
	if len(back) != len(packets) {
		t.Fatalf("decoded %d packets, want %d", len(back), len(packets))
	}
	for i := range packets {
		if back[i].Type != packets[i].Type {
			t.Errorf("packet %d type = %v, want %v", i, back[i].Type, packets[i].Type)
		}
		if back[i].Data != packets[i].Data {
			t.Errorf("packet %d data = %q, want %q", i, back[i].Data, packets[i].Data)
		}
		if string(back[i].Binary) != string(packets[i].Binary) {
			t.Errorf("packet %d binary = %v, want %v", i, back[i].Binary, packets[i].Binary)
		}
	}
}

// FuzzDecode keeps the "never panics, and anything that decodes re-encodes to
// something that decodes again" invariant under fuzzing.
func FuzzDecode(f *testing.F) {
	seeds := append([]string{
		"0{}", "1", "2", "2probe", "3probe", "4hello", "5", "6", "bAQID",
	}, malformedEIO...)
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		p, err := engineio.Decode(s)
		if err != nil {
			return
		}
		again, err := engineio.Decode(p.Encode())
		if err != nil {
			t.Fatalf("Decode(%q) round-trip failed: Encode() = %q, err %v", s, p.Encode(), err)
		}
		if again.Type != p.Type || again.Data != p.Data || string(again.Binary) != string(p.Binary) {
			t.Fatalf("round-trip of %q changed the packet: %+v -> %+v", s, p, again)
		}
		_ = p.Type.String()
	})
}
