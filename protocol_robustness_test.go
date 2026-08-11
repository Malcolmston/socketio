package socketio_test

import (
	"errors"
	"strconv"
	"strings"
	"testing"

	socketio "github.com/malcolmston/socketio"
)

// TestDecodeAttachmentCountBounds covers the "<n>-" attachment prefix of
// BINARY_EVENT / BINARY_ACK. The count is fully attacker-controlled and arrives
// before any buffer does, so it must be validated at decode time rather than
// trusted by the code that pre-allocates per-attachment storage
// (codec.go does `make([][]byte, 0, p.attachments)`).
//
// The boundary values are derived from the exported limit itself, not guessed:
// MaxAttachments must be accepted and MaxAttachments+1 refused.
func TestDecodeAttachmentCountBounds(t *testing.T) {
	max := socketio.MaxAttachments
	tests := []struct {
		name    string
		wire    string
		want    int
		wantErr error
	}{
		{"single attachment", `51-["a",{"_placeholder":true,"num":0}]`, 1, nil},
		// A BINARY_EVENT/BINARY_ACK must declare at least one attachment; upstream
		// rejects a "0-" header with "Illegal attachments".
		{"zero attachments", `50-["a"]`, 0, socketio.ErrInvalidPacket},
		{"exactly the limit", "5" + strconv.Itoa(max) + `-["a"]`, max, nil},
		{"one over the limit", "5" + strconv.Itoa(max+1) + `-["a"]`, 0, socketio.ErrTooManyAttachments},
		{"binary ack over the limit", "6" + strconv.Itoa(max+1) + `-["a"]`, 0, socketio.ErrTooManyAttachments},
		// 1<<63 does not fit in an int64; strconv.Atoi fails outright. It must
		// still be rejected rather than reaching make().
		{"beyond int64", `59223372036854775808-["a"]`, 0, socketio.ErrInvalidPacket},
		{"no attachment prefix", `5["a"]`, 0, socketio.ErrInvalidPacket},
		{"dash without digits", `5-["a"]`, 0, socketio.ErrInvalidPacket},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := socketio.DecodePacket(tt.wire)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("DecodePacket(%.40q) error = %v, want %v", tt.wire, err, tt.wantErr)
				}
				// Every attachment failure is also a malformed-packet failure,
				// so callers testing only for ErrInvalidPacket keep working.
				if !errors.Is(err, socketio.ErrInvalidPacket) {
					t.Errorf("error %v does not wrap ErrInvalidPacket", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("DecodePacket(%.40q): %v", tt.wire, err)
			}
			if p.Attachments() != tt.want {
				t.Errorf("Attachments() = %d, want %d", p.Attachments(), tt.want)
			}
		})
	}
}

// TestEncodeRejectsOversizedAttachmentCount is the encode-side mirror: a packet
// whose attachment count exceeds the limit must not be put on the wire, since a
// peer decoding it would reject the frame anyway.
func TestEncodeRejectsOversizedAttachmentCount(t *testing.T) {
	bufs := make([]any, 0, socketio.MaxAttachments+1)
	for i := 0; i <= socketio.MaxAttachments; i++ {
		bufs = append(bufs, []byte{byte(i)})
	}
	pkt := socketio.Packet{Type: socketio.Event, Namespace: "/", Data: append([]any{"ev"}, bufs...)}
	if _, err := socketio.NewEncoder().Encode(pkt); !errors.Is(err, socketio.ErrTooManyAttachments) {
		t.Fatalf("Encode with %d attachments = %v, want ErrTooManyAttachments", len(bufs), err)
	}
}

// malformedPackets is a corpus of hostile and merely broken Socket.IO frames.
// Every one must produce an error, and none may panic — a panic in the decoder
// is reachable from any unauthenticated client, so it takes the whole server
// process down.
var malformedPackets = []string{
	// "" is NOT malformed: it decodes to a CONNECT to the default namespace,
	// matching socket.io-parser (Number("") === 0 === CONNECT). See
	// payloadlessPackets.
	"7", // packet type out of range
	"9", // packet type out of range
	"/", // not a digit at all
	"\x00",
	"\xff",
	"2[",                    // truncated JSON
	"2[]",                   // EVENT payload must be non-empty
	"2{}",                   // EVENT payload must be an array
	"1{}",                   // DISCONNECT must carry nothing
	`0"str"`,                // CONNECT payload must be an object
	"3{}",                   // ACK payload must be an array
	"218446744073709551616", // ack id overflows uint64
	"2999999999999999999999999999",
	"5",  // BINARY_EVENT without the "<n>-" attachments prefix
	"5-", // ...and with a dash but no count
	"6",
	"6-",
	`51-{"a":1}`, // BINARY_EVENT payload must be an array
	"2,1[\"a\"]",
	strings.Repeat("2", 1000),
	"2[\"a\"" + strings.Repeat("]", 100),
}

// TestDecodePacketRejectsMalformedInput asserts the corpus above errors cleanly.
func TestDecodePacketRejectsMalformedInput(t *testing.T) {
	for _, s := range malformedPackets {
		p, err := socketio.DecodePacket(s)
		if err == nil {
			t.Errorf("DecodePacket(%q) = %+v, nil error; want an error", s, p)
		}
	}
}

// payloadlessPackets are packets with a well-formed header and *no* payload.
// socket.io-parser's decodeString only validates a payload when one is present
// ("if (str.charAt(++i)) { ... isPayloadValid ... }"), so a header-only EVENT or
// ACK is legal on the wire and decodes to a packet with nil Data. They are
// listed explicitly so the never-panics sweep does not quietly re-classify them
// as errors.
var payloadlessPackets = []string{
	"2",      // EVENT, no data
	"3",      // ACK, no data
	"4",      // CONNECT_ERROR, no data
	"2/nsp",  // namespace not comma-terminated: it runs to end of string
	"2/nsp,", // namespace, nothing after it
	"2/,1",   // default namespace, ack id 1
	"51-",    // BINARY_EVENT header awaiting one attachment
}

// TestDecodePacketAllowsMissingPayload pins that parity with socket.io-parser.
func TestDecodePacketAllowsMissingPayload(t *testing.T) {
	for _, s := range payloadlessPackets {
		p, err := socketio.DecodePacket(s)
		if err != nil {
			t.Errorf("DecodePacket(%q) = %v; a header with no payload is legal", s, err)
			continue
		}
		if p.Data != nil {
			t.Errorf("DecodePacket(%q).Data = %#v, want nil", s, p.Data)
		}
		// Accessors must tolerate the nil payload.
		if got := p.EventName(); got != "" {
			t.Errorf("DecodePacket(%q).EventName() = %q, want \"\"", s, got)
		}
		if got := p.Args(); got != nil {
			t.Errorf("DecodePacket(%q).Args() = %#v, want nil", s, got)
		}
	}
}

// TestDecodePacketNeverPanics sweeps every single-byte packet and every
// two-byte prefix over the interesting ASCII range, plus the malformed corpus,
// through both the one-shot decoder and the streaming Decoder. The assertion is
// simply "returns" — a panic here fails the test by crashing it.
func TestDecodePacketNeverPanics(t *testing.T) {
	var inputs []string
	for b := 0; b < 256; b++ {
		inputs = append(inputs, string(rune(b)))
	}
	for _, first := range "0123456789-,/[{\"b" {
		for _, second := range "0123456789-,/[{\"}]" {
			inputs = append(inputs,
				string(first)+string(second),
				string(first)+string(second)+`["a",1]`,
				string(first)+string(second)+"-/x,7[]",
			)
		}
	}
	inputs = append(inputs, malformedPackets...)
	inputs = append(inputs, payloadlessPackets...)

	for _, s := range inputs {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("DecodePacket(%q) panicked: %v", s, r)
				}
			}()
			_, _ = socketio.DecodePacket(s)
		}()
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("Decoder.Add(%q) panicked: %v", s, r)
				}
			}()
			_, _ = socketio.NewDecoder().Add(s)
		}()
	}
}

// FuzzDecodePacket keeps the "never panics, and anything that decodes
// re-encodes" invariant under fuzzing. Under a plain `go test` it runs the seed
// corpus; `go test -fuzz=FuzzDecodePacket` explores beyond it.
func FuzzDecodePacket(f *testing.F) {
	seeds := append([]string{
		"0", "0/admin,", `0/admin,{"token":"123"}`,
		"1", "1/admin,",
		`2["hello",1]`, `2/admin,456["project:delete",123]`,
		"3/admin,456[]",
		`4/admin,{"message":"Not authorized"}`,
		`51-["hello",{"_placeholder":true,"num":0}]`,
		`61-/admin,456[{"_placeholder":true,"num":0}]`,
	}, malformedPackets...)
	seeds = append(seeds, payloadlessPackets...)
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		p, err := socketio.DecodePacket(s)
		if err != nil {
			return
		}
		// A packet that decoded must be re-encodable: Encode only fails on a
		// payload json.Marshal rejects, and everything DecodePacket produces
		// came out of json.Unmarshal.
		if _, err := p.Encode(); err != nil {
			t.Fatalf("DecodePacket(%q) succeeded but Encode failed: %v", s, err)
		}
		// Accessors must be total, whatever the payload shape.
		_ = p.EventName()
		_ = p.Args()
		_ = p.Attachments()
		_ = p.Type.String()
	})
}
