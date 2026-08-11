package redis

import (
	"bufio"
	"bytes"
	"errors"
	"net"
	"strings"
	"testing"
)

// TestReadReplyRejectsOversizedDeclarations covers the length fields that arrive
// on the wire *before* the data they describe. Each of these strings is only a
// few bytes long, yet the pre-fix decoder used the declared length to size an
// allocation directly: "$9223372036854775807" reached make([]byte, n) and
// panicked ("runtime error: makeslice: len out of range" — confirmed by running
// make([]byte, 1<<63-1) under recover), and "*2000000000" reached
// make([]any, n), a request for 2e9 * 16 = 32 GB of pointers.
//
// The expected values are the two exported bounds, not invented constants:
// MaxBulkLength+1 and MaxArrayLength+1 are the smallest declarations that must
// be refused, and MaxBulkLength / MaxArrayLength themselves must still parse
// (they are rejected later, by the read that runs out of input).
func TestReadReplyRejectsOversizedDeclarations(t *testing.T) {
	tests := []struct {
		name string
		wire string
	}{
		{"bulk length int64 max", "$9223372036854775807\r\n"},
		{"bulk length just over limit", "$" + itoa(MaxBulkLength+1) + "\r\n"},
		{"array length just over limit", "*" + itoa(MaxArrayLength+1) + "\r\n"},
		{"array length 2 billion", "*2000000000\r\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := bufio.NewReader(strings.NewReader(tt.wire))
			_, err := readReply(r)
			if !errors.Is(err, ErrReplyTooLarge) {
				t.Fatalf("readReply(%q) error = %v, want ErrReplyTooLarge", tt.wire, err)
			}
		})
	}
}

// TestReadReplyRejectsNonNumericLengths pins the silently-wrong behavior the
// ignored strconv errors used to produce: "$abc" parsed as length 0 and yielded
// an empty bulk string instead of failing, so a corrupt stream was reported as
// valid data.
func TestReadReplyRejectsNonNumericLengths(t *testing.T) {
	for _, wire := range []string{"$abc\r\n", "*xyz\r\n", ":notanint\r\n"} {
		r := bufio.NewReader(strings.NewReader(wire))
		if got, err := readReply(r); err == nil {
			t.Errorf("readReply(%q) = %#v, nil error; want an error", wire, got)
		}
	}
}

// TestReadReplyRejectsDeepNesting feeds maxReplyDepth+2 nested one-element
// array headers. Each "*1\r\n" is four bytes, so without a depth bound a peer
// could recurse readReply until the goroutine stack was exhausted, using four
// bytes of input per frame.
func TestReadReplyRejectsDeepNesting(t *testing.T) {
	var b bytes.Buffer
	for i := 0; i < maxReplyDepth+2; i++ {
		b.WriteString("*1\r\n")
	}
	b.WriteString(":1\r\n")
	r := bufio.NewReader(&b)
	if _, err := readReply(r); !errors.Is(err, ErrReplyTooLarge) {
		t.Fatalf("readReply(deeply nested) error = %v, want ErrReplyTooLarge", err)
	}
}

// TestReadReplyAcceptsNestedArrayWithinLimit is the negative control for the
// depth bound: nesting well inside the limit must still decode.
func TestReadReplyAcceptsNestedArrayWithinLimit(t *testing.T) {
	r := bufio.NewReader(strings.NewReader("*1\r\n*1\r\n$2\r\nhi\r\n"))
	got, err := readReply(r)
	if err != nil {
		t.Fatalf("readReply: %v", err)
	}
	outer, ok := got.([]any)
	if !ok || len(outer) != 1 {
		t.Fatalf("outer = %#v, want a 1-element array", got)
	}
	inner, ok := outer[0].([]any)
	if !ok || len(inner) != 1 {
		t.Fatalf("inner = %#v, want a 1-element array", outer[0])
	}
	if b, ok := inner[0].([]byte); !ok || string(b) != "hi" {
		t.Fatalf("inner[0] = %#v, want []byte(\"hi\")", inner[0])
	}
}

// TestPublishAfterCloseReturnsErrClosed pins the contract the socketio
// broadcast operator relies on to degrade to local delivery: a closed
// broadcaster reports a definite error rather than appearing to succeed.
func TestPublishAfterCloseReturnsErrClosed(t *testing.T) {
	// A listener that accepts and ignores: New only writes AUTH/SELECT when
	// configured (they are not here) and does not wait for the SUBSCRIBE reply.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		var conns []net.Conn
		defer func() {
			for _, c := range conns {
				c.Close()
			}
		}()
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			conns = append(conns, c)
		}
	}()

	b, err := New(Options{Addr: ln.Addr().String()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := b.Publish([]byte("x")); !errors.Is(err, ErrClosed) {
		t.Fatalf("Publish after Close = %v, want ErrClosed", err)
	}
	// Close is idempotent.
	if err := b.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [24]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
