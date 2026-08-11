package socketio

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// ProtocolVersion is the Socket.IO protocol revision implemented here (v5,
// which rides on Engine.IO v4).
const ProtocolVersion = 5

// PacketType identifies a Socket.IO packet.
type PacketType byte

const (
	// Connect initiates a namespace connection.
	Connect PacketType = iota
	// Disconnect leaves a namespace.
	Disconnect
	// Event carries an application event and its arguments.
	Event
	// Ack answers an Event that requested acknowledgement.
	Ack
	// ConnectError reports a failed namespace connection.
	ConnectError
	// BinaryEvent is an Event with binary attachments (decoded as text here).
	BinaryEvent
	// BinaryAck is an Ack with binary attachments (decoded as text here).
	BinaryAck
)

// String returns the Socket.IO name of the packet type (e.g. "EVENT", "ACK"),
// or "UNKNOWN" for an unrecognized value.
func (t PacketType) String() string {
	switch t {
	case Connect:
		return "CONNECT"
	case Disconnect:
		return "DISCONNECT"
	case Event:
		return "EVENT"
	case Ack:
		return "ACK"
	case ConnectError:
		return "CONNECT_ERROR"
	case BinaryEvent:
		return "BINARY_EVENT"
	case BinaryAck:
		return "BINARY_ACK"
	default:
		return "UNKNOWN"
	}
}

// Packet is a decoded Socket.IO protocol packet.
type Packet struct {
	// Type is the packet's Socket.IO type (Connect, Event, Ack, ...).
	Type PacketType
	// Namespace the packet targets; defaults to "/".
	Namespace string
	// ID is the acknowledgement id when the packet requests or answers an ack;
	// nil otherwise.
	ID *uint64
	// Data is the decoded JSON payload: an array for Event/Ack ([name, args...]
	// or [args...]) and an object for Connect/ConnectError.
	Data any
	// attachments is the number of binary buffers a BINARY_EVENT/BINARY_ACK
	// packet carries (parsed from the "<n>-" wire prefix).
	attachments int
}

// Attachments returns the declared number of binary attachments for a
// BINARY_EVENT/BINARY_ACK packet.
func (p Packet) Attachments() int { return p.attachments }

// MaxAttachments bounds the number of binary buffers a single BINARY_EVENT or
// BINARY_ACK packet may declare in its "<n>-" wire prefix. The count is
// attacker-controlled — it arrives on the wire before any buffer does — so it is
// validated at decode time rather than trusted by the code that pre-allocates
// per-attachment storage. A packet declaring more than this is rejected with
// ErrInvalidPacket. The limit is far above any realistic payload (socket.io's
// own encoder emits one attachment per []byte argument).
const MaxAttachments = 1024

// ErrInvalidPacket indicates a malformed Socket.IO packet.
var ErrInvalidPacket = errors.New("socketio: invalid packet")

// ErrTooManyAttachments indicates a BINARY_EVENT/BINARY_ACK packet declaring
// more than MaxAttachments binary buffers. It wraps ErrInvalidPacket so callers
// that only care about malformed input can keep testing for that.
var ErrTooManyAttachments = fmt.Errorf("%w: too many binary attachments (max %d)", ErrInvalidPacket, MaxAttachments)

// Encode renders a packet to its Socket.IO wire form (the string carried inside
// an Engine.IO MESSAGE packet).
func (p Packet) Encode() (string, error) {
	if p.Type > BinaryAck {
		return "", ErrInvalidPacket
	}
	var b strings.Builder
	b.WriteByte('0' + byte(p.Type))

	if p.attachments > MaxAttachments {
		return "", ErrTooManyAttachments
	}
	// Binary attachment count prefix for binary packet types.
	if (p.Type == BinaryEvent || p.Type == BinaryAck) && p.attachments > 0 {
		b.WriteString(strconv.Itoa(p.attachments))
		b.WriteByte('-')
	}

	if p.Namespace != "" && p.Namespace != "/" {
		b.WriteString(p.Namespace)
		b.WriteByte(',')
	}
	if p.ID != nil {
		b.WriteString(strconv.FormatUint(*p.ID, 10))
	}
	if p.Data != nil {
		j, err := marshalPayload(p.Data)
		if err != nil {
			return "", err
		}
		b.Write(j)
	}
	return b.String(), nil
}

// marshalPayload serialises a Socket.IO payload to the same bytes the reference
// socket.io-parser (JavaScript JSON.stringify) would produce:
//
//   - HTML metacharacters (<, >, &) are emitted raw, not \u-escaped. Go's
//     encoding/json escapes them by default; JSON.stringify does not, and the
//     frame is compared byte-for-byte on the wire, so any signature or digest
//     taken over it must match.
//   - negative zero (-0) is emitted as 0, matching JSON.stringify(-0) === "0";
//     Go's json.Marshal would otherwise write "-0".
func marshalPayload(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(normalizeNegZero(v)); err != nil {
		return nil, err
	}
	// json.Encoder appends a trailing newline; the wire form has none.
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

// normalizeNegZero returns a copy of v with every negative-zero float64 replaced
// by positive zero, so the encoded frame matches JSON.stringify. It only copies
// the containers it needs to touch.
func normalizeNegZero(v any) any {
	switch x := v.(type) {
	case float64:
		if x == 0 && math.Signbit(x) {
			return 0.0
		}
		return x
	case []any:
		out := make([]any, len(x))
		for i := range x {
			out[i] = normalizeNegZero(x[i])
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, vv := range x {
			out[k] = normalizeNegZero(vv)
		}
		return out
	default:
		return v
	}
}

// DecodePacket parses a Socket.IO packet from its wire form.
func DecodePacket(s string) (Packet, error) {
	if s == "" {
		// An empty Socket.IO frame decodes to a CONNECT to the default
		// namespace, mirroring socket.io-parser: its decodeString does
		// Number(str.charAt(0)), and Number("") === 0 === PacketType.CONNECT,
		// with no namespace, id, or payload. Rejecting "" here diverged from the
		// reference decoder.
		return Packet{Type: Connect, Namespace: "/"}, nil
	}
	var p Packet
	if s[0] < '0' || s[0] > '6' {
		return Packet{}, ErrInvalidPacket
	}
	p.Type = PacketType(s[0] - '0')
	i := 1

	// Binary attachment count ("<n>-") for binary packet types. Upstream
	// socket.io-parser rejects a BINARY_EVENT/BINARY_ACK header that lacks a
	// valid "<n>-" attachments prefix (its "Illegal attachments" error), so a
	// bare "5" or a "5-" with no leading digits is malformed.
	if p.Type == BinaryEvent || p.Type == BinaryAck {
		j := i
		for j < len(s) && isDigit(s[j]) {
			j++
		}
		if j > i && j < len(s) && s[j] == '-' {
			n, err := strconv.Atoi(s[i:j])
			if err != nil || n <= 0 {
				// A BINARY_EVENT/BINARY_ACK must declare at least one attachment;
				// upstream's reconstructor rejects a "0-" header with "Illegal
				// attachments" (a binary packet with no buffers is meaningless and
				// was previously decoded as a complete, non-pending packet).
				return Packet{}, ErrInvalidPacket
			}
			if n > MaxAttachments {
				// Refuse an absurd count before anything sizes a buffer from it:
				// a declared count of 2^63-1 would otherwise reach make(...) and
				// panic (or exhaust memory) in the reassembling decoder.
				return Packet{}, ErrTooManyAttachments
			}
			p.attachments = n
			i = j + 1
		} else {
			return Packet{}, ErrInvalidPacket
		}
	}

	// Optional namespace, terminated by a comma.
	if i < len(s) && s[i] == '/' {
		if comma := strings.IndexByte(s[i:], ','); comma >= 0 {
			p.Namespace = s[i : i+comma]
			i += comma + 1
		} else {
			p.Namespace = s[i:]
			i = len(s)
		}
	} else {
		p.Namespace = "/"
	}

	// Optional ack id (a run of digits).
	j := i
	for j < len(s) && isDigit(s[j]) {
		j++
	}
	if j > i {
		id, err := strconv.ParseUint(s[i:j], 10, 64)
		if err != nil {
			return Packet{}, ErrInvalidPacket
		}
		p.ID = &id
		i = j
	}

	// Remaining bytes are the JSON payload. When a payload is present it must be
	// well-formed JSON and appropriate for the packet type; upstream
	// socket.io-parser rejects a mistyped payload with its "invalid payload"
	// error (e.g. a CONNECT carrying a string, a DISCONNECT carrying anything,
	// or an EVENT carrying a non-array).
	if i < len(s) {
		var data any
		if err := json.Unmarshal([]byte(s[i:]), &data); err != nil {
			return Packet{}, ErrInvalidPacket
		}
		if !isPayloadValid(p.Type, data) {
			return Packet{}, ErrInvalidPacket
		}
		p.Data = data
	}
	return p, nil
}

// isPayloadValid reports whether a decoded JSON payload is well-typed for the
// given packet type, mirroring socket.io-parser's Decoder.isPayloadValid. It is
// only consulted when a payload is actually present on the wire; the absence of
// a payload is always acceptable (except that EVENT/ACK carry their data there).
//
// The JavaScript rules, translated to Go's json.Unmarshal value types
// (object → map[string]any, array → []any, JSON null → nil). socket.io-parser's
// isPayloadValid uses isObject(payload) — `Object.prototype.toString.call(...)
// === "[object Object]"` — which is true only for a plain object, and false for
// an array or null:
//   - CONNECT:       isObject(payload)             → plain object only
//   - DISCONNECT:    payload === undefined         → never valid when present
//   - CONNECT_ERROR: isString || isObject          → string or plain object only
//   - EVENT:         non-empty array
//   - ACK:           array (any length)
func isPayloadValid(t PacketType, data any) bool {
	switch t {
	case Connect:
		// A plain object only: arrays and null are rejected (isObject()).
		_, ok := data.(map[string]any)
		return ok
	case Disconnect:
		// A DISCONNECT must not carry a payload; reaching here means one is
		// present, so it is always invalid.
		return false
	case ConnectError:
		switch data.(type) {
		case string, map[string]any:
			return true
		}
		return false
	case Event, BinaryEvent:
		arr, ok := data.([]any)
		return ok && len(arr) > 0
	case Ack, BinaryAck:
		_, ok := data.([]any)
		return ok
	}
	return true
}

// EventName returns the event name for an Event/BinaryEvent packet.
func (p Packet) EventName() string {
	if arr, ok := p.Data.([]any); ok && len(arr) > 0 {
		if name, ok := arr[0].(string); ok {
			return name
		}
	}
	return ""
}

// Args returns the event arguments (everything after the event name) for an
// Event packet, or the full array for an Ack packet.
func (p Packet) Args() []any {
	arr, ok := p.Data.([]any)
	if !ok {
		return nil
	}
	if p.Type == Event || p.Type == BinaryEvent {
		if len(arr) > 0 {
			return arr[1:]
		}
		return nil
	}
	return arr
}

// newEvent builds an EVENT packet for a namespace.
func newEvent(nsp, name string, args []any, id *uint64) Packet {
	data := make([]any, 0, len(args)+1)
	data = append(data, name)
	data = append(data, args...)
	return Packet{Type: Event, Namespace: nsp, ID: id, Data: data}
}

func isDigit(b byte) bool { return b >= '0' && b <= '9' }
