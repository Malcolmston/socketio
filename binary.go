package socketio

import "fmt"

// Binary attachment support implements the Socket.IO binary protocol: an event
// whose payload contains []byte values is sent as a BINARY_EVENT (or
// BINARY_ACK) whose JSON carries {"_placeholder":true,"num":N} markers, with the
// raw buffers following as separate Engine.IO binary frames.

// deconstruct walks a payload, replacing every []byte with a placeholder and
// collecting the buffers in order. It returns the rewritten payload and the
// buffers. The input is not mutated destructively beyond slices/maps it owns.
func deconstruct(data any) (any, [][]byte) {
	var buffers [][]byte
	var walk func(v any) any
	walk = func(v any) any {
		switch x := v.(type) {
		case []byte:
			idx := len(buffers)
			buffers = append(buffers, x)
			return map[string]any{"_placeholder": true, "num": idx}
		case []any:
			out := make([]any, len(x))
			for i := range x {
				out[i] = walk(x[i])
			}
			return out
		case map[string]any:
			out := make(map[string]any, len(x))
			for k, vv := range x {
				out[k] = walk(vv)
			}
			return out
		default:
			return v
		}
	}
	return walk(data), buffers
}

// Reconstruct walks a payload replacing {"_placeholder":true,"num":N} markers
// with the matching binary buffer. It is exported for client implementations
// that reassemble incoming binary packets.
//
// Every placeholder index is validated against the buffer list: a marker whose
// num is negative or points past the last buffer is rejected with an error
// wrapping ErrInvalidPacket, mirroring socket.io-parser's binary reconstructor,
// which throws "illegal attachments" rather than handing the raw placeholder
// object to the application. The index and the buffer count are both
// attacker-controlled (they arrive on the wire), so a mismatch is a malformed
// frame, not data.
func Reconstruct(data any, buffers [][]byte) (any, error) { return reconstruct(data, buffers) }

// reconstruct walks a payload replacing placeholder markers with the matching
// buffer, returning an error if any placeholder index is out of range.
func reconstruct(data any, buffers [][]byte) (any, error) {
	var walk func(v any) (any, error)
	walk = func(v any) (any, error) {
		switch x := v.(type) {
		case map[string]any:
			if ph, ok := x["_placeholder"].(bool); ok && ph {
				num, ok := asInt(x["num"])
				if !ok || num < 0 || num >= len(buffers) {
					return nil, fmt.Errorf("%w: illegal binary attachment index", ErrInvalidPacket)
				}
				return buffers[num], nil
			}
			out := make(map[string]any, len(x))
			for k, vv := range x {
				w, err := walk(vv)
				if err != nil {
					return nil, err
				}
				out[k] = w
			}
			return out, nil
		case []any:
			out := make([]any, len(x))
			for i := range x {
				w, err := walk(x[i])
				if err != nil {
					return nil, err
				}
				out[i] = w
			}
			return out, nil
		default:
			return v, nil
		}
	}
	return walk(data)
}

func asInt(v any) (int, bool) {
	switch n := v.(type) {
	case int:
		return n, true
	case int64:
		return int(n), true
	case float64:
		return int(n), true
	default:
		return 0, false
	}
}

// EncodeBinary encodes a packet, extracting any binary attachments. When the
// payload contains []byte values the packet type is promoted to its binary
// variant and the buffers are returned separately; otherwise buffers is nil.
// It is exported for client implementations.
func (p Packet) EncodeBinary() (text string, buffers [][]byte, err error) {
	// Only EVENT/ACK (and their already-promoted binary variants) carry binary
	// attachments in Socket.IO. socket.io-parser's hasBinary/deconstruct run only
	// for those types; a []byte inside a CONNECT/DISCONNECT/CONNECT_ERROR payload
	// is serialised inline, never lifted into an attachment frame. Deconstructing
	// unconditionally (as this used to) produced an unparseable packet: a text
	// header of type 0 with placeholder markers but no attachment-count prefix.
	switch p.Type {
	case Event, Ack, BinaryEvent, BinaryAck:
	default:
		s, e := p.Encode()
		return s, nil, e
	}

	newData, bufs := deconstruct(p.Data)
	if len(bufs) == 0 {
		s, e := p.Encode()
		return s, nil, e
	}
	bp := p
	bp.Data = newData
	bp.attachments = len(bufs)
	switch p.Type {
	case Event:
		bp.Type = BinaryEvent
	case Ack:
		bp.Type = BinaryAck
	}
	s, e := bp.Encode()
	return s, bufs, e
}

// hasBinary reports whether a payload contains any []byte values.
func hasBinary(data any) bool {
	switch x := data.(type) {
	case []byte:
		return true
	case []any:
		for _, v := range x {
			if hasBinary(v) {
				return true
			}
		}
	case map[string]any:
		for _, v := range x {
			if hasBinary(v) {
				return true
			}
		}
	}
	return false
}
