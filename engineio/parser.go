// Package engineio implements the Engine.IO v4 protocol codec — the transport
// framing layer that Socket.IO is built on. It encodes and decodes the small
// set of Engine.IO packets and the polling "payload" that batches several
// packets into a single HTTP body. Engine.IO is the lower half of the Node.js
// Socket.IO stack: it establishes and keeps alive a logical connection over an
// interchangeable transport (HTTP long-polling first, then an upgrade to
// WebSocket) and provides ordered, heartbeat-monitored delivery of opaque
// messages. This package is the Go equivalent of that layer's parser/encoder.
//
// The wire format is deliberately tiny. A string packet is a single ASCII digit
// type prefix followed by the packet data — for example "4hello" is a MESSAGE
// ("4") carrying "hello", "2probe" is a PING with data "probe", and "0{...}" is
// the OPEN handshake carrying JSON. The seven packet types (Open, Close, Ping,
// Pong, Message, Upgrade, Noop) are modeled by PacketType and its constants.
// Encode and Decode convert between a Packet and this string form; that string
// is exactly what travels inside a WebSocket text frame.
//
// Use this package when you need to speak the transport layer directly rather
// than the higher-level Socket.IO event API: the socketio server uses it to
// frame outbound messages and to interpret inbound transport packets, and the
// client package uses it to read the OPEN handshake and answer heartbeats. Most
// application code never imports engineio directly — it is a building block —
// but it is exported so alternative transports or tooling can reuse the codec.
//
// For HTTP long-polling, several packets are batched into one response body
// using EncodePayload/DecodePayload, which join packets with the Engine.IO v4
// record separator (ASCII 0x1e). Binary data is handled two ways: over
// WebSocket a binary MESSAGE rides in a native binary frame (its Binary field is
// set and Encode falls back to a base64 "b"-prefixed string only when forced
// into a text context), while in a polling payload a binary packet is always
// serialized as "b" + standard-base64. NewMessage and NewOpen are convenience
// constructors for the two packet types callers build most often.
//
// The codec is pure and stateless: functions do not retain the byte slices they
// return, hold no locks, and perform no I/O, so they are safe to call
// concurrently. Decode and DecodePayload validate their input and return
// ErrEmptyPacket or a descriptive error for malformed data rather than
// panicking. This is a focused implementation of Engine.IO v4 (Protocol == 4)
// covering exactly what Socket.IO v5 needs; it is not a general transport
// manager and does not itself open sockets, schedule pings, or negotiate
// upgrades — those responsibilities live in the socketio and client packages.
package engineio

import (
	"encoding/base64"
	"errors"
	"strings"
)

// Protocol is the Engine.IO protocol revision implemented here.
const Protocol = 4

// PacketType identifies an Engine.IO packet.
type PacketType byte

const (
	// Open is sent by the server on connection with the handshake data.
	Open PacketType = iota
	// Close requests the transport be closed.
	Close
	// Ping is part of the heartbeat (server -> client in EIO4).
	Ping
	// Pong answers a Ping.
	Pong
	// Message carries an application payload (a Socket.IO packet).
	Message
	// Upgrade signals a transport upgrade (polling -> websocket).
	Upgrade
	// Noop does nothing; used to cleanly terminate a polling request.
	Noop
)

// String returns the lowercase Engine.IO name of the packet type (e.g.
// "message", "ping"), or "unknown" for an unrecognized value.
func (t PacketType) String() string {
	switch t {
	case Open:
		return "open"
	case Close:
		return "close"
	case Ping:
		return "ping"
	case Pong:
		return "pong"
	case Message:
		return "message"
	case Upgrade:
		return "upgrade"
	case Noop:
		return "noop"
	default:
		return "unknown"
	}
}

// separator batches multiple packets in a single polling payload (EIO4 uses the
// ASCII record separator, 0x1e).
const separator = "\x1e"

// Packet is a single Engine.IO packet.
type Packet struct {
	// Type is the Engine.IO packet type (Open, Message, Ping, ...).
	Type PacketType
	// Data is the textual payload for string packets.
	Data string
	// Binary holds raw bytes for binary packets; when non-nil the packet is
	// encoded with a "b" prefix + base64 in polling payloads.
	Binary []byte
}

// ErrEmptyPacket is returned when decoding an empty packet string.
var ErrEmptyPacket = errors.New("engineio: empty packet")

// Encode renders a packet to its string wire form (used by the websocket
// transport for text frames). Binary packets return their base64 form here.
func (p Packet) Encode() string {
	if p.Binary != nil {
		return "b" + base64.StdEncoding.EncodeToString(p.Binary)
	}
	return string('0'+byte(p.Type)) + p.Data
}

// Decode parses a single packet from its string wire form.
func Decode(s string) (Packet, error) {
	if s == "" {
		return Packet{}, ErrEmptyPacket
	}
	if s[0] == 'b' {
		raw, err := base64.StdEncoding.DecodeString(s[1:])
		if err != nil {
			return Packet{}, err
		}
		return Packet{Type: Message, Binary: raw}, nil
	}
	t := PacketType(s[0] - '0')
	if t > Noop {
		return Packet{}, errors.New("engineio: invalid packet type")
	}
	return Packet{Type: t, Data: s[1:]}, nil
}

// EncodePayload joins packets into a single polling payload, separated by the
// record separator.
func EncodePayload(packets []Packet) string {
	parts := make([]string, len(packets))
	for i, p := range packets {
		parts[i] = p.Encode()
	}
	return strings.Join(parts, separator)
}

// DecodePayload splits a polling payload into its constituent packets.
func DecodePayload(payload string) ([]Packet, error) {
	if payload == "" {
		// An empty polling body is malformed: engine.io-parser's decodePayload
		// yields a single "parser error" packet for "". Returning an empty packet
		// list with no error diverged from the reference decoder (and let a
		// caller treat a truncated/empty POST body as a valid, empty batch).
		return nil, ErrEmptyPacket
	}
	parts := strings.Split(payload, separator)
	packets := make([]Packet, 0, len(parts))
	for _, part := range parts {
		p, err := Decode(part)
		if err != nil {
			return nil, err
		}
		packets = append(packets, p)
	}
	return packets, nil
}

// Convenience constructors for common packets.

// NewMessage builds a MESSAGE packet with string data.
func NewMessage(data string) Packet { return Packet{Type: Message, Data: data} }

// NewOpen builds an OPEN packet carrying handshake JSON.
func NewOpen(handshakeJSON string) Packet { return Packet{Type: Open, Data: handshakeJSON} }
