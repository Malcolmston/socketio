# engineio — implements the Engine.IO v4 protocol codec

[![Go Reference](https://pkg.go.dev/badge/github.com/malcolmston/socketio/engineio.svg)](https://pkg.go.dev/github.com/malcolmston/socketio/engineio)
[![Parity](https://img.shields.io/badge/parity-100%25-brightgreen)](https://github.com/malcolmston/go/tree/main/parity/socket.io/nested/engineio)

Package engineio implements the Engine.IO v4 protocol codec — the transport
framing layer that Socket.IO is built on. It encodes and decodes the small set
of Engine.IO packets and the polling "payload" that batches several packets
into a single HTTP body. Engine.IO is the lower half of the Node.js Socket.IO
stack: it establishes and keeps alive a logical connection over an
interchangeable transport (HTTP long-polling first, then an upgrade to
WebSocket) and provides ordered, heartbeat-monitored delivery of opaque
messages. This package is the Go equivalent of that layer's parser/encoder.

The wire format is deliberately tiny. A string packet is a single ASCII digit
type prefix followed by the packet data — for example "4hello" is a MESSAGE
("4") carrying "hello", "2probe" is a PING with data "probe", and "0{...}" is
the OPEN handshake carrying JSON. The seven packet types (Open, Close, Ping,
Pong, Message, Upgrade, Noop) are modeled by PacketType and its constants.
Encode and Decode convert between a Packet and this string form; that string
is exactly what travels inside a WebSocket text frame.

## Install

```sh
go get github.com/malcolmston/socketio@v0.5.0
```

```go
import "github.com/malcolmston/socketio/engineio"
```

## Usage

This is the package's own `Example`, so it compiles and its output is
asserted on every `go test ./engineio/`.

```go
fmt.Println(engineio.NewMessage("hello").Encode())

	p, _ := engineio.Decode("4hello")
	fmt.Printf("%s %q\n", p.Type, p.Data)

	payload := engineio.EncodePayload([]engineio.Packet{
		engineio.NewOpen(`{"sid":"abc"}`),
		engineio.NewMessage("hi"),
	})
	fmt.Printf("%q\n", payload)
```

```
4hello
message "hello"
"0{\"sid\":\"abc\"}\x1e4hi"
```

## Exported surface

### Functions

| Function | What it does |
| --- | --- |
| `func EncodeCloseFrame(code CloseCode, reason string) []byte` | EncodeCloseFrame builds the payload of a WebSocket close frame: the two-byte big-endian status code followed by the UTF-8 reason. |
| `func EncodePayload(packets []Packet) string` | EncodePayload joins packets into a single polling payload, separated by the record separator. |

### Types

| Type | What it is |
| --- | --- |
| `CloseCode` | CloseCode is a WebSocket close status code (RFC 6455 §7.4). |
| `Packet` | Packet is a single Engine.IO packet. |
| `PacketType` | PacketType identifies an Engine.IO packet. |

<details>
<summary><code>CloseCode</code> — constructors and methods</summary>

| Signature | What it does |
| --- | --- |
| `func DecodeCloseFrame(payload []byte) (CloseCode, string, error)` | DecodeCloseFrame parses a WebSocket close-frame payload into its status code and reason. |
| `func (c CloseCode) IsValid() bool` | IsValid reports whether the code may legally be sent in a close frame. |
| `func (c CloseCode) String() string` | String returns a short human-readable description of the close code, e.g. |

</details>

<details>
<summary><code>Packet</code> — constructors and methods</summary>

| Signature | What it does |
| --- | --- |
| `func Decode(s string) (Packet, error)` | Decode parses a single packet from its string wire form. |
| `func DecodePayload(payload string) ([]Packet, error)` | DecodePayload splits a polling payload into its constituent packets. |
| `func NewBinaryMessage(data []byte) Packet` | NewBinaryMessage builds a MESSAGE packet carrying raw binary data. |
| `func NewClose() Packet` | NewClose builds a CLOSE packet, which requests that the transport be closed. |
| `func NewMessage(data string) Packet` | NewMessage builds a MESSAGE packet with string data. |
| `func NewNoop() Packet` | NewNoop builds a NOOP packet, which does nothing and is used to cleanly terminate a pending long-polling request when the transport upgrades. |
| `func NewOpen(handshakeJSON string) Packet` | NewOpen builds an OPEN packet carrying handshake JSON. |
| `func NewPing(data string) Packet` | NewPing builds a PING packet. |
| `func NewPong(data string) Packet` | NewPong builds a PONG packet answering a PING, echoing its data (an empty string for a heartbeat pong, "probe" for the upgrade handshake). |
| `func NewUpgrade() Packet` | NewUpgrade builds an UPGRADE packet, sent by the client to complete a polling-to-WebSocket transport upgrade. |
| `func (p Packet) Encode() string` | Encode renders a packet to its string wire form (used by the websocket transport for text frames). |
| `func (p Packet) IsBinary() bool` | IsBinary reports whether the packet is a binary MESSAGE (its Binary field is set), as opposed to a text packet. |

</details>

<details>
<summary><code>PacketType</code> — constructors and methods</summary>

| Signature | What it does |
| --- | --- |
| `func (t PacketType) String() string` | String returns the lowercase Engine.IO name of the packet type (e.g. |

</details>

### Constants

`Protocol`

### Variables

`ErrEmptyPacket`, `ErrInvalidCloseFrame`

Full signatures, doc comments and every runnable example are on
[pkg.go.dev](https://pkg.go.dev/github.com/malcolmston/socketio/engineio).

## Measured parity

Compared case-for-case against **`engine.io-parser@5.2.3; engine.io-parser@5.2.3 + engine.io@6.6.4`** by the harness in
[`parity/socket.io/nested/engineio`](https://github.com/malcolmston/go/tree/main/parity/socket.io/nested/engineio):

| | |
| --- | --- |
| Parity | **100%** |
| Cases | 101 |
| Matching | 96 |
| Mismatching | 0 |
| Declared deviations | 5 |

Regenerate from the aggregator repo with `go test ./parity/socket.io/nested/engineio/`.
Declared deviations are documented differences, excluded from the denominator
and listed in the harness report.

## Deviations from upstream

Deliberate differences for this package, where any exist, are recorded in the
module-wide [`API-DEVIATIONS.md`](../API-DEVIATIONS.md).

## License

MIT, as part of [`github.com/malcolmston/socketio`](..).
An independent re-implementation, not affiliated with or endorsed by the
original project.
