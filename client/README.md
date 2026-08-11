# client — Go Socket.IO client

[![Go Reference](https://pkg.go.dev/badge/github.com/malcolmston/socketio/client.svg)](https://pkg.go.dev/github.com/malcolmston/socketio/client)
[![Parity](https://img.shields.io/badge/parity-100%25-brightgreen)](https://github.com/malcolmston/go/tree/main/parity/socket.io/nested/client)

Package client is a Go Socket.IO client. It connects to a Socket.IO server
(this module's server or the Node reference server) over the WebSocket
transport and provides the familiar On/Emit/EmitWithAck API that mirrors the
browser socket.io-client, letting a Go process act as a first-class Socket.IO
peer rather than merely a server:

```go
c, err := client.Dial("http://localhost:3000")
c.On("news", func(args []any) []any { fmt.Println(args); return nil })
c.Emit("hello", "world")
```

Use it whenever a Go program needs to talk to a Socket.IO endpoint as a
client: integration and end-to-end tests against a running server, service-to-
service messaging where the other side already speaks Socket.IO, bots and load
generators, or bridging a Socket.IO event stream into another system. Dial
blocks until the Socket.IO CONNECT handshake for the chosen namespace
completes (or DialTimeout elapses), so a successful return means the client is
fully connected and ready to Emit. Optional authentication data is sent with
the CONNECT packet via Options.Auth, matching the server-side socket.Auth().

## Install

```sh
go get github.com/malcolmston/socketio@v0.5.0
```

```go
import "github.com/malcolmston/socketio/client"
```

## Usage

This is the package's own `ExampleDial`, so it compiles and its output is
asserted on every `go test ./client/`.

```go
c, err := client.Dial("http://localhost:3000", client.Options{
		Namespace:    "/",
		Auth:         map[string]any{"token": "s3cret"},
		Reconnection: true,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	c.On("news", func(args []any) []any {
		fmt.Println("news:", args)
		return nil
	})

	if err := c.Emit("hello", "world"); err != nil {
		log.Fatal(err)
	}

	reply, err := c.EmitWithAck("question", 5*time.Second, "what is 2+2?")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("answer:", reply)
```

## Exported surface

### Functions

| Function | What it does |
| --- | --- |
| `func ConnectPacket(namespace string, auth any) socketio.Packet` | ConnectPacket builds the Socket.IO CONNECT packet that opens a namespace, carrying auth as its payload when non-nil. |
| `func EventPacket(namespace, event string, ackID *uint64, args []any) (socketio.Packet, error)` | EventPacket builds the EVENT packet an Emit sends: the event name followed by its arguments, addressed to namespace. |

### Types

| Type | What it is |
| --- | --- |
| `AckIDs` | AckIDs allocates the acknowledgement ids an emit claims: 0 for the first acknowledged emit on a connection, then 1, 2, … . |
| `Backoff` | Backoff computes an exponentially increasing delay with optional jitter, mirroring the JavaScript client's backo2 module used for reconnection. |
| `BackoffOptions` | BackoffOptions configures a Backoff. |
| `Client` | Client is a connected Socket.IO client. |
| `Endpoint` | Endpoint is where a Dial will actually connect: the Engine.IO WebSocket URL and the Socket.IO namespace to hand shake for. |
| `Handler` | Handler handles an inbound event. |
| `Options` | Options configures Dial. |

<details>
<summary><code>AckIDs</code> — constructors and methods</summary>

| Signature | What it does |
| --- | --- |
| `func (a *AckIDs) Next() uint64` | Next returns the id for the next acknowledged emit and advances the counter. |

</details>

<details>
<summary><code>Backoff</code> — constructors and methods</summary>

| Signature | What it does |
| --- | --- |
| `func NewBackoff(opts BackoffOptions) *Backoff` | NewBackoff creates a Backoff from the supplied options, filling in defaults for any zero field. |
| `func (b *Backoff) Attempts() int` | Attempts returns how many times Duration has been called since the last Reset. |
| `func (b *Backoff) Duration() time.Duration` | Duration returns the delay for the current attempt and advances to the next, computing exactly what backo2 (the module socket.io-client's Manager… |
| `func (b *Backoff) Reset()` | Reset returns the backoff to its initial state, so the next Duration call yields the minimum delay again. |

</details>

<details>
<summary><code>Client</code> — constructors and methods</summary>

| Signature | What it does |
| --- | --- |
| `func Dial(rawURL string, opts ...Options) (*Client, error)` | Dial connects to a Socket.IO server at rawURL (http:// or ws://) and returns once the Socket.IO CONNECT handshake completes. |
| `func (c *Client) Close() error` | Close disconnects from the server. |
| `func (c *Client) Emit(event string, args ...any) error` | Emit sends an event to the server. |
| `func (c *Client) EmitWithAck(event string, timeout time.Duration, args ...any) ([]any, error)` | EmitWithAck sends an event and waits for the server's acknowledgement, up to timeout. |
| `func (c *Client) ID() string` | ID returns the socket id assigned by the server. |
| `func (c *Client) On(event string, h Handler)` | On registers a handler for an event. |

</details>

<details>
<summary><code>Endpoint</code> — constructors and methods</summary>

| Signature | What it does |
| --- | --- |
| `func ResolveEndpoint(rawURL string, opts Options) (Endpoint, error)` | ResolveEndpoint derives the Engine.IO WebSocket URL and the Socket.IO namespace from a user-supplied URL, following the same rules as… |

</details>

<details>
<summary><code>Options</code> — constructors and methods</summary>

| Signature | What it does |
| --- | --- |
| `func (o Options) WithDefaults() Options` | WithDefaults returns a copy of o with every unset field filled in with the default documented on it. |

</details>

### Constants

`DefaultPath`

Full signatures, doc comments and every runnable example are on
[pkg.go.dev](https://pkg.go.dev/github.com/malcolmston/socketio/client).

## Measured parity

Compared case-for-case against **`socket.io-client@4.8.1; socket.io-client@4.8.1 + socket.io-parser@4.2.7`** by the harness in
[`parity/socket.io/nested/client`](https://github.com/malcolmston/go/tree/main/parity/socket.io/nested/client):

| | |
| --- | --- |
| Parity | **100%** |
| Cases | 101 |
| Matching | 95 |
| Mismatching | 0 |
| Declared deviations | 6 |

Regenerate from the aggregator repo with `go test ./parity/socket.io/nested/client/`.
Declared deviations are documented differences, excluded from the denominator
and listed in the harness report.

## Deviations from upstream

Deliberate differences for this package, where any exist, are recorded in the
module-wide [`API-DEVIATIONS.md`](../API-DEVIATIONS.md).

## License

MIT, as part of [`github.com/malcolmston/socketio`](..).
An independent re-implementation, not affiliated with or endorsed by the
original project.
