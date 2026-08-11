# redis — provides a Redis-backed Broadcaster for socketio, enabling multi-node scale-out

[![Go Reference](https://pkg.go.dev/badge/github.com/malcolmston/socketio/redis.svg)](https://pkg.go.dev/github.com/malcolmston/socketio/redis)

Package redis provides a Redis-backed Broadcaster for socketio, enabling
multi-node scale-out: broadcasts are relayed between server instances over
Redis pub/sub so a message emitted on one node reaches sockets connected to
any node. It is the Go counterpart of the Node.js @socket.io/redis-adapter,
and it speaks the Redis RESP protocol directly using only the standard library
— no third-party client and no cgo:

```go
bc, _ := redis.New(redis.Options{Addr: "localhost:6379", Channel: "socket.io"})
io.SetBroadcaster(bc) // io is a *socketio.Server
```

A single socketio.Server keeps its room and socket membership in memory, so a
broadcast only reaches clients connected to that one process. Once you run
more than one instance behind a load balancer — for horizontal scaling or
high availability — clients are spread across processes and an in-memory
broadcast no longer reaches everyone. Install this Broadcaster on every node
and each call to io.Emit, io.To(room).Emit, and the other broadcast forms is
instead published once to Redis and delivered to the local sockets of every
node that receives it. Reach for this package as soon as you scale past a
single server.

## Install

```sh
go get github.com/malcolmston/socketio@v0.5.0
```

```go
import "github.com/malcolmston/socketio/redis"
```

## Usage

This is the package's own `ExampleNew`, so it compiles and its output is
asserted on every `go test ./redis/`.

```go
bc, err := redis.New(redis.Options{
		Addr:    "localhost:6379",
		Channel: "socket.io",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer bc.Close()

	io := socketio.New()
	io.SetBroadcaster(bc)

	io.To("room1").Emit("news", "hello cluster")
```

## Exported surface

### Types

| Type | What it is |
| --- | --- |
| `Broadcaster` | Broadcaster relays socketio broadcasts over Redis pub/sub. |
| `Options` | Options configures the Redis broadcaster. |

<details>
<summary><code>Broadcaster</code> — constructors and methods</summary>

| Signature | What it does |
| --- | --- |
| `func New(opts Options) (*Broadcaster, error)` | New connects to Redis, subscribes to the broadcast channel, and returns a Broadcaster ready to install with server.SetBroadcaster. |
| `func (b *Broadcaster) Close() error` | Close implements socketio.Broadcaster. |
| `func (b *Broadcaster) OnMessage(fn func([]byte))` | OnMessage implements socketio.Broadcaster. |
| `func (b *Broadcaster) Publish(data []byte) error` | Publish implements socketio.Broadcaster: it PUBLISHes data to the channel. |

</details>

### Constants

`MaxArrayLength`, `MaxBulkLength`

### Variables

`ErrClosed`, `ErrReplyTooLarge`

Full signatures, doc comments and every runnable example are on
[pkg.go.dev](https://pkg.go.dev/github.com/malcolmston/socketio/redis).

## Deviations from upstream

Deliberate differences for this package, where any exist, are recorded in the
module-wide [`API-DEVIATIONS.md`](../API-DEVIATIONS.md).

## License

MIT, as part of [`github.com/malcolmston/socketio`](..).
An independent re-implementation, not affiliated with or endorsed by the
original project.
