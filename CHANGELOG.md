# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.5.0] - 2026-08-11
### Security
- **Silent namespace middleware no longer admits the socket (GHSA-fv7v-hmg8-9xmx,
  critical).** A middleware that returned without calling `next()` — the natural
  way to express "reject" — had its socket connected anyway, bypassing
  authentication entirely. `runMiddleware` now reports that case and the
  handshake is left unanswered.
- **Out-of-range binary placeholders are rejected (GHSA-3r64-4j95-pf42, medium).**
  Binary reassembly handed an out-of-range or zero-attachment placeholder to the
  application as data, causing type confusion. `Reconstruct` now returns an error.

### Changed
- Measured parity against `socket.io@4.8.1` raised from 81.8% to 97.0%
  (160/165), plus nested `engineio` and `client` harnesses at 100% of compared
  cases. Ordered event delivery, acks only when handled, reserved event names
  refused, `OnDisconnecting`, and client handling of a server `DISCONNECT`.

### Breaking
- `Reconstruct` returns `(any, error)`.
- Client addressing matches `socket.io-client`: the URL path is the NAMESPACE, so
  `Dial("http://h/admin")` now connects to namespace `/admin`. Previously it
  requested HTTP path `/admin/` and landed on namespace `/` — the opposite of the
  equivalent `io()` call. The HTTP mount path moved to `Options.Path`.
### Security
- **Namespace middleware that never calls `next()` is now a deny, not an admit**
  (GHSA-fv7v-hmg8-9xmx). `Namespace.runMiddleware` used to return a `nil` error —
  read by the caller as "accepted" — when a middleware returned without calling
  `next`, so a silent auth reject (`return` without `next(err)`) admitted the
  socket. It now leaves the handshake unanswered, matching upstream: the client
  times out and no `connection` event fires. The safe default for a middleware
  chain is deny.
- **Out-of-range binary placeholders are rejected instead of handed to the app**
  (GHSA-3r64-4j95-pf42). Binary reassembly (`Reconstruct`, `Decoder`, and the
  server/client read paths) now rejects a `{"_placeholder":true,"num":N}` whose
  index is negative or past the supplied buffers, and a `BINARY_EVENT`/
  `BINARY_ACK` header declaring `0` attachments, both with an error wrapping
  `ErrInvalidPacket` — matching socket.io-parser's "illegal attachments". A
  remote peer can no longer make a handler receive a raw placeholder object where
  it expected `[]byte`. `Reconstruct` now returns `(any, error)` (breaking).

### Changed / Fixed (cross-language parity with socket.io@4.8.1)
- **Client addressing now follows `socket.io-client` (breaking).** The PATH of
  the dialled URL is the NAMESPACE, as in `io("http://host/admin")`, and the HTTP
  path the server is mounted on is the new `client.Options.Path` (default
  `client.DefaultPath`, `"/socket.io/"`). `client.Dial("http://host/admin")` used
  to request the HTTP path `/admin/` and connect to namespace `/` instead. A port
  that is the scheme's default (80 for `ws`, 443 for `wss`) is now dropped, and
  userinfo in the URL never reaches the transport URL. The new
  `client.ResolveEndpoint` exposes the whole derivation without dialling.
- **Reconnection follows the JavaScript `Manager`'s schedule.** `Options` gained
  `ReconnectionDelayMax` (default 5s) and `RandomizationFactor`, and the
  reconnect loop now drives a `Backoff` instead of an open-coded doubling.
  `Options.DialTimeout`'s default moved from 10s to upstream's 20s
  (`timeout: 20000`). `Options.WithDefaults` exposes the normalisation `Dial`
  applies.
- **`Backoff` now computes exactly what `backo2` computes.** The jitter deviation
  is floored before it is applied, the ceiling is enforced *after* the jitter (so
  a clamped delay is jittered from its unclamped value), a delay is truncated to
  whole milliseconds, and a jitter outside `(0,1]` disables randomisation instead
  of being clamped to 1. Six parity cases were wrong before.
- **`client.ConnectPacket`, `client.EventPacket` and `client.AckIDs`** — the
  packet constructors `Dial`/`Emit`/`EmitWithAck` use, and the ack-id allocator
  (upstream's `Socket.ids`), are now exported so the frames a client emits can be
  compared against `socket.io-client` without a server.
- **Ordered event delivery.** Inbound events for a socket are dispatched by a
  single per-socket goroutine in arrival order, instead of one goroutine per
  packet; back-to-back emits are no longer reordered.
- **Acks only when handled.** The server and client now acknowledge an event
  only when a handler returns a non-nil slice, mirroring "the callback was
  invoked". An event with no handler, or a handler that returns `nil`, is no
  longer auto-acked — so an ack timeout is reachable again.
- **Reserved event names are refused.** `Socket.Emit` and `Client.Emit` return
  `ErrReservedEvent` for `connect`/`disconnect`/`connect_error`/… instead of
  sending them as application events.
- **`Socket.OnDisconnecting`** — a pre-disconnect hook that fires while the
  socket is still in its rooms, the equivalent of `socket.on("disconnecting")`.
- **Client learns about namespace disconnects.** The client now handles a
  server DISCONNECT packet: it fires `disconnect` with reason
  `"io server disconnect"`, stops delivering further events for the namespace,
  and no longer silently drops the packet.
- **Client `disconnect` carries a reason** (`"io server disconnect"` /
  `"transport close"`), matching socket.io-client.
- **Decoder rewrites reassembled binary packets** to plain `EVENT`/`ACK` and
  clears the attachment count, as socket.io-parser does.
- **Wire encoding matches JSON.stringify:** HTML metacharacters (`<`, `>`, `&`)
  are emitted raw, and negative zero is emitted as `0`.
- **Stricter decode:** CONNECT/CONNECT_ERROR payloads are validated with
  upstream's `isObject()` rule (arrays and `null` rejected); an empty frame `""`
  decodes to a CONNECT to `/`; `engineio.DecodePayload("")` errors.
- Binary attachments are only extracted from EVENT/ACK payloads; a `[]byte` in a
  CONNECT/DISCONNECT/CONNECT_ERROR payload is serialised inline.
- See `API-DEVIATIONS.md` for the five wire cases the port deliberately does not
  match (object key order, invalid-type and lenient-base64 handling).

### Added
- `AllowedOrigins(...)` — builds an `Options.CheckOrigin` allowlist, the
  equivalent of socket.io's `cors: { origin: [...] }`. Documented as the defense
  against cross-site WebSocket hijacking (a WebSocket upgrade is not subject to
  the same-origin policy).
- `Options.MaxSessions` (default `DefaultMaxSessions` = 65536) caps concurrent
  Engine.IO sessions; handshakes past the cap get `503`. Negative disables it.
- `redis.ErrReplyTooLarge`, `redis.ErrClosed`, `redis.MaxBulkLength`,
  `redis.MaxArrayLength`.
- Hardening notes in `SECURITY.md` and `README.md`; fuzz targets for the
  Socket.IO and Engine.IO decoders.

### Fixed
- A slow websocket client no longer stalls a room broadcast. Outbound frames go
  through a bounded per-session queue drained by a dedicated writer goroutine;
  a peer that stops reading has its session dropped instead of blocking the
  fan-out to everyone else.
- Binary packets buffered while a session was still polling were flushed with
  `WriteText` on upgrade, downgrading a native binary frame to the `"b"`+base64
  text form. They now keep their binary framing.
- A session torn down while the Engine.IO upgrade probe was in flight left the
  websocket (and its read goroutine) attached to a conn the server no longer
  tracked. `attachWebSocket` now reports the failure and the socket is closed.
- `Access-Control-Allow-Credentials: true` is no longer sent alongside an
  arbitrary reflected `Origin`; it requires `CheckOrigin` to be configured. A
  `Vary: Origin` response header was added.
- A cluster broadcast whose `Publish` failed (Redis down or closed) was dropped
  entirely — local sockets never saw it. It now degrades to local delivery.
- The Redis RESP reader sized allocations directly from wire-declared lengths:
  `$9223372036854775807` panicked in `make`, `*2000000000` requested 32 GB, and
  `strconv` errors were discarded so `$abc` decoded as an empty bulk string.
  Lengths are now bounded and validated, and array nesting is depth-limited.

## [0.4.0] - 2026-07-19
### Added
- **Upstream-parity tests** for the `engineio` Engine.IO v4 packet codec, verified
  against socketio/engine.io-parser vectors; `parity.json` published.
### Changed
- 100% exported-symbol API-doc coverage across the module.

## [0.3.0] - 2026-07-18
### Added
- Streaming Socket.IO parser (`Encoder`/`Decoder`) mirroring `socket.io-parser`:
  encode a packet into its ordered transport frames and reassemble multi-frame
  binary packets (`BINARY_EVENT`/`BINARY_ACK`), plus `Packet.HasBinaryData`.
- Catch-all socket listeners (`Socket.OnAny`, `PrependAny`, `OffAny`,
  `ListenersAny`) — the equivalent of `socket.onAny`.
- Broadcast-flag and room parity: `BroadcastOperator.Local`, `Timeout`,
  `ExceptRoom`, and `In`/`Except`/`Local` entry points on `Namespace`, `Server`,
  and `Socket` (`io.in`, `io.except`, `io.local`, `socket.in`, `socket.except`).
- `Handshake` object and `NewHandshake` capturing request headers, address
  (honoring `X-Forwarded-For`), query, TLS state, and auth — like
  `socket.handshake`.
- Reserved-event guards: `IsReservedEvent`, `ValidateEventName`,
  `ReservedEvents`.
- Engine.IO WebSocket close codes (`CloseCode` + RFC 6455 constants,
  `EncodeCloseFrame`, `DecodeCloseFrame`, `IsValid`) and packet constructors
  (`NewPing`, `NewPong`, `NewClose`, `NewUpgrade`, `NewNoop`,
  `NewBinaryMessage`, `Packet.IsBinary`).
- Client reconnection `Backoff` (exponential delay with optional deterministic
  jitter), mirroring the JS client's `backo2`.

## [0.1.0] - 2026-07-04
### Added
- Initial public release — a from-scratch Go port of Socket.IO.
- Engine.IO v4 and Socket.IO v5 packet codecs.
- WebSocket (RFC 6455) transport and HTTP long-polling transport, both
  implemented on the standard library.
- Server API: namespaces, rooms, sockets, and acknowledgements; binary payloads;
  per-socket data store; client reconnection.
- Redis adapter for multi-node broadcasting.
- Verified interop with the real `socket.io-client@4`.
- Shared `Emitter` / `RoomTargeter` / `BroadcastTarget` interfaces documenting
  the emitter/broadcaster contract across Server, Namespace, Socket and
  BroadcastOperator.
- Automated releases (VERSION-driven tags + GitHub Releases, moving `stable` tag).
- CI: build/test matrix (Go 1.23 & 1.24), `-race` + coverage, golangci-lint v2,
  govulncheck, CodeQL, benchmarks, dependency review, and a stale bot.

[Unreleased]: https://github.com/malcolmston/socket.io/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/malcolmston/socket.io/compare/v0.1.0...v0.3.0
[0.1.0]: https://github.com/malcolmston/socket.io/releases/tag/v0.1.0
