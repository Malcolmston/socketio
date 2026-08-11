# API deviations from `socket.io` (Node)

This port aims to be wire- and behaviour-compatible with `socket.io@4.8.1` /
`socket.io-parser@4.2.7` / `engine.io-parser@5.2.3`. The cross-language parity
harness (`parity/socket.io/` in the aggregator) compares both implementations
over an identical corpus; it currently scores **160/165 cases matching**. The
five remaining cases are the deliberate deviations documented below — each is
either a Go-language limitation or a case where the port is intentionally
stricter than upstream (upstream is the outlier). They are marked
`"deviation": "…"` in the case files and counted separately from bugs.

| case | group | upstream | this port | why the port differs |
| --- | --- | --- | --- | --- |
| `enc-event-key-order` | wire-encode | preserves object insertion order (`{"b":1,"a":2,"C":3}`) | sorts object keys (`{"C":3,"a":2,"b":1}`) | The payload is decoded into a Go `map[string]any` before encoding, and Go maps do not preserve insertion order; `encoding/json` emits keys sorted. The JSON is semantically identical, but the bytes differ — relevant only to a signature/digest taken over the raw frame. Not fixable without threading raw bytes through the whole codec. |
| `enc-connect-error-nsp` | wire-encode | `{"message":…,"data":…}` | `{"data":…,"message":…}` | Same Go-map key-order limitation as above. |
| `enc-binary-in-connect` | wire-encode | serialises a `Buffer` inside a CONNECT payload as `{"type":"Buffer","data":[…]}` | serialises `[]byte` as base64 (`"AP8="`) | Only EVENT/ACK carry binary attachments in Socket.IO; a buffer in a CONNECT/DISCONNECT/CONNECT_ERROR payload is *not* an attachment and is serialised inline. The two inline shapes differ (Node's `JSON.stringify(Buffer)` vs Go's base64) — neither is a wire attachment. Reachable only by hand-building a `Packet`, never through `Socket.Emit`. |
| `enc-type-out-of-range` | wire-encode | encodes a packet type outside `0–6` without complaint (`7["x"]`) | rejects it with `ErrInvalidPacket` | Emitting an unparseable frame for an unknown type is a bug in the reference encoder; the port refuses to produce it. Deliberately stricter. |
| `eio-dec-binary-bad-base64` | eio-frame | `Buffer.from(s,"base64")` is lenient and yields an empty buffer for invalid input (`"b!!!"`) | `base64.StdEncoding.DecodeString` returns an error | The port refuses to silently accept malformed base64 rather than fabricating an empty payload. Deliberately stricter. |

Everything else the harness checks — packet encode/decode, namespace/room logic,
ack semantics, binary attachment framing, disconnect reasons, reserved-event
guards, middleware admit/deny — matches upstream exactly.

## `engineio` vs `engine.io-parser@5.2.3`

The `engineio` package is a port of a *different* npm package and has its own
harness, `parity/socket.io/nested/engineio/` (101 cases, **96/96 compared cases
matching**). Its five deviations, all of them cases where upstream is the
outlier:

| case | upstream | this port | why the port differs |
| --- | --- | --- | --- |
| `dec-binary-bad-base64` | `Buffer.from("###","base64")` silently drops characters outside the alphabet and yields an empty payload | `base64.StdEncoding` returns an error | The port will not fabricate a payload out of malformed base64 (the same divergence the parent harness records as `eio-dec-binary-bad-base64`). Deliberately stricter. |
| `dec-binary-bad-base64-length` | `"b4"` — one base64 character — yields an empty payload | error | Same family. |
| `dec-binary-base64url` | Node's decoder also accepts the base64url alphabet (`-`, `_`) | error | The port insists on the standard alphabet its encoder emits. |
| `dec-binary-base64-whitespace` | Node's decoder skips embedded whitespace | error | Same family. |
| `encpay-empty-batch` | `encodePayload([], cb)` counts completed packets against the batch length, so for an empty batch **the callback is never invoked** — upstream hangs | returns `""` | A latent upstream hang; the port has no reason to reproduce it. |

## `client` vs `socket.io-client@4.8.1`

The `client` package likewise has its own harness,
`parity/socket.io/nested/client/` (101 cases, **95/95 compared cases matching**),
scored against `socket.io-client` — its reconnection schedule (upstream's bundled
`backo2`), its addressing (`url()` + `Transport.uri()`), its `Manager` option
defaults and the packets an emit builds. Six deviations:

| case | upstream | this port | why the port differs |
| --- | --- | --- | --- |
| `opt-defaults`, `opt-reconnection-explicit` | `reconnection` defaults to **true** (`opts.reconnection !== false`) | `Options.Reconnection` is false unless set | A Go struct's zero value cannot express a true default, and `Options` is part of a published API, so the field cannot become a `*bool` without breaking callers. Set `Reconnection: true` for upstream behaviour. |
| `opt-defaults`, `opt-reconnection-explicit` | `randomizationFactor` defaults to **0.5** | `Options.RandomizationFactor` is 0 (no jitter) unless set | Same Go zero-value limitation: 0 is indistinguishable from unset. The `Backoff` arithmetic itself matches `backo2` exactly. |
| `opt-jitter-out-of-range` | `Manager` stores `randomizationFactor: 1.5` verbatim and only `backo2` later decides that a value outside `(0,1]` means "no jitter" | `WithDefaults` normalises it to 0 at the option layer | The resulting *schedule* is identical (see `bo-jitter-over-one`); only the reported option value differs. |
| `ep-uppercase-scheme` | `url()` tests the scheme with the case-sensitive regex `/^(https?|wss?):\/\//`, so `HTTP://h:3000` is treated as a relative address and resolved against `window.location` — absent under Node, giving `ws://undefined/socket.io/` | resolves it normally | URI schemes are case-insensitive (RFC 3986 §3.1). Upstream is the outlier. |
| `ep-unknown-scheme`, `ep-schemeless` | any other scheme, or no scheme, is browser-only: resolved against `window.location`, which under Node yields `ws://undefined/socket.io/` | keeps the host and defaults the scheme to `ws` | Upstream is the outlier; relative addressing has no meaning outside a browser. |
