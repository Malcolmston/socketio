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
