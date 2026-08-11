# Security Policy

## Supported versions

The latest released minor version receives security fixes. Older versions are
best-effort.

## Reporting a vulnerability

**Please do not open a public issue for security problems.**

Use GitHub's private vulnerability reporting instead:

1. Go to the **Security** tab of this repository.
2. Click **Report a vulnerability** (Private vulnerability reporting).

If that is unavailable, email the maintainer at the address on their GitHub
profile. Please include:

- a description of the issue and its impact,
- steps to reproduce (a minimal proof-of-concept if possible),
- affected version(s).

We aim to acknowledge reports within a few days and to ship a fix or mitigation
as promptly as the severity warrants. Once a fix is available we will publish a
GitHub Security Advisory crediting the reporter (unless anonymity is requested).

## Hardening notes for operators

These are the knobs and defaults that decide how the server behaves under a
hostile peer. None of them replace a review of your own deployment.

### Cross-site WebSocket hijacking

`Options.CheckOrigin` is **nil by default, which accepts every origin.** A
WebSocket handshake is an ordinary `GET`: the same-origin policy does not
restrict it and no CORS preflight stands in its way, so any page a user visits
can open a socket to your server with whatever cookies the browser attaches.
If a connection carries ambient authority, set an allowlist:

```go
socketio.New(socketio.Options{
	CheckOrigin: socketio.AllowedOrigins("https://app.example.com"),
})
```

Requests with **no** `Origin` header are allowed: only browsers send one, and a
non-browser client could set any value it liked.

### CORS credentials

The polling transport reflects the request `Origin` in
`Access-Control-Allow-Origin`, but `Access-Control-Allow-Credentials: true` is
sent **only when `CheckOrigin` is configured**. Reflecting an arbitrary origin
while allowing credentials would let any site read polling responses with the
visitor's cookies.

### Client-controlled headers

`Socket.Handshake().Address` is the peer address of the socket the request
arrived on. `X-Forwarded-For` is honored only under `Options.TrustProxy` (or
`NewProxiedHandshake`), because any client can set that header and thereby
choose the address your rate limiting, authorization and audit logs see.

### Resource bounds

| Bound | Default | Configure |
| ----- | ------- | --------- |
| Concurrent Engine.IO sessions | `DefaultMaxSessions` (65536) | `Options.MaxSessions` (negative disables) |
| POSTed polling body / buffered binary attachments | 1,000,000 bytes | `Options.MaxPayload` |
| WebSocket frame and reassembled message | 1 MiB | internal (`ws.DefaultMaxMessageSize`) |
| `BINARY_EVENT` / `BINARY_ACK` attachment count | 1024 | `MaxAttachments` |
| Queued outbound packets per session | 2048, then the session is dropped | internal |
| Redis reply bulk / array declaration | 64 MiB / 1M elements | `redis.MaxBulkLength`, `redis.MaxArrayLength` |

Each of these guards a length or count that arrives on the wire *before* the
data it describes, and so would otherwise size an allocation from attacker
input.

### Availability

One client that stops reading must not stall delivery to others. Websocket
writes are drained by a per-session goroutine behind a bounded queue; when the
queue fills, that one session is dropped rather than the broadcast being
blocked. Similarly, a Redis pub/sub failure degrades a cluster broadcast to
local-only delivery instead of dropping it.

## Automated scanning

This repository is continuously scanned by:

- **CodeQL** code scanning (static analysis),
- **govulncheck** for known vulnerabilities in Go dependencies,
- **Trivy** filesystem scanning (vulnerabilities, secrets, misconfiguration),
- **Gitleaks** secret scanning, and
- GitHub secret scanning with push protection.
