package socketio

import (
	"encoding/json"
	"time"
)

// BroadcastOperator emits events to a filtered set of sockets in a namespace —
// optionally scoped to one or more rooms and excluding specific sockets. It is
// returned by Namespace.To, Server.To, and Socket.To, and is the equivalent of
// io.to(room).emit(...).
type BroadcastOperator struct {
	ns          *Namespace
	rooms       []string
	except      map[string]struct{}
	exceptRooms []string
	volatile    bool
	compress    bool
	local       bool
	timeout     time.Duration
}

// Volatile marks the broadcast as volatile: messages that cannot be delivered
// immediately (e.g. to a client mid-reconnect) may be dropped. On this
// single-node, buffered implementation it is advisory.
func (b *BroadcastOperator) Volatile() *BroadcastOperator {
	b.volatile = true
	return b
}

// Compress sets whether the payload should be compressed by the transport. It
// is advisory in this implementation.
func (b *BroadcastOperator) Compress(on bool) *BroadcastOperator {
	b.compress = on
	return b
}

// In is an alias for To, matching socket.io's io.in(room).
func (b *BroadcastOperator) In(room string) *BroadcastOperator { return b.To(room) }

// To narrows the broadcast to an additional room.
func (b *BroadcastOperator) To(room string) *BroadcastOperator {
	b.rooms = append(b.rooms, room)
	return b
}

// Except excludes a socket id from the broadcast.
func (b *BroadcastOperator) Except(socketID string) *BroadcastOperator {
	if b.except == nil {
		b.except = make(map[string]struct{})
	}
	b.except[socketID] = struct{}{}
	return b
}

// Emit sends an event to every socket matched by the operator. When a cluster
// Broadcaster is installed, the broadcast is published to all nodes (which each
// deliver it to their local sockets); otherwise it is delivered locally.
func (b *BroadcastOperator) Emit(event string, args ...any) {
	skip := b.resolvedExcept()
	// A local-only broadcast never leaves this node, even when a cluster
	// Broadcaster is installed — the equivalent of socket.io's .local flag.
	if bc := b.ns.server.broadcaster; bc != nil && !b.local {
		msg := broadcastMessage{
			Namespace: b.ns.name,
			Rooms:     b.rooms,
			Except:    exceptKeys(skip),
			Event:     event,
			Args:      args,
		}
		// Publishing succeeds only when the cluster link is healthy. Redis
		// pub/sub echoes the message back to the publisher, so on success the
		// local sockets are served by the same delivery path as every other
		// node's and must not be emitted to twice.
		//
		// If Publish fails (Redis down, connection closed, payload not
		// serializable) the broadcast used to be dropped entirely — local
		// sockets never saw it. Degrade to local-only delivery instead, so an
		// unavailable Redis costs cross-node fan-out rather than the whole
		// broadcast.
		if data, err := json.Marshal(msg); err == nil {
			if err := bc.Publish(data); err == nil {
				return
			}
		}
	}
	b.emitLocalExcept(skip, event, args...)
}

// emitLocal delivers the broadcast to this node's local sockets only.
func (b *BroadcastOperator) emitLocal(event string, args ...any) {
	b.emitLocalExcept(b.resolvedExcept(), event, args...)
}

// emitLocalExcept delivers the broadcast to this node's local sockets, skipping
// any whose id is in the provided exclusion set.
func (b *BroadcastOperator) emitLocalExcept(skip map[string]struct{}, event string, args ...any) {
	for _, s := range b.targets() {
		if _, excluded := skip[s.id]; excluded {
			continue
		}
		_ = s.Emit(event, args...)
	}
}

// resolvedExcept returns the effective exclusion set: the explicitly excluded
// socket ids plus every socket that is a member of an excepted room.
func (b *BroadcastOperator) resolvedExcept() map[string]struct{} {
	skip := make(map[string]struct{}, len(b.except))
	for id := range b.except {
		skip[id] = struct{}{}
	}
	for _, room := range b.exceptRooms {
		for _, s := range b.ns.SocketsInRoom(room) {
			skip[s.id] = struct{}{}
		}
	}
	return skip
}

func exceptKeys(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// targets resolves the set of sockets the operator addresses, de-duplicated by
// socket id when several rooms overlap.
func (b *BroadcastOperator) targets() []*Socket {
	if len(b.rooms) == 0 {
		return b.ns.Sockets()
	}
	seen := make(map[string]*Socket)
	for _, room := range b.rooms {
		for _, s := range b.ns.SocketsInRoom(room) {
			seen[s.id] = s
		}
	}
	out := make([]*Socket, 0, len(seen))
	for _, s := range seen {
		out = append(out, s)
	}
	return out
}
