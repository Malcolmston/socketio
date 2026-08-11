package socketio

// This file adds a stateful streaming codec that mirrors the JavaScript
// socket.io-parser's Encoder/Decoder pair. Where Packet.Encode and DecodePacket
// operate on a single self-contained text packet, an Encoder turns one packet
// into the ordered sequence of transport frames it occupies on the wire (a text
// header followed by any binary attachment frames), and a Decoder reassembles
// that sequence back into a packet — buffering binary attachments until every
// declared buffer has arrived. This is the piece a transport needs to carry
// BINARY_EVENT / BINARY_ACK packets, which span multiple frames.

// Encoder encodes Socket.IO packets into the sequence of transport frames they
// occupy. It is the Go equivalent of socket.io-parser's Encoder: a plain,
// stateless value that is safe to share across goroutines.
type Encoder struct{}

// NewEncoder returns a ready-to-use Encoder.
func NewEncoder() *Encoder { return &Encoder{} }

// Encode renders a packet to the ordered frames it occupies on the wire. The
// first element is always the packet's text form (a Go string); when the packet
// carries []byte values the type is promoted to its binary variant, the text
// header declares the attachment count, and each following element is a raw
// binary buffer ([]byte). A caller writes element 0 as a text frame and every
// subsequent element as a binary frame, in order.
func (e *Encoder) Encode(p Packet) ([]any, error) {
	text, buffers, err := p.EncodeBinary()
	if err != nil {
		return nil, err
	}
	frames := make([]any, 0, len(buffers)+1)
	frames = append(frames, text)
	for _, b := range buffers {
		frames = append(frames, b)
	}
	return frames, nil
}

// Decoder reassembles a packet from the stream of transport frames produced by
// an Encoder. It is the Go equivalent of socket.io-parser's Decoder and is
// stateful: feed it frames with Add in arrival order and it returns a completed
// packet once all of a packet's frames have been seen. A single Decoder handles
// one packet at a time and must not be used concurrently.
type Decoder struct {
	pending *Packet
	buffers [][]byte
	need    int
}

// NewDecoder returns an empty Decoder ready to accept frames.
func NewDecoder() *Decoder { return &Decoder{} }

// Add feeds one transport frame to the decoder. The frame must be a string (a
// text frame, i.e. a Socket.IO text packet) or a []byte (a binary attachment
// frame). Add returns a non-nil packet when the frame completes a packet, or
// (nil, nil) when more frames are required (a binary header awaiting its
// attachments). A malformed frame, or a binary attachment arriving with no
// header pending, returns ErrInvalidPacket.
//
// Reassembled binary packets are rewritten to their plain EVENT/ACK type (with
// the attachment count cleared) once every {"_placeholder":true,"num":N} marker
// has been replaced by its buffer — mirroring socket.io-parser, whose Decoder
// hands consumers an ordinary EVENT/ACK. A placeholder index that points past
// the attachments supplied is rejected with an error wrapping ErrInvalidPacket.
func (d *Decoder) Add(frame any) (*Packet, error) {
	switch f := frame.(type) {
	case string:
		if d.pending != nil {
			return nil, ErrInvalidPacket
		}
		p, err := DecodePacket(f)
		if err != nil {
			return nil, err
		}
		if (p.Type == BinaryEvent || p.Type == BinaryAck) && p.attachments > 0 {
			pc := p
			d.pending = &pc
			d.buffers = make([][]byte, 0, p.attachments)
			d.need = p.attachments
			return nil, nil
		}
		return &p, nil
	case []byte:
		if d.pending == nil {
			return nil, ErrInvalidPacket
		}
		d.buffers = append(d.buffers, f)
		if len(d.buffers) < d.need {
			return nil, nil
		}
		p := *d.pending
		data, err := reconstruct(p.Data, d.buffers)
		if err != nil {
			d.Reset()
			return nil, err
		}
		p.Data = data
		// A reassembled binary packet becomes a plain EVENT/ACK, exactly as
		// socket.io-parser's Decoder does: it rewrites BINARY_EVENT→EVENT and
		// BINARY_ACK→ACK once every attachment has arrived, and clears the
		// attachment count, so every consumer downstream sees an ordinary packet.
		switch p.Type {
		case BinaryEvent:
			p.Type = Event
		case BinaryAck:
			p.Type = Ack
		}
		p.attachments = 0
		d.Reset()
		return &p, nil
	default:
		return nil, ErrInvalidPacket
	}
}

// Pending reports whether the decoder is mid-packet, waiting for binary
// attachment frames to complete the packet whose header it already received.
func (d *Decoder) Pending() bool { return d.pending != nil }

// Reset discards any partially-decoded packet, returning the decoder to its
// empty state.
func (d *Decoder) Reset() {
	d.pending = nil
	d.buffers = nil
	d.need = 0
}

// HasBinaryData reports whether the packet's payload contains any []byte
// values, i.e. whether encoding it will produce binary attachment frames.
func (p Packet) HasBinaryData() bool { return hasBinary(p.Data) }
