package decoder

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"orbitlink/internal/recorder"
)

type Assembler struct {
	frameSize int
	buffer    []byte
	sequence  uint64
}

func NewAssembler(frameSize int) (*Assembler, error) {
	if frameSize < 8 || frameSize > 64*1024 {
		return nil, fmt.Errorf("frame size is outside decoder limits")
	}
	return &Assembler{frameSize: frameSize, buffer: make([]byte, 0, frameSize)}, nil
}

func (a *Assembler) Push(epoch int64, symbols []byte) []recorder.Frame {
	a.buffer = append(a.buffer, symbols...)
	var frames []recorder.Frame
	for len(a.buffer) >= a.frameSize {
		payload := append([]byte(nil), a.buffer[:a.frameSize]...)
		a.buffer = a.buffer[a.frameSize:]
		a.sequence++
		frames = append(frames, recorder.Frame{
			Sequence: a.sequence, Epoch: epoch, Payload: payload, CRC32: crc32.ChecksumIEEE(payload),
		})
	}
	return frames
}

// Reset drops the partially assembled frame, if any. The discarded symbols
// are returned so the session can seed a repair assembler, and the sequence
// the partial frame would have taken is reserved: the live stream skips it,
// leaving a visible hole until a retransmission repairs the gap.
func (a *Assembler) Reset() (discarded []byte, reserved uint64) {
	if len(a.buffer) == 0 {
		return nil, 0
	}
	discarded = append([]byte(nil), a.buffer...)
	a.buffer = a.buffer[:0]
	a.sequence++
	return discarded, a.sequence
}

// NewRepairAssembler builds an assembler dedicated to re-framing one gap of
// an old epoch. It is seeded with the symbols received before the epoch
// rolled over, and its first completed frame takes the reserved sequence so
// the repair lands exactly in the hole the loss left behind.
func NewRepairAssembler(frameSize int, firstSequence uint64, seed []byte) (*Assembler, error) {
	if firstSequence == 0 {
		return nil, fmt.Errorf("reserved frame sequence is required for repair")
	}
	if len(seed) == 0 || len(seed) >= frameSize {
		return nil, fmt.Errorf("repair seed must be a partial frame")
	}
	assembler, err := NewAssembler(frameSize)
	if err != nil {
		return nil, err
	}
	assembler.sequence = firstSequence - 1
	assembler.buffer = append(assembler.buffer, seed...)
	return assembler, nil
}

func EncodeTelemetry(counter uint64, size int) []byte {
	data := make([]byte, size)
	binary.BigEndian.PutUint64(data, counter)
	for index := 8; index < len(data); index++ {
		data[index] = byte((int(counter) + index) % 251)
	}
	return data
}
