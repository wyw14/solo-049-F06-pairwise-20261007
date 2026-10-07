package decoder

import (
	"time"

	"orbitlink/internal/recorder"
)

type GapStatus string

const (
	GapOpen     GapStatus = "open"
	GapRepaired GapStatus = "repaired"
)

// Gap describes one frame lost when an epoch rolled over with a partially
// assembled frame. The frame's sequence is reserved in the segment's frame
// sequence space, so the hole stays visible until a retransmission fills it.
type Gap struct {
	Epoch           int64     `json:"epoch"`
	AttemptID       string    `json:"attempt_id"`
	Sequence        uint64    `json:"sequence"`
	FrameSize       int       `json:"frame_size"`
	ReceivedSymbols int       `json:"received_symbols"`
	MissingSymbols  int       `json:"missing_symbols"`
	SegmentID       string    `json:"segment_id"`
	Status          GapStatus `json:"status"`
	RepairedBy      string    `json:"repaired_by,omitempty"`
	RepairedAt      time.Time `json:"repaired_at,omitempty"`
	seed            []byte
}

// EpochStat summarizes live reception for one decoder epoch. Repair traffic
// for older epochs is accounted on the gap, never here, so retransmissions
// cannot pollute the active epoch's counters.
type EpochStat struct {
	Epoch            int64  `json:"epoch"`
	AttemptID        string `json:"attempt_id"`
	SymbolsReceived  int    `json:"symbols_received"`
	FramesAssembled  int    `json:"frames_assembled"`
	DiscardedSymbols int    `json:"discarded_symbols"`
	OpenGaps         int    `json:"open_gaps"`
	RepairedGaps     int    `json:"repaired_gaps"`
}

// SessionReport is the decoder session's gap-centric view: per-epoch
// reception, and every frame-sequence hole keyed by epoch and attempt.
type SessionReport struct {
	PassID         string      `json:"pass_id"`
	SegmentID      string      `json:"segment_id"`
	CurrentAttempt string      `json:"current_attempt"`
	CurrentEpoch   int64       `json:"current_epoch"`
	LostSymbols    int         `json:"lost_symbols"`
	Epochs         []EpochStat `json:"epochs"`
	Gaps           []Gap       `json:"gaps"`
}

// RepairResult reports what a retransmission did to one gap. Frame is set
// once the retransmitted symbols complete the reserved frame.
type RepairResult struct {
	Gap       Gap             `json:"gap"`
	SegmentID string          `json:"segment_id"`
	Repaired  bool            `json:"repaired"`
	Frame     *recorder.Frame `json:"frame,omitempty"`
}
