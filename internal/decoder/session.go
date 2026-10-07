package decoder

import (
	"fmt"
	"sync"
	"time"

	"orbitlink/internal/audit"
	"orbitlink/internal/recorder"
)

// epochState tracks one receiver attempt's symbol stream. Live symbols are
// assembled by the session assembler; retransmitted symbols are assembled by
// an isolated per-epoch repair assembler so stale data can never corrupt the
// active epoch.
type epochState struct {
	attemptID      string
	received       int
	discarded      int
	frames         uint64
	repair         *Assembler
	repairReceived int
	repaired       map[uint64]bool
	duplicates     int
}

type FrameGap struct {
	Sequence         uint64 `json:"sequence"`
	DiscardedSymbols int    `json:"discarded_symbols"`
	Repaired         bool   `json:"repaired"`
}

type EpochReport struct {
	Epoch            int64      `json:"epoch"`
	AttemptID        string     `json:"attempt_id"`
	Active           bool       `json:"active"`
	SymbolsReceived  int        `json:"symbols_received"`
	SymbolsDiscarded int        `json:"symbols_discarded"`
	PendingSymbols   int        `json:"pending_symbols"`
	FramesAssembled  uint64     `json:"frames_assembled"`
	RepairSymbols    int        `json:"repair_symbols_received"`
	RepairPending    int        `json:"repair_pending_symbols"`
	FramesRepaired   int        `json:"frames_repaired"`
	DuplicateRepairs int        `json:"duplicate_repair_frames"`
	Gaps             []FrameGap `json:"gaps"`
}

type RepairRecord struct {
	SegmentID  string    `json:"segment_id"`
	Epoch      int64     `json:"epoch"`
	AttemptID  string    `json:"attempt_id"`
	Sequences  []uint64  `json:"sequences"`
	Duplicates int       `json:"duplicates"`
	AppliedAt  time.Time `json:"applied_at"`
}

type GapReport struct {
	PassID       string         `json:"pass_id"`
	SegmentID    string         `json:"segment_id"`
	CurrentEpoch int64          `json:"current_epoch"`
	LostSymbols  int            `json:"lost_symbols"`
	Epochs       []EpochReport  `json:"epochs"`
	Repairs      []RepairRecord `json:"repairs"`
}

type RepairResult struct {
	PassID         string   `json:"pass_id"`
	SegmentID      string   `json:"segment_id"`
	Epoch          int64    `json:"epoch"`
	AttemptID      string   `json:"attempt_id"`
	Repaired       []uint64 `json:"repaired_sequences"`
	Duplicates     int      `json:"duplicate_frames"`
	PendingSymbols int      `json:"pending_symbols"`
}

type Session struct {
	mu          sync.Mutex
	passID      string
	segmentID   string
	attemptID   string
	epoch       int64
	frameSize   int
	assembler   *Assembler
	recorder    *recorder.Manager
	audit       *audit.Trail
	lostSymbols int
	epochs      map[int64]*epochState
	order       []int64
	repairs     []RepairRecord
}

func NewSession(passID, segmentID string, frameSize int, manager *recorder.Manager, trail *audit.Trail) (*Session, error) {
	assembler, err := NewAssembler(frameSize)
	if err != nil {
		return nil, err
	}
	return &Session{
		passID: passID, segmentID: segmentID, frameSize: frameSize,
		assembler: assembler, recorder: manager, audit: trail,
		epochs: make(map[int64]*epochState),
	}, nil
}

func (s *Session) BeginEpoch(attemptID string) (int64, error) {
	if attemptID == "" {
		return 0, fmt.Errorf("receiver attempt is required for decoder epoch")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if attemptID == s.attemptID {
		return s.epoch, nil
	}
	discarded := s.assembler.Reset()
	s.lostSymbols += discarded
	if current, found := s.epochs[s.epoch]; found {
		current.discarded += discarded
	}
	s.attemptID = attemptID
	s.epoch++
	s.epochs[s.epoch] = &epochState{attemptID: attemptID, repaired: make(map[uint64]bool)}
	s.order = append(s.order, s.epoch)
	_, _ = s.audit.Record(audit.Event{Component: "decoder", Action: "epoch.started", Subject: s.passID, Fields: map[string]any{"attempt_id": attemptID, "epoch": s.epoch, "discarded_symbols": discarded}})
	return s.epoch, nil
}

func (s *Session) Feed(attemptID string, symbols []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if attemptID != s.attemptID || s.epoch == 0 {
		return 0, fmt.Errorf("symbols belong to inactive receiver attempt")
	}
	frames := s.assembler.Push(s.epoch, symbols)
	for _, frame := range frames {
		if err := s.recorder.Buffer(s.segmentID, frame); err != nil {
			return 0, err
		}
	}
	state := s.epochs[s.epoch]
	state.received += len(symbols)
	state.frames += uint64(len(frames))
	return len(frames), nil
}

// FeedRepair re-assembles retransmitted symbols for a closed epoch using an
// assembler dedicated to that epoch. The live assembler, the active epoch's
// counters and the lost-symbol tally are never touched, so a stale
// retransmission cannot pollute the current session. Frames that were already
// delivered live (or by an earlier retransmission) are counted as duplicates;
// only genuinely missing frames are buffered into the recording segment and
// linked to it through a RepairRecord.
func (s *Session) FeedRepair(attemptID string, epoch int64, symbols []byte) (RepairResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, found := s.epochs[epoch]
	if !found {
		return RepairResult{}, fmt.Errorf("decoder epoch %d is unknown for retransmission", epoch)
	}
	if epoch == s.epoch {
		return RepairResult{}, fmt.Errorf("decoder epoch %d is active; route symbols through the live feed", epoch)
	}
	if state.attemptID != attemptID {
		return RepairResult{}, fmt.Errorf("retransmission attempt does not match epoch %d receiver attempt", epoch)
	}
	if state.repair == nil {
		assembler, err := NewAssembler(s.frameSize)
		if err != nil {
			return RepairResult{}, err
		}
		state.repair = assembler
	}
	state.repairReceived += len(symbols)
	frames := state.repair.Push(epoch, symbols)
	result := RepairResult{PassID: s.passID, SegmentID: s.segmentID, Epoch: epoch, AttemptID: attemptID}
	for _, frame := range frames {
		if frame.Sequence <= state.frames || state.repaired[frame.Sequence] {
			state.duplicates++
			result.Duplicates++
			continue
		}
		if err := s.recorder.Buffer(s.segmentID, frame); err != nil {
			return result, err
		}
		state.repaired[frame.Sequence] = true
		result.Repaired = append(result.Repaired, frame.Sequence)
	}
	result.PendingSymbols = state.repair.Pending()
	if len(result.Repaired) > 0 {
		record := RepairRecord{
			SegmentID: s.segmentID, Epoch: epoch, AttemptID: attemptID,
			Sequences: append([]uint64(nil), result.Repaired...), Duplicates: result.Duplicates,
			AppliedAt: time.Now().UTC(),
		}
		s.repairs = append(s.repairs, record)
		_, _ = s.audit.Record(audit.Event{Component: "decoder", Action: "repair.applied", Subject: s.passID, Fields: map[string]any{"segment_id": s.segmentID, "epoch": epoch, "attempt_id": attemptID, "frames": len(record.Sequences), "duplicates": record.Duplicates}})
	}
	return result, nil
}

// Report breaks the session down by epoch, receiver attempt and frame
// sequence so operators can see exactly where symbols were lost and which
// gaps a retransmission has already closed.
func (s *Session) Report() GapReport {
	s.mu.Lock()
	defer s.mu.Unlock()
	report := GapReport{
		PassID: s.passID, SegmentID: s.segmentID, CurrentEpoch: s.epoch,
		LostSymbols: s.lostSymbols,
		Epochs:      make([]EpochReport, 0, len(s.order)),
		Repairs:     append([]RepairRecord(nil), s.repairs...),
	}
	for _, epoch := range s.order {
		state := s.epochs[epoch]
		entry := EpochReport{
			Epoch: epoch, AttemptID: state.attemptID, Active: epoch == s.epoch,
			SymbolsReceived: state.received, SymbolsDiscarded: state.discarded,
			FramesAssembled: state.frames, RepairSymbols: state.repairReceived,
			FramesRepaired: len(state.repaired), DuplicateRepairs: state.duplicates,
		}
		if entry.Active {
			entry.PendingSymbols = s.assembler.Pending()
		}
		if state.repair != nil {
			entry.RepairPending = state.repair.Pending()
		}
		if state.discarded > 0 {
			entry.Gaps = []FrameGap{{
				Sequence:         state.frames + 1,
				DiscardedSymbols: state.discarded,
				Repaired:         state.repaired[state.frames+1],
			}}
		}
		report.Epochs = append(report.Epochs, entry)
	}
	return report
}

func (s *Session) Snapshot() (attemptID string, epoch int64, lostSymbols int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.attemptID, s.epoch, s.lostSymbols
}
