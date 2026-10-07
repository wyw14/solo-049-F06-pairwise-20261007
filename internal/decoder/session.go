package decoder

import (
	"fmt"
	"sync"
	"time"

	"orbitlink/internal/audit"
	"orbitlink/internal/recorder"
)

type Session struct {
	mu          sync.Mutex
	passID      string
	segmentID   string
	attemptID   string
	epoch       int64
	assembler   *Assembler
	repairs     map[int64]*Assembler
	epochs      []EpochStat
	gaps        []Gap
	recorder    *recorder.Manager
	audit       *audit.Trail
	lostSymbols int
}

func NewSession(passID, segmentID string, frameSize int, manager *recorder.Manager, trail *audit.Trail) (*Session, error) {
	assembler, err := NewAssembler(frameSize)
	if err != nil {
		return nil, err
	}
	return &Session{
		passID: passID, segmentID: segmentID, assembler: assembler,
		repairs: make(map[int64]*Assembler), recorder: manager, audit: trail,
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
	discarded, reserved := s.assembler.Reset()
	if s.epoch > 0 {
		s.epochs[len(s.epochs)-1].DiscardedSymbols = len(discarded)
	}
	if len(discarded) > 0 {
		gap := Gap{
			Epoch: s.epoch, AttemptID: s.attemptID, Sequence: reserved,
			FrameSize: s.assembler.frameSize, ReceivedSymbols: len(discarded),
			MissingSymbols: s.assembler.frameSize - len(discarded),
			SegmentID:      s.segmentID, Status: GapOpen, seed: discarded,
		}
		s.gaps = append(s.gaps, gap)
		_, _ = s.audit.Record(audit.Event{Component: "decoder", Action: "gap.opened", Subject: s.passID, Fields: map[string]any{
			"epoch": gap.Epoch, "attempt_id": gap.AttemptID, "sequence": gap.Sequence,
			"missing_symbols": gap.MissingSymbols, "segment_id": s.segmentID,
		}})
	}
	s.lostSymbols += len(discarded)
	s.attemptID = attemptID
	s.epoch++
	s.epochs = append(s.epochs, EpochStat{Epoch: s.epoch, AttemptID: attemptID})
	_, _ = s.audit.Record(audit.Event{Component: "decoder", Action: "epoch.started", Subject: s.passID, Fields: map[string]any{"attempt_id": attemptID, "epoch": s.epoch, "discarded_symbols": len(discarded)}})
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
	stat := &s.epochs[len(s.epochs)-1]
	stat.SymbolsReceived += len(symbols)
	stat.FramesAssembled += len(frames)
	return len(frames), nil
}

// FeedRepair re-frames a gap left by an old epoch from retransmitted symbols.
// The symbols are assembled in a repair assembler seeded with the epoch's
// partial frame, so the live assembler and the active epoch's counters are
// never touched: a retransmission cannot pollute the current session.
func (s *Session) FeedRepair(attemptID string, epoch int64, symbols []byte) (RepairResult, error) {
	if attemptID == "" {
		return RepairResult{}, fmt.Errorf("receiver attempt is required for retransmission")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if epoch < 1 || epoch > s.epoch {
		return RepairResult{}, fmt.Errorf("decoder epoch %d is unknown", epoch)
	}
	if epoch == s.epoch {
		return RepairResult{}, fmt.Errorf("epoch %d is active; retransmissions only target closed epochs", epoch)
	}
	index := -1
	for i := range s.gaps {
		if s.gaps[i].Epoch == epoch {
			index = i
			break
		}
	}
	if index < 0 {
		return RepairResult{}, fmt.Errorf("decoder epoch %d has no recorded gap", epoch)
	}
	gap := &s.gaps[index]
	if gap.Status == GapRepaired {
		return RepairResult{}, fmt.Errorf("gap at epoch %d sequence %d is already repaired", epoch, gap.Sequence)
	}
	if len(symbols) == 0 {
		return RepairResult{}, fmt.Errorf("retransmission symbols are required")
	}
	if len(symbols) > gap.MissingSymbols {
		return RepairResult{}, fmt.Errorf("retransmission of %d symbols exceeds the %d still missing for epoch %d", len(symbols), gap.MissingSymbols, epoch)
	}
	assembler := s.repairs[epoch]
	if assembler == nil {
		seeded, err := NewRepairAssembler(s.assembler.frameSize, gap.Sequence, gap.seed)
		if err != nil {
			return RepairResult{}, err
		}
		assembler = seeded
		s.repairs[epoch] = assembler
	}
	frames := assembler.Push(gap.Epoch, symbols)
	gap.MissingSymbols -= len(symbols)
	if len(frames) == 0 {
		return RepairResult{Gap: *gap, SegmentID: s.segmentID}, nil
	}
	frame := frames[0]
	if err := s.recorder.BufferRepaired(s.segmentID, frame); err != nil {
		return RepairResult{}, err
	}
	gap.Status = GapRepaired
	gap.RepairedBy = attemptID
	gap.RepairedAt = time.Now().UTC()
	delete(s.repairs, epoch)
	_, _ = s.audit.Record(audit.Event{Component: "decoder", Action: "gap.repaired", Subject: s.passID, Fields: map[string]any{
		"epoch": gap.Epoch, "sequence": gap.Sequence, "repaired_by": attemptID, "segment_id": s.segmentID,
	}})
	return RepairResult{Gap: *gap, SegmentID: s.segmentID, Repaired: true, Frame: &frame}, nil
}

func (s *Session) Snapshot() (attemptID string, epoch int64, lostSymbols int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.attemptID, s.epoch, s.lostSymbols
}

// Report exposes gaps by epoch, frame sequence and receive attempt without
// disturbing session state.
func (s *Session) Report() SessionReport {
	s.mu.Lock()
	defer s.mu.Unlock()
	epochs := append([]EpochStat(nil), s.epochs...)
	gaps := make([]Gap, 0, len(s.gaps))
	for _, gap := range s.gaps {
		if gap.Epoch >= 1 && gap.Epoch <= int64(len(epochs)) {
			if gap.Status == GapRepaired {
				epochs[gap.Epoch-1].RepairedGaps++
			} else {
				epochs[gap.Epoch-1].OpenGaps++
			}
		}
		gaps = append(gaps, gap)
	}
	return SessionReport{
		PassID: s.passID, SegmentID: s.segmentID, CurrentAttempt: s.attemptID,
		CurrentEpoch: s.epoch, LostSymbols: s.lostSymbols, Epochs: epochs, Gaps: gaps,
	}
}
