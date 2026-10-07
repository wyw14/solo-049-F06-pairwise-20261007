package receiver

import (
	"fmt"
	"sync"

	"orbitlink/internal/decoder"
)

type Session struct {
	mu      sync.Mutex
	passID  string
	current string
	locked  bool
	decoder *decoder.Session
}

func NewSession(passID string, decoderSession *decoder.Session) *Session {
	return &Session{passID: passID, decoder: decoderSession}
}

func (s *Session) OnLock(attemptID string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if attemptID == "" {
		return 0, fmt.Errorf("lock attempt is required")
	}
	epoch, err := s.decoder.BeginEpoch(attemptID)
	if err != nil {
		return 0, err
	}
	s.current = attemptID
	s.locked = true
	return epoch, nil
}

func (s *Session) OnCarrierLoss(attemptID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.current != attemptID {
		return nil
	}
	s.locked = false
	return nil
}

func (s *Session) Feed(attemptID string, symbols []byte) (int, error) {
	s.mu.Lock()
	active := s.current == attemptID && s.locked
	s.mu.Unlock()
	if !active {
		return 0, fmt.Errorf("receiver attempt is not locked")
	}
	return s.decoder.Feed(attemptID, symbols)
}

// FeedRepair routes retransmitted symbols for a closed decoder epoch. The
// receiver must still be locked on its current attempt — the retransmission
// physically arrives on the live link — but the decoder assembles it against
// the old epoch's gap, never the active frame stream.
func (s *Session) FeedRepair(attemptID string, epoch int64, symbols []byte) (decoder.RepairResult, error) {
	s.mu.Lock()
	active := s.current == attemptID && s.locked
	s.mu.Unlock()
	if !active {
		return decoder.RepairResult{}, fmt.Errorf("receiver attempt is not locked")
	}
	return s.decoder.FeedRepair(attemptID, epoch, symbols)
}

func (s *Session) DecoderReport() decoder.SessionReport {
	return s.decoder.Report()
}

func (s *Session) Status() (attemptID string, locked bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.current, s.locked
}
