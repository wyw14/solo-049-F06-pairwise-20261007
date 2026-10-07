package decoder

import (
	"bytes"
	"strings"
	"testing"

	"orbitlink/internal/audit"
	"orbitlink/internal/recorder"
	"orbitlink/internal/store"
)

func newFixture(t *testing.T, frameSize int) (*Session, *recorder.Manager, string) {
	t.Helper()
	paths := store.NewPaths(t.TempDir())
	trail := audit.NewTrail(store.NewAppendLog(paths))
	manager, err := recorder.NewManager(paths, trail)
	if err != nil {
		t.Fatal(err)
	}
	segment, err := manager.Start("pass-1", "north", 1)
	if err != nil {
		t.Fatal(err)
	}
	session, err := NewSession("pass-1", segment.ID, frameSize, manager, trail)
	if err != nil {
		t.Fatal(err)
	}
	return session, manager, segment.ID
}

func findGap(t *testing.T, report SessionReport, epoch int64) Gap {
	t.Helper()
	for _, gap := range report.Gaps {
		if gap.Epoch == epoch {
			return gap
		}
	}
	t.Fatalf("no gap recorded for epoch %d", epoch)
	return Gap{}
}

func TestAssemblerResetReservesSequence(t *testing.T) {
	assembler, err := NewAssembler(8)
	if err != nil {
		t.Fatal(err)
	}
	if frames := assembler.Push(1, make([]byte, 11)); len(frames) != 1 || frames[0].Sequence != 1 {
		t.Fatalf("unexpected frames after first push: %#v", frames)
	}
	discarded, reserved := assembler.Reset()
	if len(discarded) != 3 || reserved != 2 {
		t.Fatalf("reset discarded %d bytes, reserved sequence %d", len(discarded), reserved)
	}
	frames := assembler.Push(2, make([]byte, 8))
	if len(frames) != 1 || frames[0].Sequence != 3 {
		t.Fatalf("live stream must skip the reserved sequence, got %#v", frames)
	}
	if discarded, reserved := assembler.Reset(); discarded != nil || reserved != 0 {
		t.Fatalf("clean reset must not reserve a sequence, got %d bytes and %d", len(discarded), reserved)
	}
}

func TestGapReportByEpochSequenceAndAttempt(t *testing.T) {
	session, _, segmentID := newFixture(t, 8)
	if _, err := session.BeginEpoch("attempt-1"); err != nil {
		t.Fatal(err)
	}
	symbols := append(EncodeTelemetry(1, 8), EncodeTelemetry(2, 8)[:3]...)
	if frames, err := session.Feed("attempt-1", symbols); err != nil || frames != 1 {
		t.Fatalf("feed assembled %d frames: %v", frames, err)
	}
	if _, err := session.BeginEpoch("attempt-2"); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Feed("attempt-2", EncodeTelemetry(9, 8)); err != nil {
		t.Fatal(err)
	}
	report := session.Report()
	if report.CurrentEpoch != 2 || report.CurrentAttempt != "attempt-2" || report.SegmentID != segmentID {
		t.Fatalf("unexpected report header: %#v", report)
	}
	if report.LostSymbols != 3 {
		t.Fatalf("lost symbols = %d, want 3", report.LostSymbols)
	}
	gap := findGap(t, report, 1)
	if gap.AttemptID != "attempt-1" || gap.Sequence != 2 || gap.ReceivedSymbols != 3 || gap.MissingSymbols != 5 {
		t.Fatalf("gap does not identify epoch/sequence/attempt: %#v", gap)
	}
	if gap.Status != GapOpen || gap.SegmentID != segmentID {
		t.Fatalf("gap not open against the recording segment: %#v", gap)
	}
	if len(report.Epochs) != 2 {
		t.Fatalf("expected 2 epoch stats, got %#v", report.Epochs)
	}
	first, second := report.Epochs[0], report.Epochs[1]
	if first.AttemptID != "attempt-1" || first.SymbolsReceived != 11 || first.FramesAssembled != 1 || first.DiscardedSymbols != 3 || first.OpenGaps != 1 {
		t.Fatalf("epoch 1 stat wrong: %#v", first)
	}
	if second.AttemptID != "attempt-2" || second.SymbolsReceived != 8 || second.FramesAssembled != 1 || second.OpenGaps != 0 {
		t.Fatalf("epoch 2 stat wrong: %#v", second)
	}
}

func TestRetransmissionReframesAndRepairsSegment(t *testing.T) {
	session, manager, segmentID := newFixture(t, 8)
	if _, err := session.BeginEpoch("attempt-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Feed("attempt-1", append(EncodeTelemetry(1, 8), EncodeTelemetry(2, 8)[:3]...)); err != nil {
		t.Fatal(err)
	}
	if _, err := session.BeginEpoch("attempt-2"); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Feed("attempt-2", EncodeTelemetry(9, 8)); err != nil {
		t.Fatal(err)
	}
	result, err := session.FeedRepair("attempt-2", 1, EncodeTelemetry(2, 8)[3:])
	if err != nil {
		t.Fatal(err)
	}
	if !result.Repaired || result.Frame == nil || result.SegmentID != segmentID {
		t.Fatalf("repair did not complete: %#v", result)
	}
	if result.Frame.Sequence != 2 || result.Frame.Epoch != 1 {
		t.Fatalf("repaired frame must take the reserved sequence of its epoch: %#v", result.Frame)
	}
	if !bytes.Equal(result.Frame.Payload, EncodeTelemetry(2, 8)) {
		t.Fatalf("reframed payload mismatch: %x", result.Frame.Payload)
	}
	report := session.Report()
	gap := findGap(t, report, 1)
	if gap.Status != GapRepaired || gap.RepairedBy != "attempt-2" || gap.MissingSymbols != 0 || gap.RepairedAt.IsZero() {
		t.Fatalf("gap not marked repaired: %#v", gap)
	}
	if report.Epochs[0].RepairedGaps != 1 || report.Epochs[0].OpenGaps != 0 {
		t.Fatalf("epoch 1 gap counters wrong: %#v", report.Epochs[0])
	}
	// The repair must not pollute the active epoch's reception counters.
	if report.Epochs[1].SymbolsReceived != 8 || report.Epochs[1].FramesAssembled != 1 {
		t.Fatalf("repair polluted active epoch stats: %#v", report.Epochs[1])
	}
	if report.LostSymbols != 3 {
		t.Fatalf("repair must not rewrite historical loss: %d", report.LostSymbols)
	}
	var segment recorder.Segment
	for _, value := range manager.List() {
		if value.ID == segmentID {
			segment = value
		}
	}
	if segment.FramesRepaired != 1 || segment.FramesBuffered != 3 {
		t.Fatalf("segment not associated with the repair: %#v", segment)
	}
}

func TestRetransmissionGuardsAgainstPollution(t *testing.T) {
	session, _, _ := newFixture(t, 8)
	if _, err := session.BeginEpoch("attempt-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Feed("attempt-1", append(EncodeTelemetry(1, 8), EncodeTelemetry(2, 8)[:3]...)); err != nil {
		t.Fatal(err)
	}
	if _, err := session.BeginEpoch("attempt-2"); err != nil {
		t.Fatal(err)
	}
	if _, err := session.FeedRepair("attempt-2", 2, make([]byte, 1)); err == nil || !strings.Contains(err.Error(), "active") {
		t.Fatalf("retransmission for the active epoch must be rejected: %v", err)
	}
	if _, err := session.FeedRepair("attempt-2", 99, make([]byte, 1)); err == nil || !strings.Contains(err.Error(), "unknown") {
		t.Fatalf("retransmission for an unknown epoch must be rejected: %v", err)
	}
	if _, err := session.FeedRepair("attempt-2", 1, make([]byte, 6)); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("oversized retransmission must be rejected: %v", err)
	}
	if _, err := session.FeedRepair("attempt-2", 1, EncodeTelemetry(2, 8)[3:]); err != nil {
		t.Fatal(err)
	}
	if _, err := session.FeedRepair("attempt-2", 1, make([]byte, 1)); err == nil || !strings.Contains(err.Error(), "already repaired") {
		t.Fatalf("double repair must be rejected: %v", err)
	}
	// The live stream is untouched by every rejected and accepted repair.
	frames, err := session.Feed("attempt-2", EncodeTelemetry(9, 8))
	if err != nil || frames != 1 {
		t.Fatalf("live feed after repairs failed: %d %v", frames, err)
	}
}

func TestPartialRetransmissionKeepsGapOpen(t *testing.T) {
	session, _, _ := newFixture(t, 8)
	if _, err := session.BeginEpoch("attempt-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Feed("attempt-1", append(EncodeTelemetry(1, 8), EncodeTelemetry(2, 8)[:3]...)); err != nil {
		t.Fatal(err)
	}
	if _, err := session.BeginEpoch("attempt-2"); err != nil {
		t.Fatal(err)
	}
	result, err := session.FeedRepair("attempt-2", 1, EncodeTelemetry(2, 8)[3:7])
	if err != nil {
		t.Fatal(err)
	}
	if result.Repaired || result.Gap.MissingSymbols != 1 || result.Gap.Status != GapOpen {
		t.Fatalf("partial retransmission must leave the gap open: %#v", result)
	}
	result, err = session.FeedRepair("attempt-2", 1, EncodeTelemetry(2, 8)[7:])
	if err != nil {
		t.Fatal(err)
	}
	if !result.Repaired || result.Frame == nil || !bytes.Equal(result.Frame.Payload, EncodeTelemetry(2, 8)) {
		t.Fatalf("follow-up retransmission must complete the frame: %#v", result)
	}
}

func TestEpochWithoutLossHasNoGap(t *testing.T) {
	session, _, _ := newFixture(t, 8)
	if _, err := session.BeginEpoch("attempt-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Feed("attempt-1", EncodeTelemetry(1, 8)); err != nil {
		t.Fatal(err)
	}
	if _, err := session.BeginEpoch("attempt-2"); err != nil {
		t.Fatal(err)
	}
	report := session.Report()
	if len(report.Gaps) != 0 || report.LostSymbols != 0 {
		t.Fatalf("clean epoch rollover must not open a gap: %#v", report.Gaps)
	}
	if _, err := session.FeedRepair("attempt-2", 1, make([]byte, 1)); err == nil || !strings.Contains(err.Error(), "no recorded gap") {
		t.Fatalf("retransmission for a gapless epoch must be rejected: %v", err)
	}
}
