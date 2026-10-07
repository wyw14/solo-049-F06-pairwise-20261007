package api

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"orbitlink/internal/receiver"
	"orbitlink/internal/rf"
)

func TestDecoderGapReportAndEpochRepair(t *testing.T) {
	runtime := newTestRuntime(t)
	value := runtime.Passes.List()[0]
	attempt := trackPass(t, runtime, value.ID)

	session, err := runtime.receiverSession(value.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.OnLock(attempt.ID); err != nil {
		t.Fatal(err)
	}
	// 100 symbols = one complete 64-byte frame plus a 36-symbol partial tail.
	if frames, err := session.Feed(attempt.ID, make([]byte, 100)); err != nil || frames != 1 {
		t.Fatalf("first epoch feed assembled %d frames: %v", frames, err)
	}

	second, err := runtime.Attempts.Begin(value.ID, "RF-02")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.OnLock(second.ID); err != nil {
		t.Fatal(err)
	}

	report := session.DecoderReport()
	if report.CurrentEpoch != 2 || report.LostSymbols != 36 {
		t.Fatalf("unexpected session totals: epoch %d lost %d", report.CurrentEpoch, report.LostSymbols)
	}
	if len(report.Epochs) != 2 {
		t.Fatalf("expected two epoch entries, got %d", len(report.Epochs))
	}
	first := report.Epochs[0]
	if first.AttemptID != attempt.ID || first.SymbolsReceived != 100 || first.SymbolsDiscarded != 36 || first.FramesAssembled != 1 {
		t.Fatalf("first epoch stats wrong: %#v", first)
	}
	if len(first.Gaps) != 1 || first.Gaps[0].Sequence != 2 || first.Gaps[0].DiscardedSymbols != 36 || first.Gaps[0].Repaired {
		t.Fatalf("first epoch gap wrong: %#v", first.Gaps)
	}
	if !report.Epochs[1].Active || report.Epochs[1].AttemptID != second.ID {
		t.Fatalf("second epoch should be active: %#v", report.Epochs[1])
	}

	// Retransmit the closed epoch's stream: frame 1 duplicates the live
	// delivery, frame 2 closes the gap and is linked to the segment.
	segment, found := runtime.Recorder.ActiveForPass(value.ID)
	if !found {
		t.Fatal("no active recording")
	}
	result, err := session.FeedRepair(attempt.ID, 1, make([]byte, 128))
	if err != nil {
		t.Fatal(err)
	}
	if result.SegmentID != segment.ID || len(result.Repaired) != 1 || result.Repaired[0] != 2 || result.Duplicates != 1 {
		t.Fatalf("repair result wrong: %#v", result)
	}

	report = session.DecoderReport()
	if !report.Epochs[0].Gaps[0].Repaired {
		t.Fatalf("gap should be marked repaired: %#v", report.Epochs[0].Gaps)
	}
	if len(report.Repairs) != 1 || report.Repairs[0].SegmentID != segment.ID || report.Repairs[0].Epoch != 1 {
		t.Fatalf("repair was not linked to the segment: %#v", report.Repairs)
	}
	segment, _ = runtime.Recorder.ActiveForPass(value.ID)
	if segment.FramesBuffered != 2 {
		t.Fatalf("segment should hold live frame plus repaired frame, has %d", segment.FramesBuffered)
	}

	// The old epoch's retransmission must not pollute the current session.
	current := report.Epochs[1]
	if report.CurrentEpoch != 2 || report.LostSymbols != 36 ||
		current.SymbolsReceived != 0 || current.FramesAssembled != 0 || current.RepairSymbols != 0 {
		t.Fatalf("current epoch was polluted by repair: %#v (lost %d, epoch %d)", current, report.LostSymbols, report.CurrentEpoch)
	}
	if _, err := session.Feed(attempt.ID, make([]byte, 64)); err == nil {
		t.Fatal("stale attempt must not feed the live epoch")
	}

	// Repair validation: wrong attempt, active epoch and unknown epoch.
	if _, err := session.FeedRepair(second.ID, 1, make([]byte, 64)); err == nil {
		t.Fatal("repair with mismatched attempt unexpectedly succeeded")
	}
	if _, err := session.FeedRepair(second.ID, 2, make([]byte, 64)); err == nil {
		t.Fatal("repair against the active epoch unexpectedly succeeded")
	}
	if _, err := session.FeedRepair(attempt.ID, 99, make([]byte, 64)); err == nil {
		t.Fatal("repair against an unknown epoch unexpectedly succeeded")
	}
}

func TestDecoderGapEndpoints(t *testing.T) {
	runtime := newTestRuntime(t)
	value := runtime.Passes.List()[0]
	attempt := trackPass(t, runtime, value.ID)
	session, err := runtime.receiverSession(value.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.OnLock(attempt.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Feed(attempt.ID, make([]byte, 100)); err != nil {
		t.Fatal(err)
	}

	router, err := NewRouter(runtime)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(router)
	defer server.Close()
	response, err := server.Client().Get(fmt.Sprintf("%s/api/passes/%s/decoder/gaps", server.URL, value.ID))
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("gap report returned %d", response.StatusCode)
	}
	var report struct {
		CurrentEpoch int64 `json:"current_epoch"`
		Epochs       []struct {
			Epoch          int64 `json:"epoch"`
			PendingSymbols int   `json:"pending_symbols"`
			Gaps           []struct {
				Sequence uint64 `json:"sequence"`
			} `json:"gaps"`
		} `json:"epochs"`
	}
	decodeBody(t, response, &report)
	if report.CurrentEpoch != 1 || len(report.Epochs) != 1 || len(report.Epochs[0].Gaps) != 0 || report.Epochs[0].PendingSymbols != 36 {
		t.Fatalf("open epoch should report pending symbols but no gaps yet: %#v", report)
	}
}

func trackPass(t *testing.T, runtime *Runtime, passID string) receiver.Attempt {
	t.Helper()
	for _, chainID := range []string{"RF-01", "RF-02"} {
		if _, err := runtime.RF.Apply(rf.Plan{PassID: passID, ChainID: chainID, CenterHz: 2_245_000_000, DopplerHz: 12_000, Polarization: rf.RightCircular}); err != nil {
			t.Fatal(err)
		}
	}
	attempt, err := runtime.Attempts.Begin(passID, "RF-01")
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Passes.BeginAcquisition(passID, attempt.ID); err != nil {
		t.Fatal(err)
	}
	decision, err := runtime.Passes.AcceptDiversity(passID, []receiver.AcquisitionResult{
		{PassID: passID, AttemptID: attempt.ID, ChainID: "RF-01", Locked: true, Synced: true, Epoch: 1},
		{PassID: passID, AttemptID: attempt.ID, ChainID: "RF-02", Locked: true, Synced: true, Epoch: 1},
	})
	if err != nil || !decision.Ready {
		t.Fatalf("acquisition did not converge: %#v %v", decision, err)
	}
	return attempt
}
