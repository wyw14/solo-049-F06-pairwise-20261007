package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	stationclock "orbitlink/internal/clock"
	passsvc "orbitlink/internal/pass"
	"orbitlink/internal/receiver"
	"orbitlink/internal/rf"
	"orbitlink/internal/satellite"
	"orbitlink/internal/weather"
)

func TestOperationsPagesAndAPIsUseRuntimeState(t *testing.T) {
	runtime := newTestRuntime(t)
	router, err := NewRouter(runtime)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(router)
	defer server.Close()
	for _, target := range []string{"/passes", "/antennas", "/rf-chains", "/recordings", "/healthz", "/api/passes", "/api/antennas", "/api/rf-chains", "/api/recordings"} {
		response, err := server.Client().Get(server.URL + target)
		if err != nil {
			t.Fatalf("GET %s: %v", target, err)
		}
		if response.StatusCode != http.StatusOK {
			t.Fatalf("GET %s returned %d", target, response.StatusCode)
		}
		_ = response.Body.Close()
	}
}

func TestPassRevisionAcquisitionAndRecordingFlow(t *testing.T) {
	runtime := newTestRuntime(t)
	value := runtime.Passes.List()[0]
	if _, err := runtime.RF.Apply(rf.Plan{PassID: value.ID, ChainID: "RF-01", CenterHz: 2_245_000_000, DopplerHz: 12_000, Polarization: rf.RightCircular}); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.RF.Apply(rf.Plan{PassID: value.ID, ChainID: "RF-02", CenterHz: 2_245_000_000, DopplerHz: 12_050, Polarization: rf.RightCircular}); err != nil {
		t.Fatal(err)
	}
	attempt, err := runtime.Attempts.Begin(value.ID, "RF-01")
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Passes.BeginAcquisition(value.ID, attempt.ID); err != nil {
		t.Fatal(err)
	}
	results := []receiver.AcquisitionResult{
		{PassID: value.ID, AttemptID: attempt.ID, ChainID: "RF-01", Locked: true, Synced: true, Epoch: 7},
		{PassID: value.ID, AttemptID: attempt.ID, ChainID: "RF-02", Locked: true, Synced: true, Epoch: 7},
	}
	decision, err := runtime.Passes.AcceptDiversity(value.ID, results)
	if err != nil || !decision.Ready {
		t.Fatalf("diversity did not converge: %#v %v", decision, err)
	}
	active, found := runtime.Recorder.ActiveForPass(value.ID)
	if !found {
		t.Fatal("tracking did not start a recorder")
	}
	session, err := runtime.NewDecoderSession(value.ID, active.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.OnLock(attempt.ID); err != nil {
		t.Fatal(err)
	}
	if written, err := session.Feed(attempt.ID, make([]byte, 128)); err != nil || written != 2 {
		t.Fatalf("decoder feed wrote %d frames: %v", written, err)
	}
	if _, err := runtime.Recorder.Drain(context.Background(), active.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Recorder.Seal(active.ID, "integration complete"); err != nil {
		t.Fatal(err)
	}
}

func TestEphemerisClockAndWindSafetyFlow(t *testing.T) {
	runtime := newTestRuntime(t)
	value := runtime.Passes.List()[0]
	previous, _ := runtime.Ephemerides.Current(value.SatelliteID)
	if _, err := runtime.Ephemerides.Import(satellite.Ephemeris{SatelliteID: value.SatelliteID, Revision: previous.Revision + 1, Epoch: time.Now().UTC(), Line1: "revised orbital element one", Line2: "revised orbital element two"}); err != nil {
		t.Fatal(err)
	}
	commands := runtime.Commands.List(value.ID)
	if len(commands) == 0 || commands[len(commands)-1].Revision != previous.Revision+1 {
		t.Fatal("revised pointing commands were not queued")
	}
	if _, err := runtime.Clock.Apply(stationclock.Sample{ObservedAt: time.Now().UTC(), Offset: 2 * time.Second, Source: "gps", Quality: "locked"}); err != nil {
		t.Fatal(err)
	}
	if len(runtime.Clock.Corrections()) != 1 {
		t.Fatal("clock correction was not persisted")
	}
	if err := runtime.RF.EnableTransmit("RF-01"); err != nil {
		t.Fatal(err)
	}
	result, err := runtime.Weather.Handle(context.Background(), weather.WindSample{StationID: "station", GustMetersPerSecond: 25, ObservedAt: time.Now().UTC()}, value.ID, value.AntennaID, "RF-01")
	if err != nil || !result.InhibitConfirmed || !result.Stowed {
		t.Fatalf("wind safety did not converge: %#v %v", result, err)
	}
}

func TestDecoderGapRetransmissionAPI(t *testing.T) {
	runtime := newTestRuntime(t)
	router, err := NewRouter(runtime)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(router)
	defer server.Close()
	value := runtime.Passes.List()[0]
	if _, err := runtime.RF.Apply(rf.Plan{PassID: value.ID, ChainID: "RF-01", CenterHz: 2_245_000_000, DopplerHz: 12_000, Polarization: rf.RightCircular}); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.RF.Apply(rf.Plan{PassID: value.ID, ChainID: "RF-02", CenterHz: 2_245_000_000, DopplerHz: 12_050, Polarization: rf.RightCircular}); err != nil {
		t.Fatal(err)
	}
	attempt, err := runtime.Attempts.Begin(value.ID, "RF-01")
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Passes.BeginAcquisition(value.ID, attempt.ID); err != nil {
		t.Fatal(err)
	}
	results := []receiver.AcquisitionResult{
		{PassID: value.ID, AttemptID: attempt.ID, ChainID: "RF-01", Locked: true, Synced: true, Epoch: 3},
		{PassID: value.ID, AttemptID: attempt.ID, ChainID: "RF-02", Locked: true, Synced: true, Epoch: 3},
	}
	if _, err := runtime.Passes.AcceptDiversity(value.ID, results); err != nil {
		t.Fatal(err)
	}
	segment, found := runtime.Recorder.ActiveForPass(value.ID)
	if !found {
		t.Fatal("acquisition did not open a recording segment")
	}
	post := func(target string, payload any) *http.Response {
		body, err := json.Marshal(payload)
		if err != nil {
			t.Fatal(err)
		}
		return requestJSON(t, server.Client(), http.MethodPost, server.URL+target, string(body))
	}
	response := post("/api/passes/"+value.ID+"/receiver/lock", map[string]any{"attempt_id": attempt.ID})
	var locked struct {
		Epoch int64 `json:"epoch"`
	}
	decodeBody(t, response, &locked)
	if locked.Epoch != 1 {
		t.Fatalf("first lock opened epoch %d", locked.Epoch)
	}
	// 74 bytes assemble one 64-byte frame and leave a 10-symbol partial frame.
	response = post("/api/passes/"+value.ID+"/receiver/symbols", map[string]any{"attempt_id": attempt.ID, "symbols": make([]byte, 74)})
	var fed struct {
		Frames int `json:"frames_buffered"`
	}
	decodeBody(t, response, &fed)
	if fed.Frames != 1 {
		t.Fatalf("expected 1 assembled frame, got %d", fed.Frames)
	}
	// A new receive attempt rolls the epoch and opens a gap at sequence 2.
	attempt2, err := runtime.Attempts.Begin(value.ID, "RF-01")
	if err != nil {
		t.Fatal(err)
	}
	response = post("/api/passes/"+value.ID+"/receiver/lock", map[string]any{"attempt_id": attempt2.ID})
	decodeBody(t, response, &locked)
	if locked.Epoch != 2 {
		t.Fatalf("second lock opened epoch %d", locked.Epoch)
	}
	var report struct {
		CurrentEpoch int64 `json:"current_epoch"`
		LostSymbols  int   `json:"lost_symbols"`
		Gaps         []struct {
			Epoch          int64  `json:"epoch"`
			AttemptID      string `json:"attempt_id"`
			Sequence       uint64 `json:"sequence"`
			MissingSymbols int    `json:"missing_symbols"`
			Status         string `json:"status"`
			RepairedBy     string `json:"repaired_by"`
		} `json:"gaps"`
	}
	response, err = server.Client().Get(server.URL + "/api/passes/" + value.ID + "/decoder/gaps")
	if err != nil {
		t.Fatal(err)
	}
	decodeBody(t, response, &report)
	if report.CurrentEpoch != 2 || report.LostSymbols != 10 || len(report.Gaps) != 1 {
		t.Fatalf("gap report wrong after rollover: %#v", report)
	}
	gap := report.Gaps[0]
	if gap.Epoch != 1 || gap.AttemptID != attempt.ID || gap.Sequence != 2 || gap.MissingSymbols != 54 || gap.Status != "open" {
		t.Fatalf("gap not keyed by epoch/sequence/attempt: %#v", gap)
	}
	// Retransmit the missing tail on the live attempt; the old epoch is repaired.
	response = post("/api/passes/"+value.ID+"/receiver/retransmit", map[string]any{"attempt_id": attempt2.ID, "epoch": 1, "symbols": make([]byte, 54)})
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("retransmit returned %d", response.StatusCode)
	}
	var repair struct {
		Repaired  bool   `json:"repaired"`
		SegmentID string `json:"segment_id"`
		Frame     *struct {
			Sequence uint64 `json:"sequence"`
			Epoch    int64  `json:"epoch"`
		} `json:"frame"`
	}
	decodeBody(t, response, &repair)
	if !repair.Repaired || repair.Frame == nil || repair.Frame.Sequence != 2 || repair.Frame.Epoch != 1 || repair.SegmentID != segment.ID {
		t.Fatalf("repair result not linked to the segment: %#v", repair)
	}
	response, err = server.Client().Get(server.URL + "/api/passes/" + value.ID + "/decoder/gaps")
	if err != nil {
		t.Fatal(err)
	}
	decodeBody(t, response, &report)
	if report.Gaps[0].Status != "repaired" || report.Gaps[0].RepairedBy != attempt2.ID {
		t.Fatalf("gap not marked repaired: %#v", report.Gaps[0])
	}
	response, err = server.Client().Get(server.URL + "/api/recordings")
	if err != nil {
		t.Fatal(err)
	}
	var recordings struct {
		Items []struct {
			ID             string `json:"id"`
			FramesRepaired int    `json:"frames_repaired"`
		} `json:"items"`
	}
	decodeBody(t, response, &recordings)
	repaired := -1
	for _, item := range recordings.Items {
		if item.ID == segment.ID {
			repaired = item.FramesRepaired
		}
	}
	if repaired != 1 {
		t.Fatalf("segment frames_repaired = %d, want 1", repaired)
	}
	// A second retransmission for the repaired gap is rejected.
	response = post("/api/passes/"+value.ID+"/receiver/retransmit", map[string]any{"attempt_id": attempt2.ID, "epoch": 1, "symbols": make([]byte, 1)})
	if response.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("double repair returned %d", response.StatusCode)
	}
	_ = response.Body.Close()
}

func TestRouterRejectsOverlappingReservation(t *testing.T) {
	runtime := newTestRuntime(t)
	first := runtime.Passes.List()[0]
	second, err := runtime.Passes.Create(passsvc.CreateRequest{SatelliteID: "SAT-C07", AntennaID: first.AntennaID, RFChains: []string{"RF-03"}, RequiredReceiverChains: []string{"RF-03"}, AOS: first.Window.AOS.Add(time.Minute), LOS: first.Window.LOS.Add(time.Minute), Revision: 1, OwnerStation: "north"})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.WindowRecompute.Apply(second.ID, second.Window); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Reservations.ReservePass(second.ID, second.AntennaID); err == nil {
		t.Fatal("overlapping reservation unexpectedly succeeded")
	}
}

func newTestRuntime(t *testing.T) *Runtime {
	t.Helper()
	runtime, err := NewRuntime(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return runtime
}

func decodeBody(t *testing.T, response *http.Response, value any) {
	t.Helper()
	defer response.Body.Close()
	if err := json.NewDecoder(response.Body).Decode(value); err != nil {
		t.Fatal(err)
	}
}

func requestJSON(t *testing.T, client *http.Client, method, target, payload string) *http.Response {
	t.Helper()
	request, err := http.NewRequest(method, target, strings.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}
