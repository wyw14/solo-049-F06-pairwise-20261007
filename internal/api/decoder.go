package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

func (r *Runtime) decoderGaps(writer http.ResponseWriter, request *http.Request) {
	session, err := r.receiverSession(chi.URLParam(request, "passID"))
	if err != nil {
		writeError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, session.DecoderReport())
}

func (r *Runtime) repairSymbols(writer http.ResponseWriter, request *http.Request) {
	var input struct {
		AttemptID string `json:"attempt_id"`
		Epoch     int64  `json:"epoch"`
		Symbols   []byte `json:"symbols"`
	}
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, err)
		return
	}
	session, err := r.receiverSession(chi.URLParam(request, "passID"))
	if err != nil {
		writeError(writer, err)
		return
	}
	result, err := session.FeedRepair(input.AttemptID, input.Epoch, input.Symbols)
	if err != nil {
		writeError(writer, err)
		return
	}
	writeJSON(writer, http.StatusAccepted, result)
}
