package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/jiahuipaung/rc_pangjiahui/internal/intake"
	"github.com/jiahuipaung/rc_pangjiahui/internal/notification"
)

const MaxRequestBodyBytes = 256 << 10

type Creator interface {
	Create(context.Context, string, string, string, json.RawMessage) (notification.Task, bool, error)
}

type createRequest struct {
	DestinationID string          `json:"destination_id"`
	Payload       json.RawMessage `json:"payload"`
}

type createResponse struct {
	NotificationID string              `json:"notification_id"`
	Status         notification.Status `json:"status"`
}

func createHandler(creator Creator) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		idempotencyKey := strings.TrimSpace(request.Header.Get("Idempotency-Key"))
		if idempotencyKey == "" || len(idempotencyKey) > 256 {
			writeError(writer, http.StatusBadRequest, "invalid_idempotency_key", "Idempotency-Key is required and must not exceed 256 characters")
			return
		}
		var input createRequest
		if err := decodeJSON(writer, request, &input); err != nil {
			var maxBytesError *http.MaxBytesError
			if errors.As(err, &maxBytesError) {
				writeError(writer, http.StatusRequestEntityTooLarge, "request_too_large", "request body exceeds 256 KiB")
				return
			}
			writeError(writer, http.StatusBadRequest, "invalid_json", "request body must be one valid JSON object")
			return
		}
		if input.DestinationID == "" || len(input.Payload) == 0 {
			writeError(writer, http.StatusBadRequest, "invalid_request", "destination_id and payload are required")
			return
		}
		task, _, err := creator.Create(request.Context(), callerFromContext(request.Context()), idempotencyKey, input.DestinationID, input.Payload)
		switch {
		case errors.Is(err, intake.ErrIdempotencyConflict):
			writeError(writer, http.StatusConflict, "idempotency_conflict", "Idempotency-Key was already used for a different request")
		case errors.Is(err, intake.ErrUnknownDestination):
			writeError(writer, http.StatusUnprocessableEntity, "unknown_destination", "destination_id is not registered")
		case err != nil:
			writeError(writer, http.StatusInternalServerError, "internal_error", "request could not be accepted")
		default:
			writeJSON(writer, http.StatusAccepted, createResponse{NotificationID: task.ID, Status: task.Status})
		}
	}
}
