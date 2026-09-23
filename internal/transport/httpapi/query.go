package httpapi

import (
	"context"
	"errors"
	"net/http"

	"github.com/jiahuipaung/rc_pangjiahui/internal/notification"
)

type Querier interface {
	Get(context.Context, string, string) (notification.View, error)
}
type Replayer interface {
	Replay(context.Context, string, string) error
}

func queryHandler(querier Querier) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		id := request.PathValue("notification_id")
		if id == "" {
			writeError(writer, http.StatusBadRequest, "invalid_notification_id", "notification id is required")
			return
		}
		view, err := querier.Get(request.Context(), callerFromContext(request.Context()), id)
		switch {
		case errors.Is(err, notification.ErrTaskNotFound):
			writeError(writer, http.StatusNotFound, "not_found", "notification not found")
		case err != nil:
			writeError(writer, http.StatusInternalServerError, "internal_error", "notification could not be queried")
		default:
			writeJSON(writer, http.StatusOK, view)
		}
	}
}

func replayHandler(replayer Replayer) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		id := request.PathValue("notification_id")
		err := replayer.Replay(request.Context(), callerFromContext(request.Context()), id)
		switch {
		case errors.Is(err, notification.ErrNotDead):
			writeError(writer, http.StatusConflict, "not_dead", "only dead notifications can be replayed")
		case err != nil:
			writeError(writer, http.StatusInternalServerError, "internal_error", "notification could not be replayed")
		default:
			writeJSON(writer, http.StatusAccepted, map[string]string{"notification_id": id, "status": "pending"})
		}
	}
}
